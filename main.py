import json
import os
import mimetypes
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import time
import boto3
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent
from strands.models import BedrockModel
#from bedrock_agentcore_starter_toolkit import Runtime
#from boto3.session import Session

def render_html_report(bucket: str, key: str, result: Dict[str, Any]) -> str:
    """
    Render a FAANG-style, human-readable HTML report from the AgentCore result.
    No JSON, no raw fields in the final HTML – just narrative intelligence.
    """
    filename = key.split("/")[-1]

    classification = result.get("classification") or {}
    processing = result.get("processing_result") or {}
    extracted_fields = processing.get("extracted_fields") or {}
    summary = processing.get("summary") or "The system processed this document and generated structured insights."

    category = classification.get("category") or "UNCLASSIFIED"

    # Map internal categories to human language for "Core Intent"
    INTENT_MAP = {
        "SUMMARY_MEMO": "Summarization and insight generation",
        "QUESTIONS_DOC": "Question understanding and knowledge retrieval",
        "KYC_DOC": "Client onboarding and identity verification",
        "ACCOUNT_STATEMENT": "Account and portfolio reporting",
        "SUITABILITY_FORM": "Financial suitability assessment",
        "DATA_JSON": "Configuration / analytics data inspection",
        "POLICY_OR_DISCLOSURE": "Policy, disclosure, or terms analysis",
        "OTHER": "General document understanding",
    }
    core_intent = INTENT_MAP.get(category, "Document understanding and insight extraction")

    # Pull out some common fields if present
    main_points = extracted_fields.get("main_points") or extracted_fields.get("main_point") or []
    action_items = extracted_fields.get("action_items") or extracted_fields.get("actions") or []
    main_questions = extracted_fields.get("main_questions") or extracted_fields.get("questions") or []
    themes = extracted_fields.get("themes") or []

    def as_list(value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return [value]
        return []

    main_points = as_list(main_points)
    action_items = as_list(action_items)
    main_questions = as_list(main_questions)
    themes = as_list(themes)

    # Start building HTML
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Document Intelligence Report – {filename}</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      margin: 0;
      padding: 0;
      background-color: #0b1120;
      color: #e5e7eb;
    }}
    .container {{
      max-width: 900px;
      margin: 40px auto;
      padding: 32px;
      background: radial-gradient(circle at top left, #1e293b, #020617);
      border-radius: 24px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.45);
      border: 1px solid rgba(148, 163, 184, 0.35);
    }}
    h1 {{
      font-size: 28px;
      margin-bottom: 4px;
    }}
    .subtitle {{
      font-size: 14px;
      color: #9ca3af;
      margin-bottom: 24px;
    }}
    h2 {{
      font-size: 18px;
      margin-top: 24px;
      margin-bottom: 8px;
      color: #e5e7eb;
    }}
    p {{
      font-size: 14px;
      line-height: 1.6;
      color: #d1d5db;
      margin: 4px 0 8px 0;
    }}
    ul {{
      margin: 0 0 8px 18px;
      padding: 0;
      color: #d1d5db;
      font-size: 14px;
    }}
    li {{
      margin-bottom: 4px;
    }}
    .pill-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 4px;
      margin-bottom: 8px;
    }}
    .pill {{
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(148, 163, 184, 0.1);
      border: 1px solid rgba(148, 163, 184, 0.35);
      font-size: 12px;
      color: #e5e7eb;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      padding: 3px 10px;
      border-radius: 999px;
      background: rgba(22, 163, 74, 0.12);
      border: 1px solid rgba(22, 163, 74, 0.6);
      color: #bbf7d0;
      margin-left: 8px;
    }}
    .meta-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      margin-bottom: 12px;
      font-size: 13px;
      color: #9ca3af;
    }}
    .meta-label {{
      font-weight: 500;
      color: #e5e7eb;
    }}
    .section {{
      padding-top: 8px;
      padding-bottom: 4px;
      border-top: 1px solid rgba(148, 163, 184, 0.18);
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>📊 Document Intelligence Report</h1>
    <div class="subtitle">{filename}</div>

    <div class="meta-row">
      <div><span class="meta-label">Detected Intent:</span> {core_intent}</div>
      <div><span class="meta-label">Internal Category:</span> {category}</div>
    </div>

    <div class="section">
      <h2>📄 Document Overview</h2>
      <p>This document was automatically ingested from your content pipeline and analyzed by a multi-agent GenAI workflow.</p>
    </div>

    <div class="section">
      <h2>🧠 High-Level Understanding</h2>
      <p>{summary}</p>
    </div>

    <div class="section">
      <h2>🎯 Core Intent Identified</h2>
      <p>{core_intent}.</p>
    </div>
"""

    # Key Insights section – derive from main_points or other signals
    insights_lines: list[str] = []
    if main_points:
        insights_lines.extend(main_points)
    # Fallback: if no explicit main_points, use summary as single insight
    if not insights_lines and summary:
        insights_lines.append(summary)

    if insights_lines:
        html += """
    <div class="section">
      <h2>🔍 Key Insights Extracted</h2>
      <ul>
"""
        for line in insights_lines:
            html += f"        <li>{line}</li>\n"
        html += "      </ul>\n    </div>\n"

    # Key questions / action items
    if main_questions or action_items:
        html += """
    <div class="section">
      <h2>❓ Key Questions / Action Items</h2>
"""
        if main_questions:
            html += "      <p><strong>Questions identified:</strong></p>\n      <ul>\n"
            for q in main_questions:
                html += f"        <li>{q}</li>\n"
            html += "      </ul>\n"
        if action_items:
            html += "      <p><strong>Action items inferred:</strong></p>\n      <ul>\n"
            for a in action_items:
                html += f"        <li>{a}</li>\n"
            html += "      </ul>\n"
        html += "    </div>\n"

    # Themes → Thematic Interpretation
    if themes:
        html += """
    <div class="section">
      <h2>🧩 Thematic Interpretation</h2>
      <p>The system inferred the following themes from the content:</p>
      <div class="pill-row">
"""
        for t in themes:
            html += f'        <div class="pill">{t}</div>\n'
        html += "      </div>\n    </div>\n"

    # Automated outcome – generic but strong
    html += """
    <div class="section">
      <h2>✨ Automated Outcome</h2>
      <p>
        The document was autonomously interpreted by a multi-agent GenAI pipeline
        built on AWS Bedrock AgentCore. The system ingested the raw content,
        understood its purpose, extracted meaningful insights, and produced this
        human-readable intelligence brief without any manual intervention.
      </p>
    </div>

  </div>
</body>
</html>
"""
    return html


def write_html_result_to_s3(bucket: str, key: str, result: Dict[str, Any]) -> None:
    """
    Render the result as an HTML intelligence report and store it in S3
    under outputs/<filename>.html.
    """
    filename = key.split("/")[-1]
    out_key = f"outputs/{filename}.html"
    html = render_html_report(bucket, key, result)

    s3_client.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )

    app.logger.info(f"HTML intelligence report written to s3://{bucket}/{out_key}")


def write_result_to_s3(bucket: str, key: str, result: dict):
    """
    Writes the final result JSON to S3 under outputs/<filename>.json
    """
    filename = key.split("/")[-1]  # extract original file name
    out_key = f"outputs/{filename}.json"

    s3_client.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=json.dumps(result, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    app.logger.info(f"Final result written to: s3://{bucket}/{out_key}")

# -----------------------------------------------------------------------------
# App + AWS clients
# -----------------------------------------------------------------------------
app = BedrockAgentCoreApp(debug=True)

AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
s3_client = boto3.client("s3", region_name=AWS_REGION)
textract_client = boto3.client("textract", region_name=AWS_REGION)

MODEL_ID = "us.meta.llama4-maverick-17b-instruct-v1:0"
llm_model = BedrockModel(model_id=MODEL_ID)

# File types where we'll use Textract instead of direct text read
TEXTRACT_EXTS = {".pdf", ".tif", ".tiff", ".png", ".jpg", ".jpeg"}

# -----------------------------------------------------------------------------
# State object
# -----------------------------------------------------------------------------
@dataclass
class WorkflowState:
    bucket: str
    key: str
    text_s3_uri: Optional[str] = None
    text: Optional[str] = None
    classification: Optional[Dict[str, Any]] = None
    processing_result: Optional[Dict[str, Any]] = None


# -----------------------------------------------------------------------------
# Utility: extract plain text from S3 object (non-Textract path)
# -----------------------------------------------------------------------------
def read_s3_object_as_text(bucket: str, key: str) -> str:
    app.logger.info(f"Reading NON-Textract object from s3://{bucket}/{key}")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()

    content_type = (
        obj.get("ContentType")
        or mimetypes.guess_type(key)[0]
        or "application/octet-stream"
    )

    # Simple heuristics – good enough for our current sample docs
    if content_type.startswith("text/") or key.lower().endswith((".html", ".htm")):
        try:
            return body.decode("utf-8")
        except UnicodeDecodeError:
            return body.decode("latin-1", errors="ignore")

    if key.lower().endswith(".json"):
        return body.decode("utf-8")

    # Fallback: try utf-8, ignore errors
    return body.decode("utf-8", errors="ignore")


# -----------------------------------------------------------------------------
# Textract helpers (for PDF/images)
# -----------------------------------------------------------------------------
def should_use_textract(key: str) -> bool:
    ext = os.path.splitext(key)[1].lower()
    return ext in TEXTRACT_EXTS


def run_textract_sync(bucket: str, key: str) -> str:
    key_lower = key.lower()

    if key_lower.endswith(".pdf"):
        start_resp = textract_client.start_document_text_detection(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}}
        )
        job_id = start_resp["JobId"]

        while True:
            job_resp = textract_client.get_document_text_detection(JobId=job_id)
            status = job_resp["JobStatus"]

            if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
                break

            time.sleep(1.0)  # <- this line needs the import

        if status != "SUCCEEDED":
            raise RuntimeError(f"Textract job {job_id} did not succeed, status: {status}")

        lines: list[str] = []
        resp = job_resp
        while True:
            for block in resp.get("Blocks", []):
                if block.get("BlockType") == "LINE":
                    txt = block.get("Text")
                    if txt:
                        lines.append(txt)

            next_token = resp.get("NextToken")
            if not next_token:
                break

            resp = textract_client.get_document_text_detection(
                JobId=job_id,
                NextToken=next_token,
            )

        return "\n".join(lines).strip()

    # fallback sync path
    response = textract_client.detect_document_text(
        Document={"S3Object": {"Bucket": bucket, "Name": key}}
    )
    lines: list[str] = []
    for block in response.get("Blocks", []):
        if block.get("BlockType") == "LINE":
            txt = block.get("Text")
            if txt:
                lines.append(txt)

    return "\n".join(lines).strip()


# -----------------------------------------------------------------------------
# Ingestion: always happens BEFORE classification
# -----------------------------------------------------------------------------
def ensure_text_ingested(bucket: str, key: str) -> Dict[str, Any]:
    """
    Returns dict with 'text' and 'text_s3_uri'.
    Writes extracted text into textract-output/<filename>.txt
    (even for NON-Textract paths, just for consistency).
    """
    filename = key.split("/")[-1]
    out_key = f"textract-output/{filename}.txt"

    # Decide path
    if should_use_textract(key):
        app.logger.info(
            f"Textract-eligible object detected, using Textract for s3://{bucket}/{key}"
        )
        text = run_textract_sync(bucket, key)
    else:
        app.logger.info(
            f"Ingesting NON-Textract object from s3://{bucket}/{key}"
        )
        text = read_s3_object_as_text(bucket, key)

    # Store extracted text back to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=text.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )

    app.logger.info(
        f"Non-Textract / Textract text stored at s3://{bucket}/{out_key}"
    )

    return {
        "text": text,
        "text_s3_uri": f"s3://{bucket}/{out_key}",
    }


# -----------------------------------------------------------------------------
# LLM helpers – robust JSON extraction from Strands Agent response
# -----------------------------------------------------------------------------
def _extract_text_from_agent_response(resp: Any) -> str:
    """
    Handles typical Strands Agent output shapes like:
      {'role': 'assistant',
       'content': [{'text': '{...json...}'}]}
    or Message objects with .content[0].text, etc.
    """
    # Dict shape
    if isinstance(resp, dict):
        content = resp.get("content")
        if isinstance(content, list) and content:
            first = content[0]
            if isinstance(first, dict) and "text" in first:
                return first["text"]

        # Fallback: common keys
        for key in ("text", "message", "output"):
            if key in resp and isinstance(resp[key], str):
                return resp[key]

    # Object with .content
    if hasattr(resp, "content"):
        blocks = getattr(resp, "content")
        if isinstance(blocks, list) and blocks:
            first = blocks[0]
            if hasattr(first, "text"):
                return first.text

    # Object with .message or .text
    if hasattr(resp, "message"):
        m = getattr(resp, "message")
        if isinstance(m, str):
            return m
    if hasattr(resp, "text"):
        t = getattr(resp, "text")
        if isinstance(t, str):
            return t

    # Last resort
    return str(resp)


def _parse_json_from_text(raw_text: str, context: str) -> Dict[str, Any]:
    """
    Tries to parse JSON; if there is narration around JSON,
    strip everything outside the outermost {...}.
    """
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError as e:
        start = raw_text.find("{")
        end = raw_text.rfind("}") + 1
        if start != -1 and end > start:
            inner = raw_text[start:end]
            app.logger.warning(
                f"{context}: non-pure JSON, attempting inner braces only: {e}"
            )
            return json.loads(inner)

        app.logger.error(
            f"{context}: invalid JSON after extraction: {e} | text={raw_text!r}"
        )
        raise


# -----------------------------------------------------------------------------
# Classification Agent
# -----------------------------------------------------------------------------
classification_system_prompt = """
You are ClassificationAgent for a wealth management firm.

Your job is to classify an input document into ONE of these categories:

- KYC_DOC                → Client onboarding / KYC / personal info forms
- ACCOUNT_STATEMENT      → Brokerage / bank / annuity / portfolio statements
- SUITABILITY_FORM       → Suitability / risk-profile / annuity suitability docs
- QUESTIONS_DOC          → Internal question lists, FAQs, questionnaire docs
- DATA_JSON              → JSON data file used for analytics or configuration
- POLICY_OR_DISCLOSURE   → Disclosures, terms & conditions, prospectus-like
- SUMMARY_MEMO           → Internal summary / notes / email-style text
- OTHER                  → Anything that does not fit above

Return STRICT JSON with this schema:

{
  "category": "<one-of-the-above>",
  "confidence": <number between 0 and 1>
}

Do not include any explanation text outside the JSON.
"""

# NOTE: Agent no longer receives `system=`; we inject system prompt in the call.
classification_agent = Agent(
    model=llm_model,
)


def run_classification_agent(text: str) -> Dict[str, Any]:
    app.logger.info("Running Classification Agent")

    prompt = (
        classification_system_prompt.strip()
        + "\n\n"
        "Here is the full document text:\n"
        "--------------------------------\n"
        f"{text}\n"
        "--------------------------------\n\n"
        "Now respond with ONLY the JSON object as specified."
    )

    resp = classification_agent(prompt)
    app.logger.debug(f"Raw classification output: {resp}")

    raw_text = _extract_text_from_agent_response(resp)
    app.logger.info(f"Classification raw text: {raw_text!r}")

    classification = _parse_json_from_text(
        raw_text, context="ClassificationAgent"
    )
    app.logger.info(f"Parsed classification JSON: {classification}")
    return classification


# -----------------------------------------------------------------------------
# Processing Agent
# -----------------------------------------------------------------------------
processing_system_prompt = """
You are ProcessingAgent for a wealth management firm.

You receive:
1. The full document text.
2. A JSON classification object with fields:
   - category
   - confidence

Based on the category, perform specialized processing and
return a single JSON object with this general shape:

{
  "category": "<copied from input>",
  "summary": "<2–4 sentence natural-language summary>",
  "key_entities": {
      "clients": [...],
      "advisors": [...],
      "accounts": [...],
      "tickers": [...]
  },
  "extracted_fields": {
      // For KYC_DOC: name, DOB, address, risk_tolerance, ...
      // For ACCOUNT_STATEMENT: period, total_value, cash_balance, holdings, ...
      // For SUITABILITY_FORM: product_type, risk_profile, time_horizon, ...
      // For QUESTIONS_DOC: main_questions, themes, ...
      // For DATA_JSON: describe structure and fields
      // For POLICY_OR_DISCLOSURE: product_name, issuer, key_risks, fees, ...
      // For SUMMARY_MEMO: main_points, action_items, owners, dates, ...
  }
}

Be concise but informative. Always return VALID JSON only.
"""

processing_agent = Agent(
    model=llm_model,
)


def run_processing_agent(text: str, classification: Dict[str, Any]) -> Dict[str, Any]:
    app.logger.info("Running Processing Agent")

    prompt = (
        processing_system_prompt.strip()
        + "\n\n"
        "Document classification JSON:\n"
        f"{json.dumps(classification, indent=2)}\n\n"
        "Document text:\n"
        "--------------------------------\n"
        f"{text}\n"
        "--------------------------------\n\n"
        "Now produce the output JSON exactly as specified in your instructions above. "
        "Return ONLY JSON, no extra text."
    )

    resp = processing_agent(prompt)
    app.logger.debug(f"Raw processing output: {resp}")

    raw_text = _extract_text_from_agent_response(resp)
    app.logger.info(f"Processing raw text: {raw_text!r}")

    result = _parse_json_from_text(
        raw_text, context="ProcessingAgent"
    )
    app.logger.info("Parsed processing JSON successfully")
    return result


# -----------------------------------------------------------------------------
# Supervisor (Python orchestrator) – enforces ordering:
#   1) Ingestion   → state.text
#   2) Classification
#   3) Processing
# -----------------------------------------------------------------------------
def supervisor(state: WorkflowState) -> WorkflowState:
    app.logger.info(
        f"Supervisor Agent starting for s3://{state.bucket}/{state.key}"
    )

    # 1. Ensure text is present (ingestion / Textract)
    if not state.text:
        app.logger.info("No text found in state → ingesting text")
        ingest_out = ensure_text_ingested(state.bucket, state.key)
        state.text = ingest_out["text"]
        state.text_s3_uri = ingest_out["text_s3_uri"]

    # 2. Ensure classification is present
    if not state.classification:
        app.logger.info("No classification found → calling Classification Agent")
        state.classification = run_classification_agent(state.text)

    # 3. Ensure processing result is present
    if not state.processing_result:
        app.logger.info("No processing result found → calling Processing Agent")
        state.processing_result = run_processing_agent(
            state.text, state.classification
        )

    return state


# -----------------------------------------------------------------------------
# AgentCore entrypoint
# -----------------------------------------------------------------------------

@app.entrypoint
def invoke(
    payload: Dict[str, Any],
    headers: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Main entrypoint for Bedrock AgentCore runtime.

    Expected payload:
      {
        "bucket": "<s3-bucket>",
        "key": "<s3-key>"
      }

    (We ignore 'prompt' here; this runtime is event-driven on S3 docs.)
    """
    app.logger.info(f"Invoke payload: {payload}")
    app.logger.info(f"Invoke headers: {headers}")

    bucket = payload.get("bucket")
    key = payload.get("key")

    if not bucket or not key:
        raise ValueError(
            "Payload must contain 'bucket' and 'key', "
            "e.g. {'bucket': 'financial-doc-intake-dev-ue1', "
            "'key': 'intake/QuestionDoc.html'}"
        )

    state = WorkflowState(bucket=bucket, key=key)

    final_state = supervisor(state)

    # Compact programmatic result structure (still returned to Lambda / caller)
    result = {
        "bucket": final_state.bucket,
        "key": final_state.key,
        "text_s3_uri": final_state.text_s3_uri,
        "classification": final_state.classification,
        "processing_result": final_state.processing_result,
    }

    # NEW: write FAANG-style HTML intelligence report to S3
    try:
        write_html_result_to_s3(bucket, key, result)
    except Exception as e:
        app.logger.exception(f"Failed to write HTML result to S3: {e}")

    return result


if __name__ == "__main__":
    app.run()
