import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import PurePosixPath

import boto3

WEBHOOK_SECRET = os.environ.get("GH_WEBHOOK_SECRET", "")
SQS_QUEUE_NAME = os.environ.get("SQS_QUEUE_NAME", "workflow-orchestrator")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
GH_AUTH_LAMBDA_NAME = os.environ.get("GH_AUTH_LAMBDA_NAME", "").strip()
GITHUB_API_BASE = "https://api.github.com"
APPROVAL_CHECK_NAME = "External PR Approval"
APPROVAL_EXTERNAL_ID_KIND = "external_pr_approval"
APPROVAL_STATE_PREFIX = "external-pr-approvals"
_PERMISSION_LEVELS = {
    "none": 0,
    "read": 1,
    "triage": 2,
    "write": 3,
    "maintain": 4,
    "admin": 5,
}

# Keep the sender allow-list hook in place, but leave it empty by default so
# webhook dispatch is unrestricted unless a deployment explicitly populates it.
ALLOWED_SENDERS = set()


def _parse_allowed_push_branches():
    value = os.environ.get("ALLOWED_PUSH_BRANCHES")
    if value is None:
        return {"main"}
    return {branch.strip() for branch in value.split(",") if branch.strip()}


def _parse_autoapprove_paths():
    raw = os.environ.get("EXTERNAL_PR_AUTOAPPROVE_PATHS_JSON", "").strip()
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        print("WARNING: EXTERNAL_PR_AUTOAPPROVE_PATHS_JSON is not valid JSON")
        return []
    if not isinstance(value, list):
        print("WARNING: EXTERNAL_PR_AUTOAPPROVE_PATHS_JSON must decode to a list")
        return []
    return [str(pattern).strip() for pattern in value if str(pattern).strip()]


def _parse_allowed_repositories():
    raw = os.environ.get("ALLOWED_REPOSITORIES_JSON", "").strip()
    if not raw:
        return set()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        print("WARNING: ALLOWED_REPOSITORIES_JSON is not valid JSON")
        return set()
    if not isinstance(value, list):
        print("WARNING: ALLOWED_REPOSITORIES_JSON must decode to a list")
        return set()
    return {str(repo).strip() for repo in value if str(repo).strip()}


def _parse_allowed_users():
    raw = os.environ.get("ALLOWED_USERS_JSON", "").strip()
    if not raw:
        return set()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        print("WARNING: ALLOWED_USERS_JSON is not valid JSON")
        return set()
    if not isinstance(value, list):
        print("WARNING: ALLOWED_USERS_JSON must decode to a list")
        return set()
    return {str(user).strip() for user in value if str(user).strip()}


ALLOWED_PUSH_BRANCHES = _parse_allowed_push_branches()
EXTERNAL_PR_AUTOAPPROVE_PATHS = _parse_autoapprove_paths()
ALLOWED_REPOSITORIES = _parse_allowed_repositories()
ALLOWED_USERS = _parse_allowed_users()


def _cancel_scope(queue_name: str) -> str:
    return "base" if (queue_name or "").strip().endswith("-base") else "default"


def _cancel_before_key(pr_number) -> str:
    return f"pr/{pr_number}/cancel-before-{_cancel_scope(SQS_QUEUE_NAME)}"


def _approval_state_key(repo: str, pr_number: int) -> str:
    repo_key = (repo or "").replace("/", "__")
    return f"{APPROVAL_STATE_PREFIX}/{repo_key}/pr/{pr_number}.json"


def _approval_external_id(repo: str, pr_number: int, head_sha: str) -> str:
    return json.dumps(
        {
            "kind": APPROVAL_EXTERNAL_ID_KIND,
            "repo": repo,
            "pr_number": pr_number,
            "head_sha": head_sha,
        },
        sort_keys=True,
    )


def _parse_approval_external_id(value: str):
    if not value:
        return None
    try:
        data = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    if data.get("kind") != APPROVAL_EXTERNAL_ID_KIND:
        return None
    return data


def _get_raw_body(event) -> str:
    body = event.get("body", "")
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return body


def verify_github_signature(event) -> None:
    if not WEBHOOK_SECRET:
        print("WARNING: GH_WEBHOOK_SECRET not set, skipping signature verification")
        return

    headers = event.get("headers") or {}
    signature = headers.get("X-Hub-Signature-256") or headers.get(
        "x-hub-signature-256"
    )
    if not signature:
        raise ValueError("Missing X-Hub-Signature-256 header")

    raw_body = _get_raw_body(event)
    expected = (
        "sha256="
        + hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            raw_body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
    )

    if not hmac.compare_digest(expected, signature):
        raise ValueError("Invalid GitHub webhook signature")


def _is_external_pr(pr, repo_full_name: str) -> bool:
    head_repo = pr.get("head", {}).get("repo", {}) or {}
    base_repo = pr.get("base", {}).get("repo", {}) or {}
    head_full_name = head_repo.get("full_name", "")
    base_full_name = base_repo.get("full_name", "") or repo_full_name
    return bool(head_repo.get("fork")) or (
        head_full_name and base_full_name and head_full_name != base_full_name
    )


def _is_allowed_repository(repo_full_name: str) -> bool:
    return not ALLOWED_REPOSITORIES or repo_full_name in ALLOWED_REPOSITORIES


def _build_workflow(action, payload, event_ts):
    """Build a CI workflow message from a pull_request event. Returns None to skip."""
    if action not in ("opened", "synchronize", "reopened", "rerequested"):
        return None

    pr = payload.get("pull_request", {})
    repo = payload.get("repository", {}).get("full_name", "")
    return {
        "type": "pull_request",
        "action": action,
        "event_ts": event_ts,
        "pr_number": pr.get("number"),
        "head_sha": pr.get("head", {}).get("sha", ""),
        "head_ref": pr.get("head", {}).get("ref", ""),
        "base_ref": pr.get("base", {}).get("ref", ""),
        "repo": repo,
        "sender": payload.get("sender", {}).get("login", ""),
        "title": pr.get("title", ""),
        "draft": pr.get("draft", False),
        "labels": [label.get("name", "") for label in pr.get("labels", [])],
        "external_pr": _is_external_pr(pr, repo),
        "head_repo": pr.get("head", {}).get("repo", {}).get("full_name", ""),
    }


def _build_push_workflow(payload, event_ts):
    """Build a CI workflow message from a push event. Returns None to skip
    (refs that aren't branch pushes, or branches not on the allow-list)."""
    ref = payload.get("ref", "")
    if not ref.startswith("refs/heads/"):
        return None
    branch = ref[len("refs/heads/") :]
    if branch not in ALLOWED_PUSH_BRANCHES:
        return None
    head_sha = payload.get("after") or payload.get("head_commit", {}).get("id", "")
    if not head_sha:
        return None
    return {
        "type": "push",
        "event_ts": event_ts,
        "head_ref": branch,
        "head_sha": head_sha,
        "repo": payload.get("repository", {}).get("full_name", ""),
        "sender": payload.get("sender", {}).get("login", ""),
    }


def _build_rerun_workflow(check_obj, payload, event_ts):
    """Build a rerun workflow message from a check_suite or check_run payload.

    Returns (workflow_dict, pr_number) or (None, None) if the event has no
    associated PR (e.g. a push-based check suite with no PR).
    """
    prs = check_obj.get("pull_requests", [])
    if not prs:
        return None, None
    pr = prs[0]
    pr_number = pr.get("number")
    head_sha = check_obj.get("head_sha", "")
    if not pr_number or not head_sha:
        return None, None
    repo = payload.get("repository", {}).get("full_name", "")
    workflow = {
        "type": "pull_request",
        "action": "rerequested",
        "event_ts": event_ts,
        "pr_number": pr_number,
        "head_sha": head_sha,
        "head_ref": pr.get("head", {}).get("ref", ""),
        "base_ref": pr.get("base", {}).get("ref", ""),
        "repo": repo,
        "sender": payload.get("sender", {}).get("login", ""),
        "title": "",
        "draft": False,
        "labels": [],
        "external_pr": False,
        "head_repo": "",
    }
    return workflow, pr_number


_sqs_queue_url = None
_sqs_client = None
_s3_client = None
_lambda_client = None
_gh_token_cache = {"token": "", "expires_at": 0.0}


def _sqs():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs")
    return _sqs_client


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def _lambda():
    global _lambda_client
    if _lambda_client is None:
        _lambda_client = boto3.client("lambda")
    return _lambda_client


def _get_queue_url():
    global _sqs_queue_url
    if _sqs_queue_url is None:
        _sqs_queue_url = _sqs().get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]
    return _sqs_queue_url


def _is_no_such_key(error: Exception) -> bool:
    code = (
        getattr(error, "response", {})
        .get("Error", {})
        .get("Code", "")
    )
    return code in ("NoSuchKey", "404", "NotFound")


def _load_approval_state(repo: str, pr_number: int):
    if not S3_BUCKET:
        return None
    try:
        response = _s3().get_object(
            Bucket=S3_BUCKET, Key=_approval_state_key(repo, pr_number)
        )
    except Exception as e:
        if _is_no_such_key(e):
            return None
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def _store_approval_state(repo: str, pr_number: int, state: dict) -> None:
    if not S3_BUCKET:
        print("  [warn] S3_BUCKET not set; cannot persist approval state")
        return
    _s3().put_object(
        Bucket=S3_BUCKET,
        Key=_approval_state_key(repo, pr_number),
        Body=json.dumps(state, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def _permission_satisfies(actual: str, required: str) -> bool:
    return _PERMISSION_LEVELS.get(actual or "none", -1) >= _PERMISSION_LEVELS.get(
        required, -1
    )


def _get_github_token(required_permissions=None) -> str:
    now = time.time()
    if (
        _gh_token_cache["token"]
        and _gh_token_cache["expires_at"]
        and now < (_gh_token_cache["expires_at"] - 60)
    ):
        return _gh_token_cache["token"]
    if not GH_AUTH_LAMBDA_NAME:
        raise RuntimeError("GH_AUTH_LAMBDA_NAME is not configured")
    response = _lambda().invoke(
        FunctionName=GH_AUTH_LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    payload = response["Payload"].read().decode("utf-8")
    data = json.loads(payload)
    if "FunctionError" in response:
        raise RuntimeError("GH auth lambda failed (payload redacted)")
    if isinstance(data, dict) and "statusCode" in data:
        if int(data.get("statusCode", 500)) >= 400:
            raise RuntimeError(
                f"GH auth lambda returned statusCode={data.get('statusCode')}"
            )
        body = data.get("body", "{}")
        data = json.loads(body) if isinstance(body, str) else body
    permissions = data.get("permissions") or {}
    for name, required in (required_permissions or {}).items():
        if not _permission_satisfies(permissions.get(name), required):
            raise RuntimeError(
                f"GH auth token lacks required permission {name}={required}"
            )
    _gh_token_cache["token"] = data["token"]
    expires_at = (data.get("expires_at") or "").replace("Z", "+00:00")
    if expires_at:
        _gh_token_cache["expires_at"] = time.mktime(
            time.strptime(expires_at[:19], "%Y-%m-%dT%H:%M:%S")
        )
    else:
        _gh_token_cache["expires_at"] = now + 300
    return _gh_token_cache["token"]


def _gh_api(method: str, path: str, token: str, body=None):
    request = urllib.request.Request(
        urllib.parse.urljoin(f"{GITHUB_API_BASE}/", path.lstrip("/")),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        data=(json.dumps(body).encode("utf-8") if body is not None else None),
        method=method,
    )
    if body is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read().decode("utf-8")
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {path} failed: HTTP {e.code}: {detail}")


def _gh_check_output(title: str, summary: str, text: str = "") -> dict:
    return {"title": title, "summary": summary, "text": text}


def _best_effort(label: str, fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        print(f"  [warn] {label} failed: {e}")
        return None


def _create_gate_check(
    repo: str,
    pr_number: int,
    head_sha: str,
    token: str,
    status: str,
    title: str,
    summary: str,
    text: str = "",
    conclusion: str | None = None,
):
    body = {
        "name": APPROVAL_CHECK_NAME,
        "head_sha": head_sha,
        "status": status,
        "details_url": f"https://github.com/{repo}/pull/{pr_number}",
        "external_id": _approval_external_id(repo, pr_number, head_sha),
        "output": _gh_check_output(title, summary, text),
    }
    if status != "completed":
        body["actions"] = [
            {
                "label": "Approve CI",
                "description": "Allow CI to run for this commit",
                "identifier": "approve",
            },
        ]
    if conclusion is not None:
        body["conclusion"] = conclusion
    return _gh_api("POST", f"/repos/{repo}/check-runs", token, body)


def _update_gate_check(
    repo: str,
    check_id: int,
    token: str,
    status: str | None = None,
    title: str | None = None,
    summary: str | None = None,
    text: str | None = None,
    conclusion: str | None = None,
):
    body = {}
    if status is not None:
        body["status"] = status
    if conclusion is not None:
        body["conclusion"] = conclusion
    if title is not None or summary is not None or text is not None:
        body["output"] = _gh_check_output(title or "", summary or "", text or "")
    if not body:
        return {}
    return _gh_api("PATCH", f"/repos/{repo}/check-runs/{check_id}", token, body)


def _get_user_permission(repo: str, login: str, token: str) -> str:
    data = _gh_api(
        "GET",
        f"/repos/{repo}/collaborators/{urllib.parse.quote(login)}/permission",
        token,
    )
    return data.get("permission", "none")


def _can_maintain_repo(repo: str, login: str, token: str) -> bool:
    return _permission_satisfies(_get_user_permission(repo, login, token), "write")


def _compare_changed_files(repo: str, base_sha: str, head_sha: str, token: str):
    data = _gh_api("GET", f"/repos/{repo}/compare/{base_sha}...{head_sha}", token)
    return [f.get("filename", "") for f in data.get("files", []) if f.get("filename")]


def _path_is_allowed(path: str) -> bool:
    pure_path = PurePosixPath(path)
    for pattern in EXTERNAL_PR_AUTOAPPROVE_PATHS:
        candidates = {pattern}
        if "/**/" in pattern:
            candidates.add(pattern.replace("/**/", "/"))
        for candidate in candidates:
            if pure_path.match(candidate):
                return True
    return False


def _changes_are_autoapprovable(repo: str, base_sha: str, head_sha: str, token: str):
    if not EXTERNAL_PR_AUTOAPPROVE_PATHS:
        return False
    try:
        changed_files = _compare_changed_files(repo, base_sha, head_sha, token)
    except Exception as e:
        print(f"  [warn] compare for autoapprove failed: {e}")
        return False
    if not changed_files:
        return True
    return all(_path_is_allowed(path) for path in changed_files)


def _cancel_run(run_id):
    """Manual UI Cancel button: write per-run cancel-request to S3."""
    if not S3_BUCKET:
        print("  [warn] S3_BUCKET not set; cannot write cancel-request")
        return
    key = f"runs/{run_id}/cancel-request"
    try:
        _s3().put_object(Bucket=S3_BUCKET, Key=key, Body=b"requested")
        print(f"CANCEL request written: s3://{S3_BUCKET}/{key}")
    except Exception as e:
        print(f"  [warn] could not write cancel-request: {e}")


def _cancel_runs_before(pr_number, event_ts, head_sha=""):
    """New push: write the scoped ``cancel-before`` marker for older SHAs.

    The marker is PR-scoped, so include both the event timestamp and the new
    head SHA. The orchestrator only self-cancels when it sees a newer marker
    for a *different* SHA; this avoids false cancels when an approved external
    PR re-enqueues the current head after the marker was already written.
    """
    if not S3_BUCKET:
        print("  [warn] S3_BUCKET not set; cannot write cancel-before")
        return
    key = _cancel_before_key(pr_number)
    try:
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps({"ts": event_ts, "head_sha": head_sha or ""}).encode(),
            ContentType="application/json",
        )
        print(f"CANCEL-BEFORE written: s3://{S3_BUCKET}/{key} ts={event_ts:.0f}")
    except Exception as e:
        print(f"  [warn] could not write cancel-before: {e}")


def _enqueue(workflow, delivery_id):
    """Send a CI workflow trigger to SQS."""
    _sqs().send_message(
        QueueUrl=_get_queue_url(),
        MessageBody=json.dumps(workflow),
        MessageAttributes={
            "delivery_id": {"DataType": "String", "StringValue": delivery_id},
        },
    )
    label = workflow["type"]
    if workflow.get("action"):
        label += f".{workflow['action']}"
    target = (
        f"PR#{workflow['pr_number']}"
        if workflow.get("pr_number")
        else f"branch={workflow.get('head_ref', '?')}"
    )
    print(f"ENQUEUED: {label} {target} delivery={delivery_id}")


def _supersede_previous_gate(state: dict | None, token: str):
    # Every new external-PR head gets its own approval check bound to that SHA.
    # When a newer head arrives, close the older waiting gate so the PR does not
    # accumulate multiple live "Approve CI" buttons for stale commits.
    if not state:
        return
    check_id = state.get("approval_check_id")
    repo = state.get("repo", "")
    if not check_id or not repo:
        return
    if state.get("status") == "awaiting":
        _best_effort(
            "supersede previous approval gate",
            _update_gate_check,
            repo,
            int(check_id),
            token,
            status="completed",
            conclusion="neutral",
            title="Superseded by a newer commit",
            summary="This approval request no longer applies because the PR head changed.",
        )


def _store_gate_state(
    workflow: dict,
    check_id: int,
    status: str,
    approved_by: str = "",
):
    state = {
        "repo": workflow["repo"],
        "pr_number": workflow["pr_number"],
        "head_sha": workflow["head_sha"],
        "approval_check_id": int(check_id),
        "status": status,
        "approved_by": approved_by,
        "workflow": workflow,
        "updated_at": time.time(),
    }
    _store_approval_state(workflow["repo"], workflow["pr_number"], state)
    return state


def _autoapprove_summary(state: dict) -> tuple[str, str]:
    approved_by = (state.get("approved_by") or "").strip() or "a maintainer"
    approved_sha = (state.get("head_sha") or "").strip()
    short_sha = approved_sha[:12] if approved_sha else "unknown"
    return (
        f"Only autoapproved paths changed since approval by {approved_by} on commit {short_sha}.",
        f"Previous approved commit: `{short_sha}`\nApproved by: `{approved_by}`",
    )


def _handle_external_pr(workflow: dict, delivery_id: str):
    token = _get_github_token(
        required_permissions={"checks": "write", "metadata": "read"}
    )
    previous_state = _load_approval_state(workflow["repo"], workflow["pr_number"])
    _supersede_previous_gate(previous_state, token)

    autoapproved = (
        previous_state is not None
        and previous_state.get("status") == "approved"
        and previous_state.get("head_sha")
        and _changes_are_autoapprovable(
            workflow["repo"],
            previous_state["head_sha"],
            workflow["head_sha"],
            token,
        )
    )
    if autoapproved:
        summary, text = _autoapprove_summary(previous_state)
        original_approver = (
            (previous_state.get("approved_by") or "").strip() or "a maintainer"
        )
        check = _create_gate_check(
            workflow["repo"],
            workflow["pr_number"],
            workflow["head_sha"],
            token,
            status="completed",
            conclusion="success",
            title="External PR approval reused",
            summary=summary,
            text=text,
        )
        _store_gate_state(
            workflow,
            int(check["id"]),
            "approved",
            approved_by=original_approver,
        )
        _enqueue(workflow, delivery_id)
        print(
            f"AUTOAPPROVED: PR#{workflow['pr_number']} sha={workflow['head_sha'][:12]}"
        )
        return

    check = _create_gate_check(
        workflow["repo"],
        workflow["pr_number"],
        workflow["head_sha"],
        token,
        status="in_progress",
        title="Awaiting maintainer approval",
        summary="This PR comes from an external fork. CI will stay blocked until a maintainer approves this exact commit.",
        text="Use the check actions below to approve or reject this commit for CI.",
    )
    _store_gate_state(workflow, int(check["id"]), "awaiting")
    print(f"AWAITING APPROVAL: PR#{workflow['pr_number']} sha={workflow['head_sha'][:12]}")


def _approve_saved_workflow(state: dict, delivery_id: str, sender: str, reason: str):
    workflow = dict(state.get("workflow") or {})
    if not workflow:
        raise RuntimeError("Missing workflow payload in approval state")
    token = _get_github_token(
        required_permissions={"checks": "write", "metadata": "read"}
    )
    _best_effort(
        "mark approval gate approved",
        _update_gate_check,
        workflow["repo"],
        int(state["approval_check_id"]),
        token,
        status="completed",
        conclusion="success",
        title="Approved",
        summary=reason,
        text=f"Approved by `{sender}`.",
    )
    _store_gate_state(workflow, int(state["approval_check_id"]), "approved", sender)
    _enqueue(workflow, delivery_id)


def _handle_gate_action(payload, delivery_id: str, sender: str, identifier: str):
    check_run = payload.get("check_run", {})
    context = _parse_approval_external_id(check_run.get("external_id", ""))
    if not context:
        print("SKIP: requested_action is not for an external PR approval check")
        return

    repo = context["repo"]
    pr_number = int(context["pr_number"])
    head_sha = context["head_sha"]
    token = _get_github_token(
        required_permissions={"checks": "write", "metadata": "read"}
    )
    if not _can_maintain_repo(repo, sender, token):
        print(f"SKIP: {sender} lacks write permission for {repo}")
        return

    state = _load_approval_state(repo, pr_number)
    if not state:
        print(f"SKIP: no approval state found for PR#{pr_number}")
        return
    if state.get("approval_check_id") != check_run.get("id") or state.get("head_sha") != head_sha:
        _best_effort(
            "mark stale approval request",
            _update_gate_check,
            repo,
            int(check_run["id"]),
            token,
            status="completed",
            conclusion="neutral",
            title="Stale approval request",
            summary="This approval request no longer matches the current PR head.",
        )
        print(f"STALE APPROVAL ACTION: PR#{pr_number} sha={head_sha[:12]}")
        return

    if identifier == "approve":
        _approve_saved_workflow(
            state,
            delivery_id,
            sender,
            "A maintainer approved this external PR for CI.",
        )
        print(f"APPROVED: PR#{pr_number} sha={head_sha[:12]} by {sender}")
        return

    print(f"SKIP: unknown approval action {identifier}")


def _handle_external_rerun(workflow: dict, delivery_id: str, sender: str):
    token = _get_github_token(
        required_permissions={"checks": "write", "metadata": "read"}
    )
    if not _can_maintain_repo(workflow["repo"], sender, token):
        print(f"SKIP: external PR rerun by non-maintainer {sender}")
        return
    state = _load_approval_state(workflow["repo"], workflow["pr_number"])
    if state and state.get("head_sha") == workflow["head_sha"] and state.get(
        "approval_check_id"
    ):
        _best_effort(
            "mark approval gate approved from rerun",
            _update_gate_check,
            workflow["repo"],
            int(state["approval_check_id"]),
            token,
            status="completed",
            conclusion="success",
            title="Approved",
            summary="A maintainer explicitly reran CI for this external PR commit.",
            text=f"Approved by maintainer rerun from `{sender}`.",
        )
        _store_gate_state(workflow, int(state["approval_check_id"]), "approved", sender)
    _enqueue(workflow, delivery_id)
    print(f"RERUN APPROVED: PR#{workflow['pr_number']} sha={workflow['head_sha'][:12]}")


def lambda_handler(event, context):
    try:
        verify_github_signature(event)
    except Exception as e:
        print(f"Signature verification failed: {e}")
        return {"statusCode": 401, "body": "unauthorized"}

    event_ts = time.time()

    headers = event.get("headers") or {}
    gh_event = headers.get("X-GitHub-Event") or headers.get("x-github-event", "unknown")
    delivery_id = headers.get("X-GitHub-Delivery") or headers.get(
        "x-github-delivery", ""
    )

    raw_body = _get_raw_body(event)
    try:
        payload = json.loads(raw_body)
    except (json.JSONDecodeError, TypeError):
        payload = {}

    action = payload.get("action", "")
    sender = payload.get("sender", {}).get("login", "")
    repo_full_name = payload.get("repository", {}).get("full_name", "")

    print(f"EVENT: {gh_event}.{action}  SENDER: {sender}  DELIVERY: {delivery_id}")

    if repo_full_name and not _is_allowed_repository(repo_full_name):
        print(f"SKIP: repository {repo_full_name} not in allow-list")
        return {"statusCode": 200, "body": "ok"}

    if gh_event == "check_run":
        if action == "requested_action":
            identifier = payload.get("requested_action", {}).get("identifier", "")
            if identifier == "cancel":
                check_run = payload.get("check_run", {})
                head_sha = check_run.get("head_sha", "")
                prs = check_run.get("pull_requests", [])
                pr_number = prs[0].get("number") if prs else None
                run_id = str(check_run.get("id", "")) or None
                if run_id:
                    _cancel_run(run_id)
                    print(f"MANUAL CANCEL: PR#{pr_number} run_id={run_id} sha={head_sha[:12]}")
                else:
                    print("SKIP: cancel action missing check_run id")
            else:
                _handle_gate_action(payload, delivery_id, sender, identifier)
        elif action == "rerequested":
            workflow, pr_number = _build_rerun_workflow(
                payload.get("check_run", {}), payload, event_ts
            )
            if workflow and (not ALLOWED_SENDERS or sender in ALLOWED_SENDERS):
                state = _load_approval_state(workflow["repo"], pr_number)
                if state and (state.get("workflow") or {}).get("external_pr"):
                    _handle_external_rerun(workflow, delivery_id, sender)
                else:
                    _enqueue(workflow, delivery_id)
                    print(f"RERUN (check_run): PR#{pr_number} sha={workflow['head_sha'][:12]}")
            else:
                print("SKIP: check_run.rerequested — no associated PR or sender not allowed")
        else:
            print(f"SKIP: check_run.{action} not handled")
        return {"statusCode": 200, "body": "ok"}

    if gh_event == "check_suite":
        if action == "rerequested":
            workflow, pr_number = _build_rerun_workflow(
                payload.get("check_suite", {}), payload, event_ts
            )
            if workflow and (not ALLOWED_SENDERS or sender in ALLOWED_SENDERS):
                state = _load_approval_state(workflow["repo"], pr_number)
                if state and (state.get("workflow") or {}).get("external_pr"):
                    _handle_external_rerun(workflow, delivery_id, sender)
                else:
                    _enqueue(workflow, delivery_id)
                    print(f"RERUN (check_suite): PR#{pr_number} sha={workflow['head_sha'][:12]}")
            else:
                print("SKIP: check_suite.rerequested — no associated PR or sender not allowed")
        else:
            print(f"SKIP: check_suite.{action} not handled")
        return {"statusCode": 200, "body": "ok"}

    if gh_event == "push":
        if ALLOWED_SENDERS and sender not in ALLOWED_SENDERS:
            print(f"SKIP: push sender {sender} not in allowed list")
            return {"statusCode": 200, "body": "ok"}
        workflow = _build_push_workflow(payload, event_ts)
        if workflow:
            _enqueue(workflow, delivery_id)
            print(
                f"PUSH: branch={workflow['head_ref']} sha={workflow['head_sha'][:12]}"
            )
        else:
            ref = payload.get("ref", "")
            print(f"SKIP: push ref {ref} not on the allow-list")
        return {"statusCode": 200, "body": "ok"}

    if gh_event != "pull_request":
        print("SKIP: not a pull_request event")
        return {"statusCode": 200, "body": "ok"}

    if ALLOWED_SENDERS and sender not in ALLOWED_SENDERS:
        print(f"SKIP: sender {sender} not in allowed list")
        return {"statusCode": 200, "body": "ok"}
    if ALLOWED_USERS and sender not in ALLOWED_USERS:
        print(f"SKIP: PR sender {sender} not in allowed users")
        return {"statusCode": 200, "body": "ok"}

    workflow = _build_workflow(action, payload, event_ts)
    if workflow:
        if action == "synchronize":
            _cancel_runs_before(
                workflow["pr_number"], event_ts, workflow.get("head_sha", "")
            )
        if workflow.get("external_pr"):
            _handle_external_pr(workflow, delivery_id)
        else:
            _enqueue(workflow, delivery_id)
    else:
        print(f"SKIP: action {action} does not trigger a workflow")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "event": gh_event,
            "action": action,
            "enqueued": workflow is not None and not workflow.get("external_pr", False),
        }),
    }
