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
# Per-job check external_id kind — the orchestrator stamps {run_id, job} on every
# per-job check so a `check_run.rerequested` webhook can target a single job.
# Kept in sync with praktika.orchestrator.state.JOB_CHECK_EXTERNAL_ID_KIND.
JOB_CHECK_EXTERNAL_ID_KIND = "praktika_job_check"
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
    # GitHub logins are case-insensitive; store casefolded so the membership
    # check matches regardless of the casing a deployment configured.
    return {str(user).strip().casefold() for user in value if str(user).strip()}


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


def _parse_job_check_external_id(value: str):
    """Return (run_id, job) from a per-job check's external_id, or None."""
    if not value:
        return None
    try:
        data = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(data, dict) or data.get("kind") != JOB_CHECK_EXTERNAL_ID_KIND:
        return None
    run_id = str(data.get("run_id") or "").strip()
    job = data.get("job")
    if not run_id or not job:
        return None
    return run_id, job


def _run_state_key(run_id: str) -> str:
    return f"runs/{run_id}/state.json"


def _load_run_snapshot(run_id: str):
    """Read the orchestrator's per-run state snapshot from S3, or None."""
    if not S3_BUCKET:
        return None
    try:
        response = _s3().get_object(Bucket=S3_BUCKET, Key=_run_state_key(run_id))
    except Exception as e:
        if _is_no_such_key(e):
            return None
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def _write_rerun_request(run_id: str, jobs, delivery_id: str) -> None:
    """Drop a per-run re-run request for the live orchestrator to pick up.

    One key per delivery so concurrent requests for the same run don't clobber;
    the orchestrator aggregates and deletes them (consume-once) in sweep_rerun.

    Raises on failure — the caller must NOT report success when the request
    wasn't actually written, or the user's re-run is silently lost.
    """
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET not set; cannot write rerun-request")
    key = f"runs/{run_id}/rerun-request/{delivery_id}.json"
    _s3().put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps({"jobs": list(jobs)}).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"RERUN request written: s3://{S3_BUCKET}/{key}")


def _is_precondition_failed(error: Exception) -> bool:
    code = (
        getattr(error, "response", {})
        .get("Error", {})
        .get("Code", "")
    )
    return code in ("PreconditionFailed", "412")


def _resume_lock_key(run_id: str) -> str:
    return f"runs/{run_id}/resume.lock"


def _claim_resume_lock(run_id: str, event_ts) -> bool:
    """Claim a finished run's *resume boot-lease* with an atomic conditional create.

    Returns True if we won the lease (the caller enqueues exactly one resume) and
    False if a resume is already being spawned for this run — in which case our
    re-run request is already in S3 and that resume will batch-drain it, so we
    must not spawn a second orchestrator.

    The lease only covers the SQS→orchestrator boot window; the resume
    orchestrator deletes it once it publishes finalized=false, and a boot crash
    is recovered by SQS redelivery — so it needs no TTL. Only a genuine
    precondition failure (the key already exists) counts as "lost the race" and
    returns False; any other S3 error is RAISED, not swallowed — swallowing it
    would strand the already-written request behind a phantom "resume in flight".
    """
    if not S3_BUCKET or not run_id:
        return False
    try:
        _s3().put_object(
            Bucket=S3_BUCKET,
            Key=_resume_lock_key(run_id),
            Body=json.dumps({"ts": event_ts}).encode("utf-8"),
            IfNoneMatch="*",
            ContentType="application/json",
        )
        return True
    except Exception as e:
        if _is_precondition_failed(e):
            return False  # another resume already claimed this run — real contention
        # Unexpected error: propagate so the webhook fails visibly / can be
        # retried, rather than returning False and stranding the request.
        raise


def _release_resume_lock(run_id: str) -> None:
    """Best-effort delete of the resume boot-lease.

    Used to roll back a claimed lock when the resume message could not be
    enqueued, so a webhook retry can re-claim and re-send instead of finding a
    stranded lock that makes it wrongly conclude a resume is already in flight.
    """
    if not S3_BUCKET or not run_id:
        return
    try:
        _s3().delete_object(Bucket=S3_BUCKET, Key=_resume_lock_key(run_id))
    except Exception as e:
        print(f"  [warn] could not release resume lock for {run_id}: {e}")


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


def _pr_metadata(pr, repo):
    """Normalize a PR object into the metadata fields a workflow message carries.

    One definition of "what we read off a PR", shared by the two ways a PR
    reaches us: the ``pull_request`` webhook (whose payload embeds the full PR
    object) and a PR refetched by number for reruns (whose ``check_run`` /
    ``check_suite`` payload omits labels/title/draft/fork). Keeping this in one
    place is what makes an internal-PR rerun schedule the same DAG the live PR
    would — see ``_fetch_pr``.
    """
    head = pr.get("head", {}) or {}
    base = pr.get("base", {}) or {}
    return {
        "head_ref": head.get("ref", ""),
        "base_ref": base.get("ref", ""),
        "title": pr.get("title", ""),
        "draft": pr.get("draft", False),
        "labels": [label.get("name", "") for label in pr.get("labels", [])],
        "external_pr": _is_external_pr(pr, repo),
        "head_repo": head.get("repo", {}).get("full_name", ""),
    }


def _fetch_pr(repo, pr_number):
    """Authoritative live PR state by number: ``(current_head_sha, metadata)``.

    The single source of truth for both trigger paths. ``check_run`` /
    ``check_suite`` rerun payloads carry only the PR number and a sha, not
    labels/title/draft/fork status; and even a ``pull_request`` payload can be a
    delayed/redelivered older event. Refetching the live PR lets callers (a)
    detect a stale event by comparing ``current_head_sha`` and (b) classify and
    DAG-filter off the current PR state. Raises on any error so callers decide
    whether to fail closed (rerun) or fall back to the payload (live event).
    """
    token = _get_github_token(required_permissions={"metadata": "read"})
    pr = _gh_api("GET", f"/repos/{repo}/pulls/{pr_number}", token)
    return pr.get("head", {}).get("sha", ""), _pr_metadata(pr, repo)


def _build_pr_workflow(action, repo, pr_number, head_sha, sender, event_ts, meta):
    """Assemble a pull_request workflow trigger message.

    Single definition of the message shape; callers supply the metadata dict
    (from ``_pr_metadata`` for a live payload, or ``_fetch_pr`` for a rerun).
    ``meta`` last so its keys can't be silently overridden by a caller.
    """
    return {
        "type": "pull_request",
        "action": action,
        "event_ts": event_ts,
        "pr_number": pr_number,
        "head_sha": head_sha,
        "repo": repo,
        "sender": sender,
        **meta,
    }


def _build_workflow(action, payload, event_ts):
    """Build a CI workflow message from a pull_request event. Returns None to skip."""
    if action not in ("opened", "synchronize", "reopened", "rerequested"):
        return None

    pr = payload.get("pull_request", {})
    repo = payload.get("repository", {}).get("full_name", "")
    return _build_pr_workflow(
        action,
        repo,
        pr.get("number"),
        pr.get("head", {}).get("sha", ""),
        payload.get("sender", {}).get("login", ""),
        event_ts,
        _pr_metadata(pr, repo),
    )


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
    """Return (changed_paths, complete) for the diff ``base_sha...head_sha``.

    ``changed_paths`` includes both sides of a rename (``filename`` and
    ``previous_filename``) so a move out of a disallowed tree cannot hide the
    original path. GitHub caps the compare ``files`` array at 300 entries and
    does not paginate it, so ``complete`` is False once that cap is reached;
    callers must then fail closed rather than trust a truncated list.
    """
    data = _gh_api("GET", f"/repos/{repo}/compare/{base_sha}...{head_sha}", token)
    files = data.get("files", []) or []
    changed_paths = set()
    for f in files:
        for key in ("filename", "previous_filename"):
            value = f.get(key)
            if value:
                changed_paths.add(value)
    complete = len(files) < 300
    return changed_paths, complete


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
        changed_paths, complete = _compare_changed_files(repo, base_sha, head_sha, token)
    except Exception as e:
        print(f"  [warn] compare for autoapprove failed: {e}")
        return False
    if not complete:
        # The changed-file list hit GitHub's 300-file cap and may be truncated;
        # we cannot prove every changed path is autoapprovable, so fall back to
        # manual maintainer approval instead of autoapproving a partial view.
        print("  [warn] changed-file list may be truncated (>=300 files); requiring manual approval")
        return False
    if not changed_paths:
        return True
    return all(_path_is_allowed(path) for path in changed_paths)


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


def _handle_rerun(check_obj, payload, delivery_id, sender, event_ts, source):
    """Route a ``check_run`` / ``check_suite`` ``rerequested`` event.

    A single per-job check carries ``{run_id, job}`` in its ``external_id``, so a
    re-run of one check becomes a *partial* re-run — only that job (and its
    failed downstream) re-runs, in place on the existing run. Anything without
    that marker (the check_suite "re-run all", the top-level check, or a run from
    before external_ids) falls back to a full-workflow re-run.
    """
    # Neither path rate-limits: the partial-rerun path is serialized by a per-run
    # resume.lock and absorbs unlimited live requests; the full-rerun path mints a
    # fresh run per click (rapid clicks just waste compute — see _handle_full_rerun).
    parsed = _parse_job_check_external_id(check_obj.get("external_id", ""))
    if parsed:
        _handle_partial_rerun(parsed[0], parsed[1], check_obj, payload, delivery_id, sender, event_ts)
        return
    _handle_full_rerun(check_obj, payload, delivery_id, sender, event_ts, source)


def _handle_partial_rerun(run_id, job, check_obj, payload, delivery_id, sender, event_ts):
    """Re-run a single failed job (+ its failed downstream) on an existing run.

    Every re-run first records the job under ``runs/<run_id>/rerun-request/`` —
    the single request log that both paths drain. Running vs finished is then
    decided from S3, never the GitHub API, via the ``finalized`` flag in
    ``runs/<run_id>/state.json``:
      - running (not finalized, or no snapshot yet) → nothing more to do; the
        live orchestrator's ``sweep_rerun`` picks up the request we just wrote.
      - finished (finalized) → claim the per-run ``resume.lock`` and, if we win
        it, enqueue one ``rerun`` message; a fresh orchestrator reloads the
        snapshot, batch-drains all pending requests, and re-drives.
    """
    repo = payload.get("repository", {}).get("full_name", "")
    if ALLOWED_SENDERS and sender not in ALLOWED_SENDERS:
        print(f"SKIP: partial rerun — sender {sender} not allowed")
        return

    # Runners check out the LIVE PR head (refs/pull/N/head), not the sha of the
    # check being rerun. So before re-running an old check we must confirm the
    # PR head has not advanced — otherwise this would run the current (possibly
    # unapproved fork) code under the old check's authorization. Fetch the PR;
    # reject on mismatch, and for fork PRs also require a maintainer. Fail closed
    # on any error / missing PR.
    rerun_sha = check_obj.get("head_sha", "")
    prs = check_obj.get("pull_requests", [])
    pr_number = prs[0].get("number") if prs else None
    if not pr_number or not rerun_sha:
        print(f"SKIP: partial rerun run={run_id} — missing PR number or head sha")
        return
    try:
        current_sha, meta = _fetch_pr(repo, pr_number)
    except Exception as e:
        print(f"SKIP: partial rerun run={run_id} — could not verify PR head, failing closed: {e}")
        return
    if current_sha != rerun_sha:
        print(
            f"SKIP: partial rerun run={run_id} — stale (rerun sha {rerun_sha[:12]} "
            f"!= current head {current_sha[:12]})"
        )
        return
    if meta["external_pr"]:
        token = _get_github_token(required_permissions={"metadata": "read"})
        if not _can_maintain_repo(repo, sender, token):
            print(f"SKIP: partial rerun run={run_id} — fork PR rerun by non-maintainer {sender}")
            return

    # Record the request in S3 FIRST — before reading finalized — so a run that
    # finalizes concurrently can't strand it: the orchestrator's finalize recheck
    # sweep runs after this write (see _drive_dag). One key per delivery,
    # consumed once. This is the single request log for both paths; the live
    # orchestrator's sweep_rerun drains it on a running run, a resume drains it on
    # a finished one. No throttle — the resume.lock below serializes spawns
    # without rate-limiting the user, and a running orchestrator absorbs any
    # number of requests (bounded per job by MAX_RERUNS_PER_JOB).
    # These handoff steps must not silently claim success on failure — that
    # loses the user's click (the running-run request would be absent, or a
    # finished run would never get a resume). Propagate so the webhook surfaces
    # a failure and can be retried, rather than returning HTTP 200.
    _write_rerun_request(run_id, [job], delivery_id)

    snap = _load_run_snapshot(run_id)

    if not (snap and snap.get("finalized")):
        # Running (or snapshot not written yet): the live orchestrator's sweep
        # picks up the request we just wrote. Nothing else to do.
        print(f"RERUN (partial, live): run={run_id} job={job!r}")
        return

    # Finished: spawn a resume — but only one. resume.lock is a per-run boot lease
    # claimed with an atomic conditional create; the winner enqueues the resume
    # and its orchestrator batch-drains every pending request (this one plus any
    # from clicks that lost the lease). Losers just return — their request is
    # already in S3.
    if not _claim_resume_lock(run_id, event_ts):
        print(
            f"RERUN (partial, resume): run={run_id} job={job!r} — "
            f"resume already in flight; request queued"
        )
        return

    # Carry the authoritative PR metadata (fork repo, labels, title, draft, refs)
    # so the resume reconstructs a real pull_request context even when the run's
    # snapshot has no environment to inherit it from — otherwise a fork PR could
    # look internal and lose its metadata (see _orchestrate_resume).
    workflow = {
        "type": "rerun",
        "run_id": run_id,
        "rerun_jobs": [job],
        "repo": snap.get("repo") or repo,
        "head_sha": snap.get("head_sha", ""),
        "pr_number": snap.get("pr_number"),
        "sender": sender,
        "event_ts": event_ts,
        **meta,
    }
    try:
        _enqueue(workflow, delivery_id)
    except Exception:
        # The lock is claimed but the resume was never sent — release it so a
        # retry re-claims and re-enqueues instead of seeing a stranded lock and
        # wrongly concluding a resume is already in flight. Then propagate.
        _release_resume_lock(run_id)
        raise
    print(f"RERUN (partial, resume, spawned): run={run_id} job={job!r}")


def _handle_full_rerun(check_obj, payload, delivery_id, sender, event_ts, source):
    """Full-workflow re-run (check_suite "re-run all", or a pre-external_id run).

    A rerun button only exists on a check that was already created for this exact
    sha, so a rerun of a real CI check is inherently a rerun of a sha that ran
    before (and, for a fork PR, was already approved). We refetch the PR by
    number: it is the single authoritative source of fork status (which decides
    internal vs external routing) and of the labels/title/draft that the check
    payload omits but the DAG is filtered on. ``source`` is "check_run" /
    "check_suite" for logging.
    """
    prs = check_obj.get("pull_requests", [])
    if not prs:
        print(f"SKIP: {source}.rerequested — no associated PR")
        return
    pr_number = prs[0].get("number")
    rerun_sha = check_obj.get("head_sha", "")
    repo = payload.get("repository", {}).get("full_name", "")
    if not pr_number or not rerun_sha:
        print(f"SKIP: {source}.rerequested — missing PR number or head sha")
        return
    if ALLOWED_SENDERS and sender not in ALLOWED_SENDERS:
        print(f"SKIP: {source}.rerequested — sender {sender} not allowed")
        return

    # One fetch gives the fork status (routing), the DAG-shaping metadata the
    # check payload lacks, and the live head sha. Fail closed on any error so a
    # fork PR can never slip onto the ungated internal enqueue path.
    try:
        current_sha, meta = _fetch_pr(repo, pr_number)
    except Exception as e:
        print(
            f"SKIP: {source}.rerequested — could not fetch PR#{pr_number}, "
            f"failing closed: {e}"
        )
        return

    # Runners check out the live PR head, not rerun_sha. If the head has advanced
    # since this check ran, re-running it would execute the current (possibly
    # unapproved fork) code under this old check's identity — reject as stale.
    if current_sha != rerun_sha:
        print(
            f"SKIP: {source}.rerequested — stale (rerun sha {rerun_sha[:12]} "
            f"!= current head {current_sha[:12]})"
        )
        return

    # No throttle / dedup here: a full re-run mints a brand-new run (new run_id,
    # new S3 prefix) and writes no cancel marker, so two rapid "re-run all" clicks
    # simply spawn two independent full workflows — wasteful, but harmless (see
    # PROTOCOL.md). Users should avoid mashing the button.
    workflow = _build_pr_workflow(
        "rerequested", repo, pr_number, rerun_sha, sender, event_ts, meta
    )

    if not meta["external_pr"]:
        # Same-repo PR: trusted code, no approval gate. Enqueue directly.
        _enqueue(workflow, delivery_id)
        print(f"RERUN ({source}): PR#{pr_number} sha={rerun_sha[:12]}")
        return

    _handle_external_rerun(workflow, delivery_id, sender)


def _handle_external_rerun(workflow: dict, delivery_id: str, sender: str):
    """Enqueue a fork-PR rerun once it is authorized.

    Metadata (labels/fork/etc.) is already authoritative on ``workflow`` from the
    refetch in ``_handle_rerun``; this only re-derives *authorization*, which is
    the one thing the internal path doesn't need:

    - Require a maintainer. GitHub only shows the re-run button to write-access
      users, but the webhook sender is not otherwise trusted for an authz call,
      and this also blocks re-running the "awaiting approval" gate check of an
      unapproved commit.
    - Reject a rerun whose sha is no longer the approved head. Old check runs
      persist after the PR head advances; a rerun of a stale check must run that
      stale sha at most, never be treated as approval of the current head.
    """
    repo = workflow["repo"]
    pr_number = workflow["pr_number"]
    rerun_sha = workflow["head_sha"]
    token = _get_github_token(
        required_permissions={"checks": "write", "metadata": "read"}
    )
    if not _can_maintain_repo(repo, sender, token):
        print(f"SKIP: external PR rerun by non-maintainer {sender}")
        return
    state = _load_approval_state(repo, pr_number)
    if not state or state.get("head_sha") != rerun_sha:
        print(
            f"SKIP: stale external rerun of PR#{pr_number} "
            f"sha={rerun_sha[:12]} current={(state or {}).get('head_sha', '')[:12]}"
        )
        return
    if state.get("approval_check_id"):
        _best_effort(
            "mark approval gate approved from rerun",
            _update_gate_check,
            repo,
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
    print(f"RERUN APPROVED: PR#{pr_number} sha={rerun_sha[:12]}")


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
            _handle_rerun(
                payload.get("check_run", {}),
                payload,
                delivery_id,
                sender,
                event_ts,
                "check_run",
            )
        else:
            print(f"SKIP: check_run.{action} not handled")
        return {"statusCode": 200, "body": "ok"}

    if gh_event == "check_suite":
        if action == "rerequested":
            _handle_rerun(
                payload.get("check_suite", {}),
                payload,
                delivery_id,
                sender,
                event_ts,
                "check_suite",
            )
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
    if ALLOWED_USERS and sender.casefold() not in ALLOWED_USERS:
        print(f"SKIP: PR sender {sender} not in allowed users")
        return {"statusCode": 200, "body": "ok"}

    def _pr_response(enqueued):
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {"ok": True, "event": gh_event, "action": action, "enqueued": enqueued}
            ),
        }

    workflow = _build_workflow(action, payload, event_ts)
    if not workflow:
        print(f"SKIP: action {action} does not trigger a workflow")
        return _pr_response(False)

    # Refetch the live PR by number — the same authoritative source the rerun
    # path uses. The webhook payload is only ordered by Lambda receive time, so a
    # delayed or redelivered older event (e.g. a `synchronize` for commit A) can
    # arrive after the head advanced to B; acting on it would write a
    # cancel-before marker that cancels the running B (and, for external PRs,
    # overwrite the approval state with A). Drop any event whose head no longer
    # matches the live PR, and otherwise replace the payload metadata with the
    # live labels/title/draft/fork so every trigger schedules the DAG from one
    # place. On fetch failure: for an internal PR fall back to the payload
    # (availability over the rare reorder-during-outage); for an external PR fail
    # closed — the payload alone can't prove the event isn't a stale/reordered
    # one for a superseded commit, and running the live fork head under a stale
    # event's authority (or overwriting approval state) must never happen.
    try:
        current_sha, meta = _fetch_pr(workflow["repo"], workflow["pr_number"])
    except Exception as e:
        if workflow.get("external_pr"):
            print(
                f"SKIP: could not refetch external PR#{workflow['pr_number']}, "
                f"failing closed: {e}"
            )
            return _pr_response(False)
        print(
            f"  [warn] could not refetch PR#{workflow['pr_number']}, using payload "
            f"metadata: {e}"
        )
    else:
        if workflow["head_sha"] != current_sha:
            print(
                f"SKIP: stale {action} for PR#{workflow['pr_number']} "
                f"sha={workflow['head_sha'][:12]} current={current_sha[:12]}"
            )
            return _pr_response(False)
        workflow.update(meta)

    if action == "synchronize":
        _cancel_runs_before(
            workflow["pr_number"], event_ts, workflow.get("head_sha", "")
        )
    if workflow.get("external_pr"):
        _handle_external_pr(workflow, delivery_id)
        return _pr_response(False)
    _enqueue(workflow, delivery_id)
    return _pr_response(True)
