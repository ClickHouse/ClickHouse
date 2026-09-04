"""Native AI code-review job (``praktika review``).

Reviews a pull request with an LLM and posts the results back to GitHub. The
design keeps the model on a short leash: it is asked for a **structured result
only**. It may investigate the checked-out PR with read-only tools
(``read_file`` / ``grep_repo``), but it never acts on GitHub itself — it just
returns JSON describing what it found.

**Trusted Praktika code** then applies that result: a top-level summary comment,
a single batched inline review, and resolve/unresolve/reply on review threads —
and only on threads the reviewing bot itself authored (ownership is enforced
here, in code, not trusted from the model). Because the model has no shell and
no write access, a prompt-injected diff cannot make it run arbitrary commands or
touch anything outside this bounded surface.

Configuration is entirely via CLI args, so a consuming repo wires it as a plain
job command with no new config classes::

    Job.Config(
        name="Code Review",
        runs_on=RunnerLabels.SMALL_ARM,
        command="praktika review --provider bedrock-openai "
                "--model openai.gpt-oss-120b-1:0 --prompt ./ci/prompts/code_review.md",
        allow_failure=True,
        enable_gh_auth=True,
    )

The provider is any name in the AI registry (``mock`` / ``anthropic`` /
``bedrock-anthropic`` / ``bedrock-openai``); ``--model`` overrides its default model.
``--prompt`` points at a repo-local Markdown file with project-specific review
guidance, appended to the fixed review protocol.
"""
import json
import os
import re
import tempfile
import traceback

from .gh import GH
from .info import Info
from .orchestrator.ai import anthropic as _anthropic
from .orchestrator.ai.provider import resolve_provider
from .result import Result
from .utils import Utils

# Number of attempts at the model round-trip. The provider may hit a transient
# API error, or return output that is not the requested JSON; re-running the
# whole call is the simplest reliable recovery. Mirrors the CH review job.
MAX_ATTEMPTS = 3

# Read-only investigation tools offered to the model (reused from the anthropic
# provider). fetch_log is intentionally NOT offered — a review reasons over the
# diff and source, not CI logs.
_REVIEW_TOOLS = list(_anthropic._REPO_TOOLS)


def _tool_executor(name, tool_input):
    # No log URLs in a review context; pass an empty allowlist so fetch_log (if
    # ever requested) is refused while read_file / grep_repo work normally.
    return _anthropic._execute_tool(name, tool_input, set())


# JSON Schema for the review result. Passed to provider.complete() as
# response_schema so a provider that supports structured output (bedrock-openai)
# forces the model to return exactly this shape via a tool call, instead of
# emitting free-text JSON a reasoning model tends to muddle with its analysis.
_REVIEW_SCHEMA = {
    "type": "object",
    "properties": {
        "change_summary": {
            "type": "string",
            "description": (
                "1-3 sentence plain summary of WHAT this PR changes (not the "
                "review outcome). Always provide it, even when there are no "
                "findings."
            ),
        },
        "verdict": {
            "type": "string",
            "enum": ["no_issues", "nits_only", "issues_found", "blocking_issues"],
            "description": (
                "Overall result of the review: no_issues, nits_only, "
                "issues_found, or blocking_issues."
            ),
        },
        "summary_md": {
            "type": "string",
            "description": (
                "Markdown body detailing the findings (or a short 'no issues' "
                "note). Do NOT include a top-level title/heading, a change "
                "summary, or the verdict — those are rendered separately; start "
                "directly with the findings."
            ),
        },
        "inline_findings": {
            "type": "array",
            "description": "Per-line findings to post as one batched review.",
            "items": {
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "line": {"type": "integer"},
                    "side": {"type": "string", "enum": ["RIGHT", "LEFT"]},
                    "start_line": {"type": "integer"},
                    "body": {"type": "string"},
                },
                "required": ["path", "line", "body"],
            },
        },
        "thread_actions": {
            "type": "array",
            "description": "Actions on existing review threads.",
            "items": {
                "type": "object",
                "properties": {
                    "thread_id": {"type": "string"},
                    "action": {
                        "type": "string",
                        "enum": ["resolve", "unresolve", "reply"],
                    },
                    "body": {"type": "string"},
                },
                "required": ["thread_id", "action"],
            },
        },
    },
    "required": [
        "change_summary",
        "verdict",
        "summary_md",
        "inline_findings",
        "thread_actions",
    ],
}


_SYSTEM = """\
You are an automated code reviewer for a pull request. You are given the PR
title and body, the full diff, and the existing review threads (each with a
stable `thread_id`, an `authored_by_me` flag, whether it is resolved,
`path`/`line`, and all comments). The repository is checked out at the PR head.

Investigate before deciding: use `grep_repo` to locate code and `read_file` to
read the relevant source and confirm whether a concern is a real defect. Review
the current code, not only the diff or prior discussion.

A thread is yours when its `authored_by_me` is true. You may only resolve,
unresolve, or reply to threads where `authored_by_me` is true — never touch
threads started by anyone else. Treat a human reply on one of your threads as a
deliberate decision: if it explains, fixes, or dismisses the point, drop it;
only reply when the author asked you a direct question or claimed a fix that the
current code contradicts.

Do NOT re-post a finding you already raised: if a thread with `authored_by_me`
true already covers an issue at a `path`/`line`, do not add another inline
finding there. Only add an inline finding for a genuinely new issue that has no
existing thread. If the issue is now fixed, `resolve` your thread instead.

Do NOT raise issues that dedicated CI jobs already catch: build/compile errors
and style/lint/formatting. Mention those, if at all, only as nits in the summary.

Respond with ONLY a JSON object (no prose, no markdown fences) of the form:
{
  "change_summary": "<1-3 sentence plain summary of WHAT the PR changes; always fill this in, even when there are no findings>",
  "verdict": "no_issues | nits_only | issues_found | blocking_issues",
  "summary_md": "<Markdown body detailing the findings, or a short 'no issues' note. Do NOT add a title/heading, the change summary, or the verdict — those are rendered separately. Start directly with the findings, using #### or bullet sub-sections as needed>",
  "inline_findings": [
    {"path": "<repo-relative file>", "line": <int>, "side": "RIGHT",
     "start_line": <int, optional for a multi-line range>,
     "body": "<Markdown comment for this specific line>"}
  ],
  "thread_actions": [
    {"thread_id": "<id from the provided threads>",
     "action": "resolve" | "unresolve" | "reply",
     "body": "<Markdown, required only for reply>"}
  ]
}
Use "side": "LEFT" for a comment on a deleted line, otherwise "RIGHT". Only
include a finding as an inline comment when it maps to a specific changed line;
architectural notes go in `summary_md`. Return empty arrays when there is
nothing to add.
"""


def _first_object(text):
    m = re.search(r"\{.*\}", text or "", re.DOTALL)
    return m.group(0) if m else ""


def _parse_review(text):
    """Lenient parse of the model reply into the review dict. Tolerates markdown
    fences and surrounding prose. Returns {} when nothing JSON-like is found."""
    raw = (text or "").strip()
    candidate = raw
    if candidate.startswith("```"):
        candidate = re.sub(r"^```[a-zA-Z]*\n?", "", candidate)
        candidate = re.sub(r"\n?```$", "", candidate).strip()
    for attempt in (candidate, _first_object(candidate)):
        if not attempt:
            continue
        try:
            data = json.loads(attempt)
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    return {}


def _thread_authored_by_viewer(thread):
    """True if the reviewing bot (the authenticated token) authored the thread's
    first comment.

    Uses GitHub's ``viewerDidAuthor``, which the API evaluates server-side
    against the authenticated identity. It is not derivable from
    user-controllable content, so ownership can be enforced without knowing (or
    trusting) any bot login — a PR participant cannot make their thread look
    bot-authored.
    """
    nodes = (thread.get("comments") or {}).get("nodes") or []
    return bool(nodes and nodes[0].get("viewerDidAuthor"))


def _thread_first_author(thread):
    nodes = (thread.get("comments") or {}).get("nodes") or []
    if not nodes:
        return ""
    return ((nodes[0].get("author") or {}).get("login")) or ""


def _thread_first_comment_db_id(thread):
    nodes = (thread.get("comments") or {}).get("nodes") or []
    if not nodes:
        return None
    return nodes[0].get("databaseId")


def _compact_threads(threads):
    """Compact the GraphQL review threads for the prompt: drop pagination noise,
    keep what the model needs to decide (id, ownership, state, location, text)."""
    out = []
    for t in threads:
        comments = []
        for c in (t.get("comments") or {}).get("nodes") or []:
            comments.append(
                {
                    "author": (c.get("author") or {}).get("login"),
                    "body": c.get("body"),
                    "createdAt": c.get("createdAt"),
                }
            )
        out.append(
            {
                "thread_id": t.get("id"),
                "first_author": _thread_first_author(t),
                # Whether this thread is yours (the reviewing bot). Trust this,
                # not first_author, to decide resolve/unresolve/reply.
                "authored_by_me": _thread_authored_by_viewer(t),
                "isResolved": t.get("isResolved"),
                "resolvedBy": (t.get("resolvedBy") or {}).get("login"),
                "path": t.get("path"),
                "line": t.get("line"),
                "comments": comments,
            }
        )
    return out


def _build_user_content(info, diff, threads, project_prompt):
    payload = {
        "pr": {
            "number": info.pr_number,
            "title": info.pr_title,
            "body": info.pr_body,
            "url": info.pr_url,
            "head_sha": info.sha,
        },
        "review_threads": _compact_threads(threads),
        "diff": diff,
    }
    content = json.dumps(payload, indent=2)
    if project_prompt:
        content += (
            "\n\n---\nProject-specific review guidance "
            "(from the repo's prompt file):\n" + project_prompt
        )
    return content


def _run_model(provider, system, user_content):
    """Call the provider with retries; return the parsed review dict.

    An attempt succeeds only if the call returns text that parses into a dict.
    Raises RuntimeError after MAX_ATTEMPTS so the job fails loudly.
    """
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            turn = provider.complete(
                system=system,
                user_content=user_content,
                tools=_REVIEW_TOOLS,
                tool_executor=_tool_executor,
                response_schema=_REVIEW_SCHEMA,
            )
            if turn.error:
                last_error = turn.error
            else:
                review = _parse_review(turn.reasoning)
                if review:
                    return review, turn.usage
                last_error = "model reply did not parse into a JSON object"
                # Surface the raw reply (truncated) so a parse failure is
                # diagnosable from the job log instead of opaque.
                print(f"  raw model reply: {(turn.reasoning or '')[:800]!r}")
        except Exception as e:  # noqa: BLE001 — any failure here is retryable
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        print(f"WARNING: review attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
    raise RuntimeError(f"AI review failed after {MAX_ATTEMPTS} attempts: {last_error}")


# Fixed heading prepended to the review comment so it always has a consistent
# "Code Review" title and a horizontal rule above it, whatever the model wrote.
_REVIEW_HEADER = "---\n\n### Code Review\n\n"

# Human-readable label for each verdict enum value.
_VERDICT_LABELS = {
    "no_issues": "✅ No issues found",
    "nits_only": "🟢 Nits only",
    "issues_found": "⚠️ Issues found",
    "blocking_issues": "❌ Blocking issues",
}


def _investigation_footer(usage):
    """A short note on how much of the tool-call budget the review used, so a
    reader can tell whether the model had room to investigate the whole change.
    Empty when the provider ran no tool loop (max_tool_rounds == 0)."""
    total = getattr(usage, "max_tool_rounds", 0) or 0
    if not total:
        return ""
    used = getattr(usage, "tool_rounds", 0) or 0
    calls = getattr(usage, "tool_calls", 0) or 0
    if getattr(usage, "exhausted", False):
        return (
            f"> [!WARNING]\n> The reviewer used its full investigation budget "
            f"({used}/{total} rounds, {calls} tool calls) and may not have seen "
            "the whole change - the review could be incomplete."
        )
    return f"_Investigation: {used}/{total} rounds, {calls} tool calls._"


def _render_review_body(review_data, footer=""):
    """Compose the review comment: a heading, the overall result, a plain
    change summary, the findings detail, then an investigation footer. Always
    shows the result and change summary even when there are no findings."""
    verdict = review_data.get("verdict") or ""
    change_summary = (review_data.get("change_summary") or "").strip()
    summary_md = (review_data.get("summary_md") or "").strip()

    sections = []
    verdict_label = _VERDICT_LABELS.get(verdict, verdict)
    if verdict_label:
        sections.append(f"**Result:** {verdict_label}")
    if change_summary:
        sections.append(f"**What changed:** {change_summary}")
    if summary_md:
        sections.append(summary_md)
    if not sections:
        return ""
    if footer:
        sections.append(footer)
    return _REVIEW_HEADER + "\n\n".join(sections)


def _post_summary(review_data, dry_run, footer=""):
    """Post/update the top-level review comment. Returns a list of write-error
    strings (empty on success) so the caller can fail the job on a failed write
    instead of falsely reporting OK."""
    body = _render_review_body(review_data, footer)
    if not body:
        print("No summary to post")
        return []
    if dry_run:
        print(f"[dry-run] would post/update summary comment:\n{body}")
        return []
    if not GH.post_updateable_comment(comment_tags_and_bodies={"review": body}):
        return ["failed to post review summary comment"]
    return []


def _existing_bot_finding_locations(threads):
    """{(path, line)} of review threads the bot itself opened — used to avoid
    re-posting the same inline finding on a later run (which is how duplicate
    review comments accumulated). Ownership is decided by GitHub's
    ``viewerDidAuthor``, so no bot login is needed."""
    locations = set()
    for t in threads or []:
        if not _thread_authored_by_viewer(t):
            continue
        line = t.get("line")
        if line is not None:
            locations.add((t.get("path"), int(line)))
    return locations


def _post_inline_findings(findings, commit_id, dry_run, threads=None):
    findings = [f for f in (findings or []) if isinstance(f, dict) and f.get("body")]
    # Drop findings at a location the bot already commented on, so re-runs don't
    # stack duplicate inline comments on the same line.
    existing = _existing_bot_finding_locations(threads)
    if existing:
        kept = []
        for f in findings:
            try:
                loc = (f.get("path"), int(f.get("line")))
            except (TypeError, ValueError):
                loc = None
            if loc in existing:
                print(f"Skipping duplicate finding at {loc[0]}:{loc[1]} (existing thread)")
            else:
                kept.append(f)
        findings = kept
    if not findings:
        print("No inline findings to post")
        return []
    if dry_run:
        for f in findings:
            print(f"[dry-run] inline {f.get('path')}:{f.get('line')} -> {f.get('body')}")
        return []
    tmp_files = []
    comments = []
    try:
        for f in findings:
            fd, path = tempfile.mkstemp(suffix=".md")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(f["body"])
            tmp_files.append(path)
            comment = {
                "path": f["path"],
                "line": int(f["line"]),
                "side": f.get("side", "RIGHT"),
                "body_file": path,
            }
            if f.get("start_line") is not None:
                comment["start_line"] = int(f["start_line"])
                comment["start_side"] = f.get("start_side", comment["side"])
            comments.append(comment)
        if not GH.post_pr_review(commit_id=commit_id, comments=comments):
            return [f"failed to post inline review ({len(comments)} finding(s))"]
        return []
    finally:
        for p in tmp_files:
            try:
                os.unlink(p)
            except OSError:
                pass


def _apply_thread_actions(actions, threads, dry_run):
    """Apply resolve/unresolve/reply, but only on threads the bot authored.

    Ownership is enforced here (not trusted from the model): an action targeting
    a thread the bot did not author (per GitHub's ``viewerDidAuthor``) is dropped
    with a warning, so the bot can never touch a human's thread.
    """
    actions = [a for a in (actions or []) if isinstance(a, dict)]
    if not actions:
        return []

    errors = []
    by_id = {t.get("id"): t for t in threads}
    for a in actions:
        thread_id = a.get("thread_id")
        action = a.get("action")
        thread = by_id.get(thread_id)
        if thread is None:
            print(f"WARNING: thread_action targets unknown thread [{thread_id}] — skip")
            continue
        if not _thread_authored_by_viewer(thread):
            print(
                f"WARNING: refusing {action} on thread [{thread_id}] authored by "
                f"[{_thread_first_author(thread)}] — not this bot"
            )
            continue
        if dry_run:
            print(f"[dry-run] {action} thread {thread_id}")
            continue
        if action == "resolve":
            if not GH.resolve_pr_review_thread(thread_id):
                errors.append(f"failed to resolve thread [{thread_id}]")
        elif action == "unresolve":
            if not GH.unresolve_pr_review_thread(thread_id):
                errors.append(f"failed to unresolve thread [{thread_id}]")
        elif action == "reply":
            body = (a.get("body") or "").strip()
            parent = _thread_first_comment_db_id(thread)
            if not body or parent is None:
                print(f"WARNING: reply on [{thread_id}] missing body/parent — skip")
                continue
            fd, path = tempfile.mkstemp(suffix=".md")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(body)
                if not GH.post_pr_line_comment(body_file=path, in_reply_to=parent):
                    errors.append(f"failed to reply on thread [{thread_id}]")
            finally:
                try:
                    os.unlink(path)
                except OSError:
                    pass
        else:
            print(f"WARNING: unknown thread action [{action}] — skip")
    return errors


def review(args):
    """Gather PR context, consult the model, and apply the structured result."""
    info = Info()
    if not info.pr_number or int(info.pr_number) <= 0:
        print("Not a PR — skipping AI review")
        return Result.create_from(status=Result.Status.SKIPPED, info="not a PR")

    project_prompt = ""
    if args.prompt:
        if not os.path.isfile(args.prompt):
            raise FileNotFoundError(f"Prompt file not found: {args.prompt}")
        with open(args.prompt, "r", encoding="utf-8") as f:
            project_prompt = f.read()

    provider = resolve_provider(args.provider, model=args.model or "")
    effort = getattr(args, "reasoning_effort", "") or ""
    if effort and hasattr(provider, "reasoning_effort"):
        provider.reasoning_effort = effort
    print(f"AI review: provider={provider.name} model={provider.resolved_model()} "
          f"effort={getattr(provider, 'reasoning_effort', '(n/a)')} "
          f"dry_run={bool(args.dry_run)}")

    diff = GH.get_pr_diff()
    threads = GH.list_pr_review_threads()
    user_content = _build_user_content(info, diff, threads, project_prompt)

    review_data, usage = _run_model(provider, _SYSTEM, user_content)

    # Aggregate write failures from every apply step: the GH helpers return
    # False (not raise) on API failure, so a failed/partial write must fail the
    # job rather than falsely report a successfully applied review.
    footer = _investigation_footer(usage)
    errors = []
    errors += _post_summary(review_data, args.dry_run, footer)
    errors += _post_inline_findings(
        review_data.get("inline_findings"),
        info.sha,
        args.dry_run,
        threads=threads,
    )
    errors += _apply_thread_actions(
        review_data.get("thread_actions"), threads, args.dry_run
    )

    rounds = ""
    if usage.max_tool_rounds:
        rounds = (
            f" rounds={usage.tool_rounds}/{usage.max_tool_rounds}"
            f" tool_calls={usage.tool_calls}"
            + (" exhausted" if usage.exhausted else "")
        )
    info_line = (
        f"provider={provider.name} model={provider.resolved_model()} "
        f"verdict={review_data.get('verdict') or '(none)'} "
        f"findings={len(review_data.get('inline_findings') or [])} "
        f"thread_actions={len(review_data.get('thread_actions') or [])} "
        f"tokens={usage.input_tokens}/{usage.output_tokens}{rounds}"
    )
    if errors:
        info_line += "\nWrite failures:\n- " + "\n- ".join(errors)
    print(info_line)
    status = Result.Status.FAIL if errors else Result.Status.OK
    return Result.create_from(status=status, info=info_line)


def main(args):
    sw = Utils.Stopwatch()
    status = Result.Status.OK
    info = ""
    result = None
    try:
        result = review(args)
    except Exception as e:
        info = f"ERROR: {e}"
        print(info)
        traceback.print_exc()
        status = Result.Status.FAIL
    if result is None:
        result = Result.create_from(status=status, stopwatch=sw, info=info)
    result.complete_job(with_job_summary_in_info=False)
