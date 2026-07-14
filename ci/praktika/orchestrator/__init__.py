import json
import os
import re
import sys
import time
from collections import defaultdict, deque

from ..mangle import _get_artifact_to_providing_job_map, _get_workflows
from ..settings import Settings
from ..workflow import Workflow


# Map SQS event types to Workflow.Event values
_EVENT_MAP = {
    "pull_request": Workflow.Event.PULL_REQUEST,
    "push": Workflow.Event.PUSH,
}


def _branch_matches(branch, patterns):
    """Check if branch matches any of the patterns (exact or regex)."""
    for pattern in patterns:
        if pattern == branch:
            return True
        if re.fullmatch(pattern, branch):
            return True
    return False


def _current_orchestrator_filter() -> str:
    explicit = (os.environ.get("PRAKTIKA_ORCHESTRATOR_FILTER") or "").strip()
    if explicit:
        return explicit

    queue_name = (os.environ.get("PRAKTIKA_CONTROLLER_QUEUE") or "").strip()
    if queue_name.endswith("-base"):
        return "base"
    return "default"


def find_workflows_for_event(event, workflow_name=None):
    """Find all workflows matching the trigger event. Returns empty list if no match."""
    workflow_name = (workflow_name or "").strip()
    event_type = event.get("type", "")
    workflow_event = _EVENT_MAP.get(event_type)
    if not workflow_event:
        print(f"No workflow event mapping for trigger type [{event_type}]")
        return []

    if event_type == "pull_request":
        branch = event.get("base_ref", "")
    elif event_type == "push":
        branch = event.get("head_ref", "")
    else:
        branch = ""

    matched = []
    orchestrator_filter = _current_orchestrator_filter()
    for wf in _get_workflows():
        if workflow_name and wf.name != workflow_name:
            continue
        if wf.engine == Workflow.Engine.GH_ACTIONS:
            continue
        workflow_filter = (getattr(wf, "orchestrator_filter", "") or "default").strip()
        if not workflow_name and workflow_filter != orchestrator_filter:
            print(
                f"Skip workflow [{wf.name}] for orchestrator filter "
                f"[{orchestrator_filter}] (workflow requires [{workflow_filter}])"
            )
            continue
        if wf.event != workflow_event:
            continue
        if event_type == "pull_request" and wf.base_branches:
            if _branch_matches(branch, wf.base_branches):
                matched.append(wf)
        elif event_type == "push" and wf.branches:
            if _branch_matches(branch, wf.branches):
                matched.append(wf)

    if not matched:
        name_hint = f" name [{workflow_name}]" if workflow_name else ""
        print(
            f"No workflow found for event [{workflow_event}] "
            f"branch [{branch}]{name_hint}"
        )
    return matched


def build_job_dag(workflow):
    """Build the dependency DAG for a workflow's jobs.

    Returns (levels, job_deps) where:
      - levels: list of lists, each inner list is a set of jobs that can run in parallel
      - job_deps: dict mapping job name -> set of job names it depends on
    """
    jobs_by_name = {job.name: job for job in workflow.jobs}
    artifact_provider = _get_artifact_to_providing_job_map(workflow)

    # Build adjacency: job_name -> set of predecessor job names
    job_deps = defaultdict(set)
    for job in workflow.jobs:
        # Hard deps: requires -> find the providing job
        for req in job.requires:
            if req in artifact_provider:
                job_deps[job.name].add(artifact_provider[req])
            elif req in jobs_by_name:
                # requires can also reference job names directly (for report artifacts)
                job_deps[job.name].add(req)
        # Ordering deps
        for dep_name in job.run_after:
            if dep_name in jobs_by_name:
                job_deps[job.name].add(dep_name)
        # Ensure every job appears in the dict
        job_deps.setdefault(job.name, set())

    # Topological sort by levels (Kahn's algorithm)
    in_degree = {name: len(deps) for name, deps in job_deps.items()}
    # Reverse: who depends on me
    dependents = defaultdict(set)
    for name, deps in job_deps.items():
        for dep in deps:
            dependents[dep].add(name)

    levels = []
    ready = deque(sorted(n for n, d in in_degree.items() if d == 0))
    visited = set()

    while ready:
        level = list(ready)
        levels.append(level)
        ready.clear()
        for name in level:
            visited.add(name)
            for succ in sorted(dependents[name]):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    ready.append(succ)

    # Check for cycles
    if len(visited) != len(jobs_by_name):
        missing = set(jobs_by_name) - visited
        print(f"WARNING: dependency cycle detected, unreachable jobs: {missing}")

    return levels, dict(job_deps)


def print_execution_plan(workflow, levels, job_deps):
    """Print a human-readable execution plan."""
    jobs_by_name = {job.name: job for job in workflow.jobs}
    total_jobs = sum(len(lv) for lv in levels)

    print(f"\n{'='*80}")
    print(f"Execution plan for workflow [{workflow.name}]")
    print(f"Total jobs: {total_jobs}, Execution levels: {len(levels)}")
    print(f"{'='*80}")

    for i, level in enumerate(levels):
        print(f"\n--- Level {i} ({len(level)} jobs, parallel) ---")
        for name in level:
            job = jobs_by_name[name]
            deps = job_deps.get(name, set())
            runs_on = ", ".join(job.runs_on) if job.runs_on else "default"
            dep_str = f" <- [{', '.join(sorted(deps))}]" if deps else ""
            provides_str = f" -> [{', '.join(job.provides)}]" if job.provides else ""
            print(f"  {name}")
            print(f"    runner: {runs_on}{dep_str}{provides_str}")

    print(f"\n{'='*80}\n")


# Exit code the orchestrator returns when it could NOT run the workflow at all
# (startup/infra failure: bad AI provider, plan build couldn't reach S3/GH, …).
# Distinct from rc=1 (the DAG ran and jobs legitimately failed) so the
# controller can tell a retryable infra fault apart from a real red build and
# replace the instance + retry only for the former. Kept well clear of the
# small exit codes tools use for ordinary failures.
INFRA_EXIT_CODE = 100


def _current_instance_id():
    return (os.environ.get("INSTANCE_ID") or "").strip()


def _current_attempt():
    """The cross-instance attempt label (e.g. ``2/3``) the controller passes in
    via ``PRAKTIKA_ATTEMPT`` so retries on fresh orchestrators are visible on
    the check. Empty when running outside the controller."""
    return (os.environ.get("PRAKTIKA_ATTEMPT") or "").strip()


def _check_output(workflow, state, error=None, report_url=None, phase=None):
    """Assemble a Check API `output` dict (title, summary, text) from the
    live ``WorkflowState``. Called on every PATCH so the top-level check's
    Markdown body tracks the current per-job table.

    ``phase`` is a coarse lifecycle label (``starting``/``ai_setup``/
    ``planning``/``running``/``finalizing``) surfaced alongside the
    orchestrator instance id so a stuck or failed run shows *where* it got to,
    not just that it is in progress. ``attempt`` (e.g. ``2/3``) makes
    cross-instance infra retries visible."""
    instance_id = _current_instance_id()
    attempt = _current_attempt()
    if workflow is None:
        summary = "No workflow matched this event"
        if instance_id:
            summary += f" — orchestrator `{instance_id}`"
        return {
            "title": "No workflow",
            "summary": summary,
            "text": (
                f"**Orchestrator instance:** `{instance_id}`"
                if instance_id
                else ""
            ),
        }
    title = workflow.name
    if error is not None:
        summary = f"Orchestrator failed: {error}"
    elif state is None:
        summary = f"`{workflow.name}` — {phase}" if phase else f"Planning `{workflow.name}`"
    elif state.cancelled:
        summary = f"Cancelled — {state.md_status_summary()}"
    else:
        summary = state.md_status_summary()
    if report_url:
        summary += f" — [CI Report]({report_url})"
    if attempt:
        summary += f" — attempt {attempt}"
    if instance_id:
        summary += f" — orchestrator `{instance_id}`"
    header_bits = []
    if instance_id:
        header_bits.append(f"**Orchestrator instance:** `{instance_id}`")
    if attempt:
        header_bits.append(f"**Attempt:** `{attempt}`")
    if phase:
        header_bits.append(f"**Phase:** `{phase}`")
    header = " — ".join(header_bits)
    text = state.md_status() if state is not None else ""
    if header:
        text = header + (f"\n\n{text}" if text else "")
    if error is not None:
        text += f"\n\n### Error\n\n```\n{error}\n```"
    # Check API caps output.text at ~64 KB.
    limit = 60_000
    if len(text) > limit:
        text = text[:limit] + "\n\n... (truncated)\n"
    return {"title": title, "summary": summary, "text": text}


def _patch_top_check(check, workflow, state, error=None, details_url=None, phase=None):
    """PATCH the top-level workflow check with the current Markdown snapshot.

    Wrapped in try/except: a stuck GitHub API call must not kill the
    orchestration loop. The error is reported but otherwise swallowed.
    """
    if check is None:
        return
    try:
        check.update(
            output=_check_output(workflow, state, error=error, report_url=details_url, phase=phase),
            details_url=details_url,
        )
    except Exception as e:
        print(f"  [warn] top-level check PATCH failed: {type(e).__name__}: {e}")


def orchestrate(
    event,
    check=None,
    gh_token=None,
    run_id=None,
    ci=True,
    workflow_name=None,
    bootstrap_check_id=None,
):
    """Single orchestrator entry-point used by both the SQS runner and the CLI.

    Finds all workflows matching ``event`` and runs them sequentially. Each
    matched workflow gets its own GitHub check run (named after the workflow).

    ``ci=False``: local/dry-run mode — no GH auth, no check runs, no buffering.

    Returns the process exit code (0 on success, 1 if any orchestration crashed).
    """
    print(
        f"Trigger event: {event.get('type')}.{event.get('action', '')} "
        f"repo={event.get('repo', '')} sender={event.get('sender', '')}"
    )

    if ci and gh_token is None:
        # The orchestrator loop outlives a single installation token (~1h),
        # so we hand the check-run code a self-refreshing provider rather
        # than a string snapshot. Mint eagerly here so a misconfigured
        # secret fails fast (before we open any check runs); subsequent
        # API calls go through the provider and pick up fresh tokens
        # transparently as the cached one nears expiry.
        from ..gh_auth import GHTokenProvider
        try:
            provider = GHTokenProvider()
            provider.get()  # eager mint to surface auth errors here
            gh_token = provider
        except Exception as e:
            raise RuntimeError(f"Failed to mint GH token for CI orchestration: {e}") from e

    # The controller opens a bootstrap check run *before* the (slow) clone so
    # the PR shows CI immediately and an interrupted clone still leaves a
    # signal. Adopt it here now that the workflow name is known, instead of
    # opening a fresh check.
    if check is None and ci and gh_token and bootstrap_check_id:
        from .check_run import CheckRun

        check = CheckRun(gh_token, event.get("repo", ""), bootstrap_check_id, "CI")

    workflows = find_workflows_for_event(event, workflow_name=workflow_name)
    if not workflows:
        print("No matching workflows, exiting")
        if check is not None:
            try:
                check.complete("neutral", output=_check_output(None, None))
            except Exception:
                print(f"Failed to complete check run: {check}", file=sys.stderr)
        return 0

    print(f"Matched {len(workflows)} workflow(s): {[wf.name for wf in workflows]}")

    # TODO: run only ONE workflow per orchestrator and parallelize distinct
    # workflows across orchestrator instances (dispatch one message per
    # matched workflow). Today an event can match several workflows and we run
    # them sequentially in a single process, which is why a single process
    # exit code has to stand in for all of them — hence the severity-collapse
    # `overall_rc` hack below. Once each workflow has its own message + check +
    # instance, this loop and the collapse go away. See README roadmap.
    #
    # Until then: highest severity wins so an infra failure (INFRA_EXIT_CODE)
    # in any workflow outranks an ordinary failure (1) — the controller then
    # retries the whole event on a fresh instance. 0 < 1 < INFRA_EXIT_CODE.
    overall_rc = 0
    for i, workflow in enumerate(workflows):
        rc = _orchestrate_single(
            workflow,
            event,
            gh_token=gh_token,
            local_mode=not ci,
            # Only the first workflow adopts the single bootstrap check; any
            # further matched workflows open their own named checks.
            existing_check=check if i == 0 else None,
        )
        overall_rc = max(overall_rc, rc)
    return overall_rc


def _orchestrate_single(workflow, event, gh_token=None, local_mode=False, existing_check=None):
    """Run one workflow: open a check run, execute the DAG, close the check.

    Returns 0 on success, 1 on crash.
    """
    from .check_run import CheckRun

    repo = event.get("repo", "")
    head_sha = event.get("head_sha", "")

    report_url = None
    if workflow.enable_report:
        from ..info import Info
        report_url = Info.get_specific_report_url_static(
            pr_number=event.get("pr_number") or 0,
            branch=event.get("head_ref", ""),
            sha=head_sha,
            job_name="",
            workflow_name=workflow.name,
        )

    check = None
    if gh_token:
        try:
            if existing_check is not None:
                # Adopt the controller's pre-clone bootstrap check: rename it
                # to this workflow and reuse the same check-run id.
                check = existing_check.retitle(workflow.name, details_url=report_url)
            else:
                check = CheckRun.start(gh_token, repo, head_sha, workflow.name, details_url=report_url)
        except Exception as e:
            raise RuntimeError(
                f"Failed to open initial check run for [{workflow.name}]: {e}"
            ) from e

    run_id = str(check.id) if check is not None else None

    print(f"Matched workflow [{workflow.name}] with {len(workflow.jobs)} jobs")

    from .ai import OrchestratorAI
    from .state import WorkflowState

    # The top-level check body is rendered from `state.md_status()` (a live
    # per-job table) by `_check_output`, not from this stream — so we print
    # straight to stdout. Lets the user (or `journalctl -fu praktika-controller`)
    # see progress in real time, especially important when the loop hangs.
    #
    # Everything past the opened check runs under one try/finally so the check
    # is ALWAYS finalized with an explicit, phase-tagged error — a crash here
    # (e.g. a bad AI provider, see PR #130) must never leave the check stuck
    # `in_progress`. `phase` records how far we got and is surfaced on the
    # check itself next to the orchestrator instance id.
    phase = "starting"
    advisor = None
    state = None
    error = None
    # Whether we got past startup into the DAG. A failure before this is an
    # infra/startup fault (no jobs dispatched yet) and is safe for the
    # controller to retry on a fresh instance; a failure after it is not
    # (jobs are already running) and is reported as an ordinary failure.
    dag_started = False
    _patch_top_check(check, workflow, None, phase=phase, details_url=report_url)
    try:
        # Startup (AI advisor + workflow plan build) talks to S3/GH and can
        # hit transient infra errors; retry it a bounded number of times.
        # The job loop below is NOT retried — once jobs are dispatched a
        # restart would double-run them.
        attempts = max(1, getattr(Settings, "MAX_RETRIES_ORCHESTRATOR", 3))
        for attempt in range(1, attempts + 1):
            try:
                phase = "ai_setup"
                _patch_top_check(check, workflow, None, phase=phase, details_url=report_url)
                # None when AI orchestration is disabled (the default) or the
                # configured provider can't be resolved — the loop is unchanged.
                # The patcher lets a cancel_and_patch decision land a fix. In CI
                # it commits+pushes to the PR branch; locally it applies to the
                # working tree only (no push) so a local run can produce the
                # patch for inspection. None keeps the decision advisory.
                if local_mode:
                    patcher = _make_local_patcher()
                elif gh_token:
                    patcher = _make_ai_patcher(
                        repo, event.get("head_ref", ""), head_sha, gh_token
                    )
                else:
                    patcher = None
                # The "AI Orchestrator [Workflow]" check mirrors the
                # orchestrator AI observations and decisions
                # decisions (CI only; created lazily on the first consultation).
                ai_check = (
                    _make_ai_check_updater(gh_token, repo, head_sha, workflow.name)
                    if not local_mode
                    else None
                )
                advisor = OrchestratorAI.maybe_create(
                    event=event, run_id=run_id, local_mode=local_mode,
                    patcher=patcher, ai_check=ai_check, workflow_config=workflow,
                )

                phase = "planning"
                _patch_top_check(check, workflow, None, phase=phase, details_url=report_url)
                state = WorkflowState(
                    workflow,
                    event=event,
                    gh_token=gh_token,
                    repo=event.get("repo", ""),
                    head_sha=event.get("head_sha", ""),
                    run_id=run_id,
                    local_mode=local_mode,
                )
                state.print_plan()
                break
            except Exception as e:
                # Discard any partial startup state before retrying.
                if advisor is not None:
                    try:
                        advisor.finalize(None)
                    except Exception:
                        pass
                advisor = None
                state = None
                print(
                    f"[infra] orchestrator startup attempt {attempt}/{attempts} "
                    f"failed in phase [{phase}]: {type(e).__name__}: {e}"
                )
                if attempt >= attempts:
                    raise
                _patch_top_check(
                    check, workflow, None,
                    phase=f"{phase} — retry {attempt + 1}/{attempts}",
                    details_url=report_url,
                )
                time.sleep(min(2 ** attempt, 30))

        phase = "running"
        dag_started = True
        _patch_top_check(check, workflow, state, phase=phase, details_url=report_url)
        if advisor is not None:
            advisor.on_run_start(state, event)

        cancel_handled = False
        while state.not_finished():
            if state.cancelled and not cancel_handled:
                state.cancel_unfinished_jobs()
                cancel_handled = True
            for job in state.get_ready():
                job.kick()
            state.wait()
            if advisor is not None:
                advisor.on_workflow_update(state, event)
            _patch_top_check(check, workflow, state, phase=phase, details_url=report_url)

        state.print_summary()
    except Exception as e:
        error = f"[phase: {phase}] {type(e).__name__}: {e}"
        print(f"\n\nError: {error}")
    finally:
        phase = "finalizing"
        if advisor is not None:
            advisor.finalize(state)
        if state is not None:
            state.cleanup()

    if check is not None:
        if error is not None:
            conclusion = "failure"
        elif state is not None and state.cancelled:
            conclusion = "cancelled"
        else:
            conclusion = "neutral"
        try:
            check.complete(
                conclusion,
                output=_check_output(workflow, state, error=error, report_url=report_url, phase=phase),
                details_url=report_url,
            )
        except Exception:
            print(f"Failed to complete check run: {check}", file=sys.stderr)

    if error is None:
        return 0
    # Startup/infra failure (workflow never ran) → retryable on a fresh
    # instance; a crash once the DAG was running is an ordinary failure.
    return 1 if dag_started else INFRA_EXIT_CODE


def _git_output(*args):
    import subprocess
    r = subprocess.run(["git"] + list(args), capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


def _resolve_gh_token(gh_token):
    """Return the token *string* from either a raw token or a GHTokenProvider.

    In CI the orchestrator passes a self-refreshing ``GHTokenProvider`` (not a
    str) as ``gh_token`` — call ``.get()`` to mint/refresh the current token;
    a plain string passes through. Resolved fresh at push time so a long run
    never uses an expired token.
    """
    getter = getattr(gh_token, "get", None)
    return getter() if callable(getter) else gh_token


def _make_ai_check_updater(gh_token, repo, head_sha, workflow_name):
    """Build the ``updater(status, summary_md)`` the AI advisor mirrors to.

    Returns a callable that maintains a dedicated
    **AI Orchestrator [Workflow Name]** GitHub check: ``status="in_progress"``
    while an observation is out to the model, ``"neutral"`` once the turn
    lands. The check run is created **lazily on first use** (the first model
    consultation), so green runs never show one. Returns None when there's
    nothing to authenticate with.
    """
    if not (gh_token and repo and head_sha):
        return None
    from .check_run import CheckRun

    state = {"check": None}
    title = f"AI Orchestrator [{workflow_name}]"

    def update(status, summary):
        out = {"title": title, "summary": summary}
        if state["check"] is None:
            state["check"] = CheckRun.start(
                gh_token, repo, head_sha, title, with_cancel_action=False
            )
        if status == "in_progress":
            state["check"].update(status="in_progress", output=out)
        else:
            state["check"].update(status="completed", conclusion="neutral", output=out)

    return update


def _make_ai_patcher(repo, head_ref, head_sha, gh_token):
    """Build the ``patcher(files, message) -> commit_sha`` the advisor calls to
    land a ``cancel_and_patch`` fix.

    Creates the commit through GitHub's **Git Data API** (blob → tree → commit →
    ref update) with the installation token, so GitHub attributes it to the
    **app bot** that mints the token and marks it **verified** — no author
    identity to configure. The new file contents are read from the working tree
    (already written by ``_apply_edits``).

    Scope is **same-repo PR branches**, fast-forward only: it reads the branch
    ref and proceeds only when ``head_ref`` exists on ``repo`` at ``head_sha``.
    A missing ref means a fork PR; a moved ref (or a non-fast-forward on the
    final ref update, ``force=false``) means human-takeover — both return ``""``
    and the decision stays advisory. Returns None (no patcher) when inputs to
    authenticate are missing.

    ``gh_token`` may be a raw token or a ``GHTokenProvider``; it is resolved to a
    string per call (see ``_resolve_gh_token``).
    """
    if not (repo and head_ref and head_sha and gh_token):
        return None

    import os

    import requests

    api = "https://api.github.com"

    def _patch(files, message):
        token = _resolve_gh_token(gh_token)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        def _req(method, path, **kw):
            return requests.request(
                method, f"{api}{path}", headers=headers, timeout=20, **kw
            )

        # Same-repo + fast-forward guard: branch must exist on repo at head_sha.
        r = _req("GET", f"/repos/{repo}/git/ref/heads/{head_ref}")
        if r.status_code == 404:
            print(f"[AI   ] patch skipped: {head_ref} not on {repo} (fork PR?)")
            return ""
        r.raise_for_status()
        remote_sha = r.json().get("object", {}).get("sha", "")
        if remote_sha != head_sha:
            print(
                f"[AI   ] patch skipped: {head_ref} moved past {head_sha[:12]} "
                f"on {repo} (human takeover)"
            )
            return ""

        # Base tree of the current head commit.
        r = _req("GET", f"/repos/{repo}/git/commits/{head_sha}")
        r.raise_for_status()
        base_tree = r.json()["tree"]["sha"]

        # One blob per changed file (new content from the working tree).
        tree = []
        for rel in files:
            with open(rel, "r", encoding="utf-8") as f:
                content = f.read()
            r = _req(
                "POST", f"/repos/{repo}/git/blobs",
                json={"content": content, "encoding": "utf-8"},
            )
            r.raise_for_status()
            mode = "100755" if os.access(rel, os.X_OK) else "100644"
            tree.append({"path": rel, "mode": mode, "type": "blob", "sha": r.json()["sha"]})

        r = _req(
            "POST", f"/repos/{repo}/git/trees",
            json={"base_tree": base_tree, "tree": tree},
        )
        r.raise_for_status()
        tree_sha = r.json()["sha"]

        # No author/committer → GitHub stamps the app bot and signs (verified).
        r = _req(
            "POST", f"/repos/{repo}/git/commits",
            json={"message": message, "tree": tree_sha, "parents": [head_sha]},
        )
        r.raise_for_status()
        commit_sha = r.json()["sha"]

        # Fast-forward the branch ref (force=false → 422 if it advanced).
        r = _req(
            "PATCH", f"/repos/{repo}/git/refs/heads/{head_ref}",
            json={"sha": commit_sha, "force": False},
        )
        if r.status_code == 422:
            print(f"[AI   ] patch skipped: {head_ref} advanced mid-patch (not fast-forward)")
            return ""
        r.raise_for_status()
        return commit_sha

    return _patch


def _build_event(args):
    """Build a trigger event dict from CLI args + current git state."""
    import re
    import subprocess

    repo = args.repo
    if not repo:
        remote = _git_output("remote", "get-url", "origin")
        m = re.search(r"github\.com[:/](.+?)(?:\.git)?$", remote)
        repo = m.group(1) if m else ""

    head_sha = args.head_sha or _git_output("rev-parse", "HEAD")
    head_ref = args.head_ref or _git_output("branch", "--show-current")
    sender = args.sender or _git_output("config", "user.name")

    pr_number = args.pr_number
    if pr_number is None:
        r = subprocess.run(
            ["gh", "pr", "view", "--json", "number", "-q", ".number"],
            capture_output=True, text=True,
        )
        try:
            pr_number = int(r.stdout.strip()) if r.returncode == 0 else 0
        except ValueError:
            pr_number = 0

    event = {
        "type": args.event_type,
        "action": "synchronize",
        "repo": repo,
        "head_sha": head_sha,
        "head_ref": head_ref,
        "base_ref": args.base_ref,
        "pr_number": pr_number,
        "sender": sender,
        "title": "",
        "draft": False,
        "labels": [],
    }
    print(
        f"Event: {event['type']}.{event['action']} "
        f"PR#{event['pr_number']} [{event['head_ref']} -> {event['base_ref']}] "
        f"sha={event['head_sha'][:12] if event['head_sha'] else ''}"
    )
    return event


def _coerce_setting(raw):
    """Coerce a CLI string into bool/int/float/str for a Settings override."""
    low = raw.strip().lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _apply_settings_overrides(pairs):
    """Apply ``KEY=VALUE`` CLI overrides onto the live ``Settings`` instance.

    Keys are Settings attribute names exactly as in the config (e.g.
    ``AWS_REGION=eu-north-1``). Unknown keys are still set — Settings is a
    plain object and readers use ``getattr`` — so a typo silently does nothing
    rather than erroring; the printed line is the receipt.
    """
    if not pairs:
        return
    from praktika.settings import Settings

    for item in pairs:
        if "=" not in item:
            print(f"[settings] ignoring malformed override {item!r} (need KEY=VALUE)")
            continue
        key, _, raw = item.partition("=")
        key = key.strip()
        value = _coerce_setting(raw)
        setattr(Settings, key, value)
        print(f"[settings] override {key} = {value!r}")


def _make_local_patcher():
    """Patcher for local runs: `cancel_and_patch` edits are already written to
    the working tree by `_apply_edits`; here we just report them and skip the
    commit/push, so a local run *gets the patch* (inspect with `git diff`) and
    still exercises the full record + cancel flow. Returns a non-empty marker so
    the advisor treats it as applied."""
    def _patch(files, message):
        print("[AI   ] LOCAL patch applied to working tree (not committed/pushed):")
        for f in files:
            print(f"          {f}")
        print("        proposed commit message:")
        for line in message.splitlines():
            print(f"          | {line}")
        print("        inspect with `git diff`; undo with `git checkout -- .`")
        return "local-dryrun"

    return _patch


def run(event_file=None, args=None):
    """CLI entry-point (``praktika orchestrate workflow [event.json]``)."""
    _apply_settings_overrides(getattr(args, "settings", None))
    if event_file:
        with open(event_file) as f:
            event = json.load(f)
    else:
        event = _build_event(args)
    ci = getattr(args, "ci", False)
    workflow_name = getattr(args, "name", None)
    bootstrap_check_id = os.environ.get("PRAKTIKA_BOOTSTRAP_CHECK_RUN_ID", "").strip() or None
    try:
        rc = orchestrate(
            event,
            ci=ci,
            workflow_name=workflow_name,
            bootstrap_check_id=bootstrap_check_id,
        )
    except Exception:
        import traceback

        traceback.print_exc()
        # An exception escaping orchestrate means we never ran the workflow
        # (token mint, routing, …) — an infra failure the controller can retry
        # on a fresh instance, not an ordinary red build.
        sys.exit(INFRA_EXIT_CODE)
    sys.exit(rc)
