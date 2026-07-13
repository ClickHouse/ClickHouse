"""AI advisor for the orchestrator (skeleton).

The orchestrator drives an ``OrchestratorAI`` through the run's lifecycle. As the
workflow state advances (job results land), the advisor classifies what changed
and routes it to the matching **provider hook** — ``on_job_failure`` when a job
fails, ``on_job_success`` when one passes, plus ``on_run_start`` /
``on_run_finish`` around the run. Each hook either consults the model and
returns a ``Turn`` (reasoning + decision + token usage + cost, which the advisor
records) or is the provider's inherited no-op (returns ``None`` → no model call,
nothing recorded). So a green run is free: only ``on_job_failure`` is wired to a
model today.

Decisions are recorded, and the actionable ones are dispatched: ``cancel_run``
flags the run cancelled and the orchestrator loop tears it down;
``cancel_and_patch`` applies the model's edits, commits + pushes them via the
injected patcher (triggering a fresh run), and cancels the superseded run.
The default provider is ``mock``, which does nothing. Disabled by default
(``Workflow.Config.ai_orchestrator.enabled``).

Real providers (``anthropic`` / ``bedrock``) implement ``on_job_failure`` only,
running a tool-use loop so the model can investigate the failure before deciding
— reading the job's logs (``fetch_log``, restricted to URLs in the observation)
and the checked-out PR source (``grep_repo`` / ``read_file``, rooted at the repo).
"""
import difflib
import os

from .provider import Observation, Turn, Usage, resolve_provider
from .session import SessionManager
from .trace import TraceLogger, UsageLedger

# Job status values (JobStatus.value) that count as terminal — a result has
# landed. Kept as plain strings so the advisor stays decoupled from the
# JobStatus enum and is trivial to drive with stubs in tests.
_TERMINAL_STATUSES = {"success", "failure", "skipped", "cancelled"}


def _status_value(job_state):
    """Best-effort read of a JobState's status as a plain string."""
    status = getattr(job_state, "status", None)
    return getattr(status, "value", status)


# Job lifecycle statuses (JobStatus.value) that warrant pulling the job's
# Result detail into the observation — only failed/cancelled jobs are worth
# the extra prompt tokens. These mirror JobStatus.value (lowercase), which is
# what `_delta`/`build_observation` put on each `changed` entry.
_FAILED_STATUSES = {"failure", "cancelled"}

# Result.Status values that are NOT failures: a sub-result in one of these
# states adds no signal to a failure digest, so it is skipped.
_RESULT_OK_STATUSES = {"OK", "SKIPPED", "XFAIL", "PENDING", "RUNNING"}

# Bounds so a single broken job can't blow up the prompt: cap how many
# failing sub-results we surface, how long each free-text field may be, and
# how many log links we list per node.
_MAX_FAILED_SUBRESULTS = 20
_MAX_INFO_CHARS = 500
_MAX_LINKS = 10


def _get_ai_config(workflow_config):
    if workflow_config is None:
        return None
    return getattr(workflow_config, "ai_orchestrator", None)


def _clean_links(links):
    """Return up to _MAX_LINKS http(s) URLs from a Result node's `links`.

    Only http(s) URLs are kept — they are what the advisor's fetch_log tool
    can retrieve, and the allowlist built from these guards against fetching
    anything the run didn't actually publish.
    """
    out = []
    for url in links or []:
        url = str(url)
        if url.startswith("http://") or url.startswith("https://"):
            out.append(url)
        if len(out) >= _MAX_LINKS:
            break
    return out


def _result_digest(result):
    """Compact, failure-focused view of a job's Result for the advisor.

    The full Result is a recursive tree (top-level status/info/links plus
    nested ``results``; see CLAUDE.md) and can be large. The advisor prompt
    can't afford the whole tree, so we keep only what it needs to reason
    about a failure: the top-level status/info, the failing sub-results
    (name + status + truncated info + log links) flattened and capped, any
    ``ext.errors``, and the top-level log links. The links let the model
    decide which logs to pull via the fetch_log tool. Returns None when
    there is no usable result.
    """
    if not isinstance(result, dict):
        return None

    def _trunc(s):
        s = str(s or "")
        return s if len(s) <= _MAX_INFO_CHARS else s[:_MAX_INFO_CHARS] + " …"

    failed = []

    def _walk(node):
        for sub in node.get("results") or []:
            if not isinstance(sub, dict):
                continue
            if sub.get("status") not in _RESULT_OK_STATUSES:
                entry = {"name": sub.get("name", ""), "status": sub.get("status")}
                info = _trunc(sub.get("info"))
                if info:
                    entry["info"] = info
                links = _clean_links(sub.get("links"))
                if links:
                    entry["links"] = links
                failed.append(entry)
            _walk(sub)

    _walk(result)

    digest = {"status": result.get("status")}
    info = _trunc(result.get("info"))
    if info:
        digest["info"] = info
    links = _clean_links(result.get("links"))
    if links:
        digest["links"] = links
    if failed:
        digest["failed"] = failed[:_MAX_FAILED_SUBRESULTS]
        overflow = len(failed) - _MAX_FAILED_SUBRESULTS
        if overflow > 0:
            digest["failed_overflow"] = overflow
    ext = result.get("ext")
    if isinstance(ext, dict) and ext.get("errors"):
        digest["errors"] = [_trunc(e) for e in ext["errors"]][:_MAX_FAILED_SUBRESULTS]
    return digest


def build_observation(state, event, changed) -> Observation:
    """Snapshot the live WorkflowState into a serializable Observation."""
    jobs = []
    for js in state.jobs.values():
        started = getattr(js, "started_at", None)
        finished = getattr(js, "finished_at", None)
        duration_s = round(finished - started, 1) if started and finished else None
        entry = {"name": js.name, "status": _status_value(js)}
        if duration_s is not None:
            entry["duration_s"] = duration_s
        reason = getattr(js, "filter_reason", None)
        if reason:
            entry["reason"] = reason
        jobs.append(entry)

    # Enrich the just-became-terminal jobs that failed with a compact Result
    # digest (the orchestrator stashed the raw Result on the JobState). Only
    # the `changed` set carries detail, so the prompt grows with failures
    # this turn, not with the whole DAG.
    changed = [dict(c) for c in (changed or [])]
    for entry in changed:
        if entry.get("status") not in _FAILED_STATUSES:
            continue
        js = state.jobs.get(entry.get("name"))
        digest = _result_digest(getattr(js, "result", None)) if js else None
        if digest:
            entry["result"] = digest

    ev = event or {}
    return Observation(
        event={
            "type": ev.get("type", ""),
            "action": ev.get("action", ""),
            "pr_number": ev.get("pr_number"),
            "head_sha": ev.get("head_sha", ""),
            "head_ref": ev.get("head_ref", ""),
        },
        jobs=jobs,
        changed=changed,
        summary=state.md_status_summary() if hasattr(state, "md_status_summary") else "",
    )


def _safe_join(root, rel):
    """Resolve `rel` under `root` and confirm it stays inside (after symlinks).

    Returns the absolute path, or None if it escapes the root — the same guard
    the read tools use, applied to writes so a patch can't touch host files.
    """
    root = os.path.realpath(root)
    target = os.path.realpath(os.path.join(root, rel or ""))
    if target == root or target.startswith(root + os.sep):
        return target
    return None


def _apply_edits(edits, root=None):
    """Apply ``[{path, search, replace}]`` edits to files under ``root``.

    Each ``search`` must occur **exactly once** in its file; multiple edits to
    the same file compose in order. Validation is all-or-nothing: every edit is
    checked (path inside repo, file exists, search unique) before anything is
    written, so a bad edit leaves the tree untouched. Returns
    ``(ok, files, patch_text, error)`` — on success ``files`` is the sorted list
    of changed repo-relative paths and ``patch_text`` a unified diff for the
    session log; on failure ``ok`` is False and ``error`` says why.
    """
    root = os.path.realpath(root or os.getcwd())
    if not isinstance(edits, list) or not edits:
        return False, [], "", "no edits provided"

    originals = {}  # abs_path -> original text
    current = {}    # abs_path -> (rel, text)
    for i, e in enumerate(edits):
        if not isinstance(e, dict):
            return False, [], "", f"edit #{i} is not an object"
        rel = e.get("path") or ""
        search = e.get("search")
        replace = e.get("replace")
        if not rel or not isinstance(search, str) or not search or not isinstance(replace, str):
            return False, [], "", f"edit #{i} missing path/search/replace"
        target = _safe_join(root, rel)
        if target is None:
            return False, [], "", f"edit #{i} path escapes repo: {rel!r}"
        if not os.path.isfile(target):
            return False, [], "", f"edit #{i} no such file: {rel!r}"
        if target not in current:
            try:
                with open(target, "r", encoding="utf-8") as f:
                    text = f.read()
            except Exception as ex:
                return False, [], "", f"edit #{i} cannot read {rel!r}: {ex}"
            originals[target] = text
            current[target] = (rel, text)
        rel_name, text = current[target]
        n = text.count(search)
        if n != 1:
            return False, [], "", (
                f"edit #{i} search must occur exactly once in {rel!r} (found {n})"
            )
        current[target] = (rel_name, text.replace(search, replace, 1))

    files, diffs = [], []
    for target, (rel, new_text) in current.items():
        old_text = originals[target]
        if new_text == old_text:
            continue
        with open(target, "w", encoding="utf-8") as f:
            f.write(new_text)
        files.append(rel)
        diffs.append(
            "".join(
                difflib.unified_diff(
                    old_text.splitlines(keepends=True),
                    new_text.splitlines(keepends=True),
                    fromfile=f"a/{rel}",
                    tofile=f"b/{rel}",
                )
            )
        )
    if not files:
        return False, [], "", "edits produced no change"
    return True, sorted(files), "\n".join(diffs), ""


def _patch_commit_message(detail, round_id=""):
    """Compose the commit message for an AI patch (subject + body + trailer).

    The commit is authored by the app bot (created via the Git Data API), so no
    Co-Authored-By is added; the ``AI-Session-Round`` trailer marks it as an
    AI-session commit (the human-takeover seam)."""
    detail = (detail or "").strip()
    subject = detail.splitlines()[0] if detail else "apply fix"
    subject = subject[:68]
    lines = [f"AI fix: {subject}"]
    if detail and detail != subject:
        lines += ["", detail]
    if round_id:
        lines += ["", f"AI-Session-Round: {round_id}"]
    return "\n".join(lines)


class OrchestratorAI:
    """Routes workflow lifecycle events to the provider's hooks and records turns."""

    def __init__(self, provider, console, session=None, event=None, patcher=None):
        self._provider = provider
        self._console = console
        self._session = session
        self._event = event or {}
        # Injected by the orchestrator: patcher(files, message) -> commit_sha,
        # commits the working-tree edits to the PR branch and pushes. None when
        # pushing isn't possible (local mode / fork PR / no token) — then a
        # cancel_and_patch decision stays advisory.
        self._patcher = patcher
        self._ledger = UsageLedger()
        # Last status value seen per job — used to route an event only when a
        # job newly reaches a terminal state (a result was actually received).
        self._last_status = {}

    @classmethod
    def maybe_create(
        cls, event=None, run_id=None, local_mode=False, patcher=None, ai_check=None,
        workflow_config=None
    ):
        """Build an OrchestratorAI from workflow AI config, or return None if disabled.

        Also opens (or rejoins) the durable per-PR AI session and registers
        this CI run with it. Session setup is best-effort: if it fails the
        advisor still runs with console-only output.

        ``patcher`` is the orchestrator-supplied commit+push callback used to
        apply a ``cancel_and_patch`` decision; when None (local mode / fork PR /
        no token) such a decision stays advisory. ``ai_check`` is the AI Advisor
        check updater the provider mirrors observations/turns to (None disables
        the check).

        The advisor is advisory and must never break core orchestration, so
        provider resolution is best-effort too: if the configured
        ``AI_PROVIDER`` can't be resolved or instantiated by the running
        runtime — e.g. an older orchestrator that predates the provider, or a
        provider whose SDK isn't installed — the advisor is disabled (returns
        None) rather than crashing the run.
        """
        ai_config = _get_ai_config(workflow_config)
        if ai_config is None or not getattr(ai_config, "enabled", False):
            return None
        spec = getattr(ai_config, "provider", "mock") or "mock"
        model = getattr(ai_config, "model", "") or ""
        try:
            provider = resolve_provider(spec, model=model)
        except Exception as e:
            print(
                f"[AI   ] advisor disabled: cannot use AI_PROVIDER={spec!r}: "
                f"{type(e).__name__}: {e}"
            )
            return None
        provider.attach_check_updater(ai_check)
        console = TraceLogger(run_id=run_id)
        print(
            f"[AI   ] advisor enabled: provider={provider.name} "
            f"model={provider.model or '(default)'}"
        )

        session = None
        if event is not None:
            try:
                session = SessionManager.from_event(
                    event, run_id=run_id, local_mode=local_mode,
                    ai_config=ai_config, console=console
                )
                session.begin_run(
                    run_id=run_id or "", sha=event.get("head_sha", ""), event=event
                )
            except Exception as e:
                print(f"  [warn] AI session unavailable: {type(e).__name__}: {e}")
                session = None
        return cls(provider, console, session=session, event=event, patcher=patcher)

    def _delta(self, state):
        """Return [{name, status, ...}] for jobs newly terminal since last turn."""
        changed = []
        for name, js in state.jobs.items():
            value = _status_value(js)
            if value == self._last_status.get(name):
                continue
            self._last_status[name] = value
            if value in _TERMINAL_STATUSES:
                entry = {"name": name, "status": value}
                reason = getattr(js, "filter_reason", None)
                if reason:
                    entry["reason"] = reason
                changed.append(entry)
        return changed

    def _consult(self, event, observation, state):
        """Dispatch a lifecycle ``event`` to the provider and, if it produced a
        turn, record + dispatch it.

        ``provider.consult`` owns the model call plus its bracketing (observation
        /turn tracking, the AI Advisor check, and turning a provider exception
        into an error ``Turn``). Returns the recorded ``Turn``, or ``None`` when
        the hook opted out (no model call, no ledger entry, no persisted turn).
        """
        turn = self._provider.consult(event, observation)
        if turn is None:
            return None
        self._ledger.add(turn.usage)
        if self._session is not None:
            # Session owns persistence + console + index; it also opens a round
            # implicitly on the first failure.
            self._session.observe_turn(observation, turn)
        else:
            self._console.record(observation, turn)
        self._dispatch(state, turn)
        return turn

    def on_run_start(self, state, event):
        """Lifecycle hook: the run just started, before any job is terminal.

        Routes to ``on_run_start`` — a no-op for today's providers, so this costs
        nothing unless a provider opts into the event.
        """
        return self._consult("run_start", build_observation(state, event, []), state)

    def on_workflow_update(self, state, event):
        """Hook called from the orchestrator loop after each ``state.wait()``.

        Routes the jobs that newly reached a terminal state to the matching
        provider hook: failures → ``on_job_failure``, successes →
        ``on_job_success``. Skipped/cancelled transitions are tracked (so they
        don't re-fire) but routed nowhere — they carry no problem to act on, and
        re-consulting on a cascade cancel just burns tokens. Idle poll ticks
        (nothing changed) do nothing. Returns the failure turn if one fired.
        """
        changed = self._delta(state)
        if not changed:
            return None
        failures = [c for c in changed if c.get("status") == "failure"]
        successes = [c for c in changed if c.get("status") == "success"]
        turn = None
        if failures:
            turn = self._consult(
                "job_failure", build_observation(state, event, failures), state
            )
        if successes:
            self._consult(
                "job_success", build_observation(state, event, successes), state
            )
        return turn

    def _dispatch(self, state, turn):
        """Apply actionable decisions from a turn to the run.

        Wired actions:

        * ``cancel_run`` — flag the run cancelled; the orchestrator loop tears it
          down (cancels unfinished jobs, writes the S3 cancel flag) next iteration.
        * ``cancel_and_patch`` — apply the decision's ``edits`` to the checked-out
          PR, commit+push them via the injected patcher (triggering a fresh run),
          record the edit on the session, then cancel this now-superseded run.
          Falls back to advisory (does nothing) when there is no patcher, the
          round budget is spent, or the edits don't apply/push cleanly.

        The first actionable decision wins; other types stay advisory.
        """
        if turn is None or turn.error:
            return
        for item in turn.decision or []:
            if not isinstance(item, dict):
                continue
            dtype = item.get("type")
            if dtype == "cancel_run":
                self._cancel_run(state, item.get("detail", ""))
                return
            if dtype == "cancel_and_patch":
                self._patch_and_cancel(state, item)
                return

    def _cancel_run(self, state, detail):
        if not getattr(state, "cancelled", False):
            print(f"[AI   ] cancel_run requested by advisor: {detail}")
            state.cancelled = True

    def _patch_and_cancel(self, state, item):
        """Apply a ``cancel_and_patch`` decision. Returns True if the patch was
        pushed and the run cancelled, False if it stayed advisory."""
        detail = item.get("detail", "")
        edits = item.get("edits")
        if self._patcher is None:
            print("[AI   ] cancel_and_patch advisory: no patcher (local/fork/no token)")
            return False
        if self._session is not None:
            ok, reason = self._session.can_continue_round()
            if not ok:
                print(f"[AI   ] cancel_and_patch skipped: {reason}")
                return False
        applied, files, patch_text, err = _apply_edits(edits)
        if not applied:
            print(f"[AI   ] cancel_and_patch not applied: {err}")
            return False
        round_id = ""
        if self._session is not None:
            round_id = self._session.current_round_id() or ""
        message = _patch_commit_message(detail, round_id=round_id)
        try:
            commit_sha = self._patcher(files, message)
        except Exception as e:
            print(f"[AI   ] cancel_and_patch push failed: {type(e).__name__}: {e}")
            return False
        if not commit_sha:
            print("[AI   ] cancel_and_patch: nothing pushed (branch moved?)")
            return False
        if self._session is not None:
            self._session.record_edit(patch_text, commit_sha=commit_sha, files=files)
        print(
            f"[AI   ] cancel_and_patch applied: {len(files)} file(s) -> "
            f"{commit_sha[:12]}; cancelling superseded run"
        )
        state.cancelled = True
        return True

    def finalize(self, state=None):
        """End-of-run hook: print the run cost total and close out the session.

        Routes the terminal run to ``provider.on_run_finish`` first (a no-op for
        today's providers), then prints the run cost total. On a green run an
        open round auto-resolves; otherwise it is left open and persisted, so the
        next CI run on this PR rejoins it.
        """
        if state is not None:
            self._consult(
                "run_finish", build_observation(state, self._event, []), state
            )
        # Don't leave the AI Advisor check spinning if a round-trip died mid-flight.
        self._provider.finalize_check()
        self._console.summary(self._ledger)
        if self._session is not None:
            self._session.finalize_run(
                conclusion=_run_conclusion(state),
                job_outcomes=_job_outcomes(state),
            )


def _run_conclusion(state):
    if state is None:
        return "error"
    if getattr(state, "cancelled", False):
        return "cancelled"
    if state.any_failed():
        return "failure"
    return "success"


def _job_outcomes(state):
    if state is None:
        return []
    return [{"name": js.name, "status": _status_value(js)} for js in state.jobs.values()]
