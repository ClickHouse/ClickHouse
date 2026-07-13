"""Runtime state of a workflow execution.

`WorkflowState` is the live, mutable counterpart to the static `Workflow` config:
it owns a `JobState` per job, tracks DAG-ready jobs, and exposes a small
kick/wait interface so the orchestrator's main loop reads as:

    state = WorkflowState(workflow)
    state.print_plan()
    while state.not_finished():
        for job in state.get_ready():
            job.kick()
        state.wait()
    state.print_summary()

`kick` dispatches each job: in CI mode by sending a ``job_task`` to the
runner-specific SQS queue (the runner picks it up, executes, and posts a
``job_completion`` message back); in local mode by running ``praktika
orchestrate job`` synchronously as a subprocess. ``wait`` long-polls the
per-run completions queue in CI mode, and is a no-op in local mode (the
sync subprocess already advanced state by the time it returned).
"""

import json
import os
import time
from collections import defaultdict
from enum import Enum

from . import build_job_dag
from praktika.settings import Settings


def _queue_prefix():
    project_slug = (getattr(Settings, "PROJECT_SLUG", "") or "").strip()
    if project_slug:
        return f"{project_slug}-"
    gh_auth_lambda = (getattr(Settings, "GH_AUTH_LAMBDA_NAME", "") or "").strip()
    if gh_auth_lambda.endswith("-gh-token"):
        return gh_auth_lambda.removesuffix("-gh-token") + "-"
    return ""


# Job liveness — S3-based heartbeat (see roadmap). The job agent posts
# ``heartbeat.json`` under ``runs/<run_id>/<job>/`` every
# ``HEARTBEAT_INTERVAL_S``. The orchestrator sweeps dispatched jobs once per
# wait() cycle and marks them dead under two rules:
#   - still QUEUED (dispatched but never picked up) AND age since kick >
#     RUNNER_PICKUP_TIMEOUT_S → runner pool did not pick up the job;
#   - RUNNING AND age since last heartbeat > HEARTBEAT_TIMEOUT_S → runner died
#     mid-job after pickup.
# Pickup grace covers queue/ASG delays before any runner has emitted a heartbeat.
# Heartbeat timeout is intentionally longer than the heartbeat interval so
# transient S3/read delays do not kill a live runner.
HEARTBEAT_INTERVAL_S = int(getattr(Settings, "HEARTBEAT_INTERVAL_S", 30) or 30)
RUNNER_PICKUP_TIMEOUT_S = int(
    getattr(Settings, "RUNNER_PICKUP_TIMEOUT_S", 3600) or 3600
)
HEARTBEAT_TIMEOUT_S = int(getattr(Settings, "HEARTBEAT_TIMEOUT_S", 300) or 300)

# wait() blocks for this long between S3 sweeps. Kept short so the
# orchestrator reacts quickly to cancel signals and finished jobs (no
# SQS long-poll any more).
WAIT_POLL_INTERVAL_S = 10


def _normalize_job_name_for_s3(name):
    """Turn a job name into an S3-safe path segment (mirrors job log path)."""
    return name.replace(" ", "_").replace("/", "_")


def _build_check_output(result, rc, instance_id="", report_url=""):
    """Render a job's Result as the ``output`` dict for a check-run
    completion. ``result`` is a ``praktika.Result`` reconstructed from the
    completion payload (the runner ships it in ``final.json``). Returns
    None on any failure so the caller can fall back to a bodyless
    completion."""
    try:
        text = result.to_markdown(report_url=report_url)
        # Check API caps output.text at ~64 KB.
        limit = 60_000
        if len(text) > limit:
            text = text[:limit] + "\n\n_… (truncated)_\n"
        dur = f" in {int(result.duration)}s" if result.duration else ""
        if rc != 0 and result.is_ok():
            # The runner process crashed after writing an OK result to disk —
            # OOM, disk-full, SIGKILL, etc. Report ERROR so the summary matches
            # the failure conclusion and the cause is clearly not the job logic.
            displayed_status = "ERROR"
            text = f"Runner process exited with rc={rc} after reporting OK — likely OOM or disk-full.\n\n{text}"
        else:
            displayed_status = result.status
        if displayed_status == "FAIL":
            displayed_status = "FAILED"
        summary = f"**{displayed_status}**{dur}"
        if report_url:
            summary += f" — [CI Report]({report_url})"
        if instance_id:
            summary += f" — runner `{instance_id}`"
            text = f"**Runner instance:** `{instance_id}`" + (
                f"\n\n{text}" if text else ""
            )
        return {"title": displayed_status, "summary": summary, "text": text}
    except Exception as e:
        print(f"  [warn] could not render job Result as MD: {type(e).__name__}: {e}")
        return None


def _is_missing_s3_key_error(exc):
    """Best-effort check for a missing S3 object without importing botocore."""
    if isinstance(exc, KeyError):
        return True
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error")
    if not isinstance(error, dict):
        return False
    return str(error.get("Code", "")) in {
        "NoSuchKey",
        "NoSuchBucket",
        "404",
        "NotFound",
    }


def _record_value(record, key, default=None):
    if isinstance(record, dict):
        return record.get(key, default)
    return getattr(record, key, default)


def _queue_for_runs_on(runs_on):
    """First meaningful ``runs_on`` label → ``<project-slug>-<label>`` queue name.

    "self-hosted" is a GitHub-Actions runner-group label with no meaning to the
    praktika engine, so it is skipped — the pool/size label is what maps to a
    queue (e.g. ``["self-hosted", "style-checker-aarch64"]`` →
    ``<slug>-style-checker-aarch64``).
    """
    for label in runs_on or ():
        if label and label != "self-hosted":
            return f"{_queue_prefix()}{label}"
    return None


class JobCheckRun:
    """Per-job GitHub check run.

    Lifecycle: ``queue`` creates the check as ``status=queued`` (shows up in
    the PR UI as pending) at the moment the orchestrator kicks the job,
    ``set_in_progress`` flips it once a runner heartbeat is observed, and
    ``complete`` closes it with a conclusion
    (``success``/``failure``/``skipped``/``neutral``). The orchestrator owns
    all GitHub check transitions; runners only publish heartbeat/final-state
    objects to S3.
    """

    @staticmethod
    def _api(method, url, token, json_body=None):
        import requests

        from .check_run import _resolve_token

        resp = requests.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {_resolve_token(token)}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json=json_body,
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    @classmethod
    def queue(cls, token, repo, head_sha, name, output=None):
        body = {"name": name, "head_sha": head_sha, "status": "queued"}
        if output is not None:
            body["output"] = output
        data = cls._api(
            "POST",
            f"https://api.github.com/repos/{repo}/check-runs",
            token,
            body,
        )
        return cls(token, repo, data["id"], name)

    @classmethod
    def create_completed(
        cls, token, repo, head_sha, name, conclusion, output=None, details_url=None
    ):
        """Create a check run already in its terminal state in one POST.

        Used for jobs that never run (skipped) so the check posts its final
        conclusion directly, rather than the queue()->complete() two-call
        dance that briefly surfaces a pending check before flipping it.
        """
        body = {
            "name": name,
            "head_sha": head_sha,
            "status": "completed",
            "conclusion": conclusion,
        }
        if output is not None:
            body["output"] = output
        if details_url is not None:
            body["details_url"] = details_url
        data = cls._api(
            "POST",
            f"https://api.github.com/repos/{repo}/check-runs",
            token,
            body,
        )
        return cls(token, repo, data["id"], name)

    def __init__(self, token, repo, id, name):
        self.token = token
        self.repo = repo
        self.id = id
        self.name = name

    def set_in_progress(self, output=None, details_url=None):
        body = {"status": "in_progress"}
        if output is not None:
            body["output"] = output
        if details_url is not None:
            body["details_url"] = details_url
        self._api(
            "PATCH",
            f"https://api.github.com/repos/{self.repo}/check-runs/{self.id}",
            self.token,
            body,
        )

    def complete(self, conclusion, output=None, details_url=None):
        body = {"status": "completed", "conclusion": conclusion}
        if output is not None:
            body["output"] = output
        if details_url is not None:
            body["details_url"] = details_url
        self._api(
            "PATCH",
            f"https://api.github.com/repos/{self.repo}/check-runs/{self.id}",
            self.token,
            body,
        )


class JobStatus(Enum):
    PENDING = "pending"  # not yet runnable (deps unresolved)
    READY = "ready"  # all deps resolved, queued for kick
    QUEUED = "queued"  # dispatched to runner pool, awaiting first heartbeat
    RUNNING = "running"  # runner has received the task and emitted heartbeat
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"  # didn't need to run — Config Workflow marked the job
    # out (cache hit, not affected by diff, missing opt-in
    # label). Not a failure: outputs are still reachable
    # from S3. SUCCESS-equivalent for dep resolution.
    CANCELLED = "cancelled"  # couldn't run — the run was cancelled (user action,
    # new push) OR an upstream dep failed (cascade).
    # Counts as a failure for workflow-level summary.
    # Maps 1:1 to the Checks API ``cancelled`` conclusion.


_TERMINAL = {
    JobStatus.SUCCESS,
    JobStatus.FAILURE,
    JobStatus.SKIPPED,
    JobStatus.CANCELLED,
}


class JobState:
    """Mutable runtime state for one job in a workflow run."""

    def __init__(self, job, workflow_state=None):
        self.job = job
        self.check = None  # JobCheckRun, created lazily on kick()
        self._workflow_state = workflow_state  # back-ref for SQS dispatch
        self.status = JobStatus.PENDING
        self.rc = None
        self.started_at = None
        self.finished_at = None
        self.filter_reason = None  # set by .skip() when Config Workflow skips it
        # S3-heartbeat liveness. ``last_heartbeat_ts`` stays None until the
        # orchestrator's sweep first sees a heartbeat file in S3; once seen,
        # the job transitions to RUNNING, the check flips to in_progress, and
        # stale-heartbeat checks apply.
        self.last_heartbeat_ts = None
        self.runner_instance_id = None
        self.last_heartbeat_phase = None
        # Raw job Result (plain dict) as shipped in the completion payload.
        # Populated by sweep_completions; rendered into the check-run output
        # and retained for AI observation.
        self.result = None

    @property
    def name(self):
        return self.job.name

    def _update_check(self, transition):
        """Run a check-run API call; never let it take down the orchestrator."""
        if self.check is None:
            return
        try:
            transition(self.check)
        except Exception as e:
            print(f"  [warn] check update for {self.name!r}: {type(e).__name__}: {e}")

    def _create_check(self):
        """Queue the GitHub check run (status=queued) — called at kick time.

        Shows up in the PR as a pending check the moment the orchestrator
        decides to run the job, not back at workflow-start time. The check
        output names the target runner pool so reviewers can tell what
        kind of runner the job was dispatched to (and spot when a job is
        stuck waiting on an empty pool).
        """
        if self.check is not None:
            return
        ws = self._workflow_state
        if ws is None or not ws.can_post_checks:
            return
        check_name = f"{ws.workflow.name} / {self.name}"
        runs_on = ", ".join(self.job.runs_on) if self.job.runs_on else "default"
        output = {
            "title": "QUEUED",
            "summary": f"QUEUED: job dispatched to runner pool `{runs_on}`.",
        }
        try:
            self.check = JobCheckRun.queue(
                ws._gh_token, ws._repo, ws._head_sha, check_name, output=output
            )
        except Exception as e:
            print(
                f"  [warn] could not queue check for {check_name!r}: "
                f"{type(e).__name__}: {e}"
            )

    def _create_completed_check(self, conclusion, output=None, details_url=None):
        """Post the GitHub check run directly in its terminal state.

        For jobs that never run (skipped), this posts the final conclusion in
        a single API call instead of queue()->complete(), which would briefly
        show a pending check before flipping it.
        """
        if self.check is not None:
            return
        ws = self._workflow_state
        if ws is None or not ws.can_post_checks:
            return
        check_name = f"{ws.workflow.name} / {self.name}"
        try:
            self.check = JobCheckRun.create_completed(
                ws._gh_token,
                ws._repo,
                ws._head_sha,
                check_name,
                conclusion,
                output=output,
                details_url=details_url,
            )
        except Exception as e:
            print(
                f"  [warn] could not post {conclusion} check for {check_name!r}: "
                f"{type(e).__name__}: {e}"
            )

    def kick(self):
        """Transition READY -> QUEUED, post the pending check, and dispatch
        to the runner.

        Two dispatch paths, one print:
          * local mode → ``_dispatch_local`` runs the job synchronously as a
            subprocess and calls ``finish`` before returning;
          * CI mode  → ``_dispatch`` sends a ``job_task`` to the per-runner
            SQS queue and returns immediately; the runner writes final state
            to S3, which ``wait()`` picks up to drive ``finish``.

        Either way the ``[KICK ]`` line is printed before the dispatch call
        so the local subprocess's own output (and the eventual ``[DONE ]``
        from ``finish``) appears beneath it in chronological order.
        """
        if self.status != JobStatus.READY:
            return
        self.status = JobStatus.QUEUED
        self.started_at = time.time()
        runs_on = ", ".join(self.job.runs_on) if self.job.runs_on else "default"

        # Queue the check run at the moment of kick, so nothing shows up on
        # the PR until the orchestrator actually decides to run the job.
        self._create_check()

        ws = self._workflow_state
        target = (
            "local"
            if ws is not None and ws.local_mode
            else _queue_for_runs_on(self.job.runs_on)
        )
        assert target is not None, (
            f"Job {self.name!r} has no dispatch target: runs_on={self.job.runs_on!r} "
            f"and orchestrator is not in local mode"
        )

        print(f"[KICK ] {self.name:70s} runs_on={runs_on}  -> {target}")
        if not ws._dispatch(self, target):
            # Dispatch failed (e.g. SQS error, missing queue) — fail the job;
            # nothing else will ever drive it forward. Pass an explicit output
            # so the terminal check doesn't keep the earlier "QUEUED" summary.
            self.finish(
                success=False,
                output={
                    "title": "DISPATCH FAILED",
                    "summary": (
                        f"Failed to dispatch job to runner pool `{target}` "
                        f"(its SQS queue is missing or unreachable)."
                    ),
                },
            )

    def finish(self, success=True, output=None, details_url=None):
        """Transition in-flight jobs -> SUCCESS/FAILURE and emit a finish line.

        The orchestrator owns the GitHub check lifecycle: runners publish
        final state to S3, and this method completes the check.
        """
        if self.status not in (JobStatus.QUEUED, JobStatus.RUNNING):
            return
        self.status = JobStatus.SUCCESS if success else JobStatus.FAILURE
        self.finished_at = time.time()
        self.rc = 0 if success else 1
        self._update_check(
            lambda c: c.complete(
                "success" if success else "failure",
                output=output,
                details_url=details_url,
            )
        )
        duration = self.finished_at - (self.started_at or self.finished_at)
        tag = "[DONE ]" if success else "[FAIL ]"
        print(f"{tag} {self.name:70s} ({duration:.1f}s)")

    def skip(self, reason="", output=None, details_url=None, post_check=False):
        """Transition PENDING -> SKIPPED.

        Used when the job doesn't need to run — Config Workflow marked
        it out (cache hit, not affected by diff, missing opt-in label).
        Not a failure: outputs are still reachable from S3.

        Config Workflow skips request per-job checks so the Checks API shows
        the same job names regardless of whether work ran or was skipped.
        """
        if self.status != JobStatus.PENDING:
            return False
        self.status = JobStatus.SKIPPED
        self.filter_reason = reason
        if post_check:
            self._create_completed_check(
                "skipped", output=output, details_url=details_url
            )
        suffix = f" ({reason})" if reason else ""
        print(f"[SKIP ] {self.name:70s}{suffix}")
        return True

    def fail_dead(self, reason):
        """Transition an in-flight job -> FAILURE because it stopped responding.

        Triggered by the orchestrator's heartbeat sweep when the job either
        was not picked up by ``RUNNER_PICKUP_TIMEOUT_S`` or stopped emitting
        heartbeats after pickup. The runner is presumed gone, so the
        orchestrator completes the check itself with ``failure`` — nothing
        else will ever drive the check forward.
        """
        if self.status not in (JobStatus.QUEUED, JobStatus.RUNNING):
            return
        self.status = JobStatus.FAILURE
        self.finished_at = time.time()
        self.rc = 1
        output = {"title": reason, "summary": reason}
        self._update_check(lambda c: c.complete("failure", output=output))
        duration = self.finished_at - (self.started_at or self.finished_at)
        print(f"[DEAD ] {self.name:70s} ({duration:.1f}s) {reason}")

    def cancel(self, reason="run cancelled"):
        """Transition pending or in-flight jobs -> CANCELLED.

        Used for two cases that both produce a Checks API ``cancelled``
        conclusion:
          - the run itself was cancelled (``WorkflowState.cancel_unfinished_jobs``
            on a new-push or UI Cancel signal);
          - an upstream dep ended in FAILURE or CANCELLED, so this job
            can't run either (``get_ready`` cascade).
        PENDING jobs have no check-run yet so nothing to patch.
        In-flight jobs have a queued or in-progress check-run; the
        orchestrator completes it here because the runner will never post
        back.
        """
        if self.status not in (
            JobStatus.PENDING,
            JobStatus.QUEUED,
            JobStatus.RUNNING,
        ):
            return
        was_in_flight = self.status in (JobStatus.QUEUED, JobStatus.RUNNING)
        self.status = JobStatus.CANCELLED
        if was_in_flight:
            self.finished_at = time.time()
            # Pass an explicit output so the terminal check reflects the
            # cancellation instead of keeping the earlier "QUEUED" summary.
            self._update_check(
                lambda c: c.complete(
                    "cancelled",
                    output={"title": "CANCELLED", "summary": f"CANCELLED: {reason}."},
                )
            )
        print(f"[CANCL] {self.name:70s} ({reason})")


class WorkflowState:
    """DAG-aware live state of a workflow run.

    ``event`` (the SQS workflow-trigger body) is stashed so ``JobState.kick``
    can build task messages with the full PR context (repo, pr_number,
    head_sha, head_ref).

    ``gh_token``, ``repo`` and ``head_sha`` are kept so each ``JobState`` can
    queue its own GitHub check run lazily at kick time — nothing is posted
    on the PR until the orchestrator actually decides to run a job.
    """

    def __init__(
        self,
        workflow,
        event=None,
        gh_token=None,
        repo=None,
        head_sha=None,
        run_id=None,
        local_mode=False,
    ):
        self.workflow = workflow
        self.local_mode = local_mode
        self._event = event or {}
        self._gh_token = gh_token
        self._repo = repo
        self._head_sha = head_sha
        # Event timestamp (lambda receive time). Older runs for the same PR
        # are cancelled when a new event with a larger event_ts triggers the
        # queue-scoped `pr/<pr>/cancel-before-<scope>` marker — see
        # sweep_cancel.
        self._event_ts = float(self._event.get("event_ts") or 0.0)
        self._pr_number = self._event.get("pr_number")
        # Unique identifier for this specific orchestrator run — the GitHub
        # check run ID (string), used as the suffix of the per-run S3 prefix.
        # Falls back to a UUID when running without a check (local mode).
        import uuid

        self._run_id = str(run_id) if run_id else str(uuid.uuid4())
        # Last environment.json snapshot published by a finished job. Seeded
        # into every subsequent dispatched task so WORKFLOW_CONFIG (and other
        # job-side additions) flow forward the same way step outputs do in
        # GHA. Later completions overwrite earlier ones — the serialized
        # environment is already cumulative.
        self._environment = None
        self.cancelled = (
            False  # set by sweep_cancel() on cancel-request / cancel-before
        )

        # S3 client used by sweep_liveness, sweep_completions, sweep_cancel,
        # and the orchestrator → runners kill flag. Only created in CI mode;
        # local mode runs jobs synchronously inside `kick` (no S3 needed).
        if not local_mode:
            import boto3

            region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
            self._s3 = boto3.client("s3", region_name=region)
        else:
            self._s3 = None

        # SQS client is still used by ``_dispatch`` for the per-runner-pool
        # job_task queues (`<project-slug>-<label>`). Phase 2b only retired the
        # per-run completions queue.
        self._sqs = None
        self._queue_urls = {}

        # Per-run S3 prefix. Lambda → orchestrator cancel flows through S3:
        # lambda writes either runs/<run_id>/cancel-request (manual UI button)
        # or a queue-scoped pr/<pr>/cancel-before-<scope> marker (new push,
        # fan-out to older runs in the same orchestrator scope only);
        # sweep_cancel polls both. The orchestrator → runners kill flag at
        # runs/<run_id>/cancel is written by cancel_unfinished_jobs.
        from ..settings import Settings

        self._cancel_s3_bucket = Settings.S3_ARTIFACT_BUCKET
        self._runs_s3_prefix = f"runs/{self._run_id}"
        self._cancel_s3_key = f"{self._runs_s3_prefix}/cancel"
        self._cancel_request_s3_key = f"{self._runs_s3_prefix}/cancel-request"
        queue_name = (os.environ.get("PRAKTIKA_CONTROLLER_QUEUE") or "").strip()
        cancel_scope = "base" if queue_name.endswith("-base") else "default"
        self._pr_cancel_before_s3_key = (
            f"pr/{self._pr_number}/cancel-before-{cancel_scope}"
            if self._pr_number
            else None
        )

        self.jobs = {
            job.name: JobState(job, workflow_state=self) for job in workflow.jobs
        }

        self._levels, job_deps = build_job_dag(workflow)
        self._deps = job_deps
        self._dependents = defaultdict(set)
        for name, deps in job_deps.items():
            for dep in deps:
                self._dependents[dep].add(name)

    @property
    def can_post_checks(self):
        """True iff we have everything needed to open a GitHub check run."""
        return bool(self._gh_token and self._repo and self._head_sha)

    def apply_workflow_config(self, workflow_config):
        """Apply Config Workflow decisions from the runner environment.

        Config Workflow exposes two skip surfaces:
          - ``filtered_jobs``: ``{job_name: reason}`` for jobs filtered by
            changed files, labels, or other workflow config logic.
          - ``cache_success`` + ``cache_jobs``: jobs whose prior successful
            result can be reused from cache.

        Both must become SKIPPED in the orchestrator DAG. SKIPPED is treated
        as SUCCESS-equivalent by ``get_ready`` because the skipped job's
        outputs are already in S3 from a prior run.

        Unknown job names are ignored so Config Workflow and the orchestrator
        don't have to agree on the exact set of workflow jobs (e.g. a job
        enabled only in the YAML but removed from the Python config).
        """
        if not isinstance(workflow_config, dict):
            return

        filtered = workflow_config.get("filtered_jobs") or {}
        cache_success = workflow_config.get("cache_success") or []
        cache_jobs = workflow_config.get("cache_jobs") or {}
        for name, reason in filtered.items():
            js = self.jobs.get(name)
            if js is None:
                continue
            if js.status != JobStatus.PENDING:
                continue
            reason = reason or "Filtered by Config Workflow"
            output = {
                "title": "SKIPPED",
                "summary": f"SKIPPED: {reason}.",
            }
            js.skip(reason, output=output, post_check=True)

        for name in cache_success:
            if name in filtered:
                continue
            js = self.jobs.get(name)
            if js is None:
                continue
            if js.status != JobStatus.PENDING:
                continue

            reason = "reused from cache"
            details_url = self._cached_job_report_url(name, cache_jobs.get(name))
            output = {
                "title": "SKIPPED",
                "summary": "SKIPPED: reused from cache.",
            }
            if details_url:
                output["summary"] += f" [CI Report]({details_url})"
                output["text"] = f"Reused a successful cached result for `{name}`."
            js.skip(
                reason,
                output=output,
                details_url=details_url,
                post_check=True,
            )

    def _cached_job_report_url(self, job_name, record):
        if not record:
            return None
        sha = _record_value(record, "sha", "")
        if not sha:
            return None
        workflow_name = _record_value(record, "workflow", "") or self.workflow.name
        pr_number = _record_value(record, "pr_number", 0) or 0
        branch = _record_value(record, "branch", "")
        if not pr_number and not branch:
            return None
        try:
            from ..info import Info

            return Info.get_specific_report_url_static(
                pr_number=pr_number,
                branch=branch,
                sha=sha,
                job_name=job_name,
                workflow_name=workflow_name,
            )
        except Exception as e:
            print(
                f"  [warn] could not build cached report URL for {job_name!r}: "
                f"{type(e).__name__}: {e}"
            )
            return None

    # ---------------------------------------------------------- liveness

    def _heartbeat_s3_key(self, job_name):
        return f"{self._runs_s3_prefix}/{_normalize_job_name_for_s3(job_name)}/heartbeat.json"

    def _final_state_s3_key(self, job_name):
        return (
            f"{self._runs_s3_prefix}/{_normalize_job_name_for_s3(job_name)}/final.json"
        )

    def sweep_cancel(self):
        """Detect lambda-driven cancel signals on S3.

        Two channels:
          - ``runs/<run_id>/cancel-request`` (manual UI cancel button) —
            lambda writes this on a check_run.requested_action=cancel
            event addressed to a specific run.
          - ``pr/<pr>/cancel-before-<scope>`` carrying ``{ts, head_sha}``
            (new push) — lambda writes this on synchronize. Every still-running
            orchestrator for the same PR and orchestrator scope with
            ``event_ts < ts`` and a different head SHA self-cancels; the
            freshly enqueued run for that same SHA stays alive.

        Both paths set ``state.cancelled = True``; the main loop handles
        that flag exactly once via ``cancel_unfinished_jobs``, so seeing
        the flag again on subsequent sweeps is a no-op.
        """
        if self._s3 is None or self.local_mode or self.cancelled:
            return
        try:
            self._s3.head_object(
                Bucket=self._cancel_s3_bucket, Key=self._cancel_request_s3_key
            )
            print(f"[CANCEL] run {self._run_id} (manual)")
            self.cancelled = True
            return
        except Exception:
            pass  # not present, or transient S3 error — try again next sweep

        if not self._pr_cancel_before_s3_key:
            return
        try:
            obj = self._s3.get_object(
                Bucket=self._cancel_s3_bucket, Key=self._pr_cancel_before_s3_key
            )
            payload = json.loads(obj["Body"].read())
            cancel_before = float(payload.get("ts", 0))
            cancel_sha = str(payload.get("head_sha") or "").strip()
        except Exception:
            return
        current_sha = str(self._event.get("head_sha") or "").strip()
        if cancel_before > self._event_ts > 0 and (
            not cancel_sha or cancel_sha != current_sha
        ):
            print(
                f"[CANCEL] run {self._run_id} (newer event {cancel_before:.0f} > "
                f"event_ts {self._event_ts:.0f})"
            )
            self.cancelled = True

    def sweep_completions(self):
        """Advance in-flight jobs whose ``final.json`` has landed in S3.

        Replaces the SQS ``job_completion`` path: the runner writes
        ``runs/<run_id>/<job>/final.json`` with ``{rc, environment, ...}``
        on exit; we read it here and call ``js.finish``. ``finish`` is a
        no-op for non-in-flight jobs, so seeing the same file twice (e.g.
        across orchestrator restart) is harmless. No-op in local mode and
        when no job is in flight.
        """
        if self._s3 is None or self.local_mode:
            return
        running = [
            js
            for js in self.jobs.values()
            if js.status in (JobStatus.QUEUED, JobStatus.RUNNING)
        ]
        if not running:
            return
        for js in running:
            key = self._final_state_s3_key(js.name)
            try:
                obj = self._s3.get_object(Bucket=self._cancel_s3_bucket, Key=key)
                payload = json.loads(obj["Body"].read())
            except Exception:
                # Not present yet, or transient S3 error — try again next sweep.
                continue
            rc = int(payload.get("rc", 1))
            env = payload.get("environment")
            if isinstance(env, dict):
                self._environment = env
                wc = env.get("WORKFLOW_CONFIG")
                if isinstance(wc, dict):
                    self.apply_workflow_config(wc)
            details_url = payload.get("details_url")
            if not isinstance(details_url, str):
                details_url = None
            instance_id = payload.get("instance_id")
            if isinstance(instance_id, str) and instance_id.strip():
                js.runner_instance_id = instance_id.strip()
            # The runner ships the raw job Result in the payload. Stash it on
            # the JobState for AI observation, then render it into the
            # check-run output here (the orchestrator owns the check
            # lifecycle).
            output = None
            result_dict = payload.get("result")
            if isinstance(result_dict, dict):
                js.result = result_dict
                try:
                    from copy import deepcopy

                    from ..result import Result

                    # from_dict mutates its argument, so reconstruct from a
                    # copy to keep js.result a plain, serializable dict.
                    result = Result.from_dict(deepcopy(result_dict))
                    output = _build_check_output(
                        result,
                        rc,
                        instance_id=js.runner_instance_id or "",
                        report_url=details_url or "",
                    )
                except Exception as e:
                    print(
                        f"  [warn] could not render Result for {js.name!r}: "
                        f"{type(e).__name__}: {e}"
                    )
            js.finish(success=(rc == 0), output=output, details_url=details_url)

    def sweep_liveness(self, now=None):
        """Mark in-flight jobs whose runner stopped responding as FAILURE.

        Reads each in-flight job's ``heartbeat.json`` from S3 and applies
        the two liveness rules (pickup grace + dead threshold). Called from
        ``wait()`` once per loop iteration. No-op in local mode (no S3
        client) and when nothing is in flight.
        """
        if self._s3 is None or self.local_mode:
            return
        running = [
            js
            for js in self.jobs.values()
            if js.status in (JobStatus.QUEUED, JobStatus.RUNNING)
        ]
        if not running:
            return
        now = now if now is not None else time.time()
        for js in running:
            runs_on = ", ".join(js.job.runs_on) if js.job.runs_on else "default"
            key = self._heartbeat_s3_key(js.name)
            heartbeat_missing = False
            try:
                obj = self._s3.get_object(Bucket=self._cancel_s3_bucket, Key=key)
                body = obj["Body"].read()
                hb = json.loads(body)
                ts = float(hb.get("ts", 0))
                if ts > 0:
                    js.last_heartbeat_ts = ts
                    phase = str(hb.get("phase") or "").strip()
                    if phase:
                        js.last_heartbeat_phase = phase
                    instance_id = str(hb.get("instance_id") or "").strip()
                    if instance_id:
                        js.runner_instance_id = instance_id
                    if js.status == JobStatus.QUEUED:
                        js.status = JobStatus.RUNNING
                        output = {
                            "title": "RUNNING",
                            "summary": "RUNNING: runner picked up the job.",
                        }
                        if js.runner_instance_id:
                            output["summary"] = f"RUNNING on runner `{instance_id}`."
                        if phase:
                            output["summary"] += f" Phase: `{phase}`."
                        js._update_check(lambda c: c.set_in_progress(output=output))
                        duration = now - (js.started_at or now)
                        print(f"[PICK ] {js.name:70s} ({duration:.1f}s)")
            except Exception as e:
                if _is_missing_s3_key_error(e):
                    # Heartbeat file may not exist yet. If pickup grace
                    # expires without ever seeing one, declare the job dead.
                    heartbeat_missing = True
                else:
                    print(
                        f"  [warn] could not read heartbeat for {js.name!r}: "
                        f"{type(e).__name__}: {e}"
                    )
                    continue

            kicked = js.started_at or now
            age_since_kick = now - kicked
            if js.status == JobStatus.QUEUED:
                if heartbeat_missing and age_since_kick > RUNNER_PICKUP_TIMEOUT_S:
                    js.fail_dead(
                        f"runner pool `{runs_on}` never started job (no heartbeat in "
                        f"{int(age_since_kick)}s, timeout={RUNNER_PICKUP_TIMEOUT_S}s)"
                    )
            elif js.status == JobStatus.RUNNING:
                age_since_hb = now - js.last_heartbeat_ts
                if age_since_hb > HEARTBEAT_TIMEOUT_S:
                    runner = js.runner_instance_id
                    phase = js.last_heartbeat_phase
                    if runner:
                        reason = f"runner `{runner}` in pool `{runs_on}` stopped heartbeating"
                        if phase:
                            reason += f" during phase `{phase}`"
                        reason += (
                            f" (no heartbeat in {int(age_since_hb)}s, "
                            f"timeout={HEARTBEAT_TIMEOUT_S}s)"
                        )
                    else:
                        reason = (
                            f"runner pool `{runs_on}` died "
                            f"(no heartbeat in {int(age_since_hb)}s, "
                            f"timeout={HEARTBEAT_TIMEOUT_S}s)"
                        )
                    js.fail_dead(reason)

    # ---------------------------------------------------------- dispatch

    def _dispatch(self, job_state, queue_name):
        """Send a ``job_task`` message to ``queue_name`` for ``job_state``.

        Returns True on success, False on any failure (missing boto3, queue
        doesn't exist, SQS error). On failure ``kick()`` fails the job —
        nothing else will ever drive it forward.
        """
        task = {
            "type": "job_task",
            "repo": self._event.get("repo", ""),
            "pr_number": self._event.get("pr_number"),
            "head_sha": self._event.get("head_sha", ""),
            "head_ref": self._event.get("head_ref", ""),
            "base_ref": self._event.get("base_ref", ""),
            "sender": self._event.get("sender", ""),
            "title": self._event.get("title", ""),
            "labels": self._event.get("labels", []),
            "workflow_name": self.workflow.name,
            "job_name": job_state.name,
            "always_run": bool(job_state.job.always_run),
            "runs_on": list(job_state.job.runs_on) if job_state.job.runs_on else [],
            "cancel_s3_bucket": self._cancel_s3_bucket,
            "cancel_s3_key": self._cancel_s3_key,
            "heartbeat_s3_bucket": self._cancel_s3_bucket,
            "heartbeat_s3_key": self._heartbeat_s3_key(job_state.name),
            "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
            "final_state_s3_bucket": self._cancel_s3_bucket,
            "final_state_s3_key": self._final_state_s3_key(job_state.name),
            "check_run_id": job_state.check.id if job_state.check else None,
            "environment": self._environment,
        }

        if self.local_mode:
            return self._dispatch_local(job_state, task)

        try:
            if self._sqs is None:
                import boto3

                region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
                self._sqs = boto3.client("sqs", region_name=region)

            url = self._queue_urls.get(queue_name)
            if url is None:
                url = self._sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
                self._queue_urls[queue_name] = url

            self._sqs.send_message(QueueUrl=url, MessageBody=json.dumps(task))
            return True
        except Exception as e:
            print(
                f"  [warn] dispatch of {job_state.name!r} to {queue_name!r} failed: "
                f"{type(e).__name__}: {e}"
            )
            return False

    def _dispatch_local(self, job_state, task):
        """Run the job synchronously as a subprocess.

        After the child exits, snapshot ``environment.json`` and store it as
        ``self._environment`` so downstream jobs in the same local run inherit
        whatever the upstream job wrote (most importantly ``WORKFLOW_CONFIG``
        from Config Workflow). In SQS mode this hand-off goes through the
        ``job_completion`` message that ``wait()`` consumes; locally there is
        no message round-trip, so we read the file directly — same result.
        """
        import subprocess
        from ..settings import Settings

        task_file = os.path.join(
            Settings.TEMP_DIR, f"task_{job_state.name.replace(' ', '_')}.json"
        )
        os.makedirs(Settings.TEMP_DIR, exist_ok=True)
        with open(task_file, "w") as f:
            json.dump(task, f, indent=2)

        env = {**os.environ, "PRAKTIKA_LOCAL_RUN": "1"}
        result = subprocess.run(["praktika", "orchestrate", "job", task_file], env=env)

        env_path = os.path.join(Settings.TEMP_DIR, "environment.json")
        if os.path.isfile(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    env_snapshot = json.load(f)
                self._environment = env_snapshot
                wc = env_snapshot.get("WORKFLOW_CONFIG")
                if isinstance(wc, dict):
                    self.apply_workflow_config(wc)
            except Exception as e:
                print(f"  [warn] could not read env snapshot from {env_path}: {e}")

        job_state.finish(success=(result.returncode == 0))
        return True

    # ------------------------------------------------------------ lifecycle

    def not_finished(self):
        """True while any job is still pending / ready / running."""
        return any(j.status not in _TERMINAL for j in self.jobs.values())

    def get_ready(self):
        """Promote PENDING jobs whose deps are resolved -> READY and return them.

        Normal jobs:
          - any dep in FAILURE or CANCELLED ⇒ cascade this job to CANCELLED
            (upstream failed / upstream cancelled, this can't proceed);
          - every dep in SUCCESS or SKIPPED ⇒ promote to READY (SKIPPED
            outputs still exist in S3 from a prior run — SUCCESS-equivalent
            for dep resolution).

        ``always_run`` jobs (Finish Workflow is the only one
        today) promote to READY once every dep reaches *any* terminal
        state, regardless of success/failure/skip/cancel. That's how the
        post-run jobs (CIDB writeback, merge-ready check, Slack notify)
        fire even when the run was cancelled or the DAG failed.
        """
        ready = []
        for name, js in self.jobs.items():
            if js.status != JobStatus.PENDING:
                continue
            dep_states = [self.jobs[d].status for d in self._deps.get(name, ())]
            if js.job.always_run:
                if all(s in _TERMINAL for s in dep_states):
                    js.status = JobStatus.READY
                    ready.append(js)
                continue
            if any(s == JobStatus.FAILURE for s in dep_states):
                js.cancel(reason="upstream failed")
                continue
            if any(s == JobStatus.CANCELLED for s in dep_states):
                js.cancel(reason="upstream cancelled")
                continue
            if all(s in (JobStatus.SUCCESS, JobStatus.SKIPPED) for s in dep_states):
                js.status = JobStatus.READY
                ready.append(js)
        return ready

    def cancel_unfinished_jobs(self):
        """When a cancel signal arrives mid-run, mark every PENDING or
        in-flight job that isn't flagged ``always_run`` as
        CANCELLED. Leaves unconditional post-run jobs (Finish Workflow)
        alone so they still fire after their deps settle.

        In-flight jobs that are cancelled here had their task already
        dispatched to a runner. The cancel flag written to S3 signals those
        runners to tear down; the eventual final state (if it still arrives)
        will be ignored because finish() only accepts in-flight states.
        """
        has_running = any(
            js.status in (JobStatus.QUEUED, JobStatus.RUNNING) and not js.job.always_run
            for js in self.jobs.values()
        )
        for js in self.jobs.values():
            if (
                js.status in (JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING)
                and not js.job.always_run
            ):
                js.cancel(reason="run cancelled")
        if has_running and self._s3 is not None:
            try:
                self._s3.put_object(
                    Bucket=self._cancel_s3_bucket,
                    Key=self._cancel_s3_key,
                    Body=b"cancelled",
                )
                print(
                    f"  [cancel] wrote s3://{self._cancel_s3_bucket}/{self._cancel_s3_key}"
                )
            except Exception as e:
                print(f"  [warn] could not write cancel flag: {type(e).__name__}: {e}")

    def wait(self):
        """Block briefly, then sweep S3 for heartbeats / final state / cancel.

        Local mode dispatched the job synchronously inside ``kick`` and the
        job is already in a terminal state by the time we get here; nothing
        to wait on. In CI mode there is no SQS queue any more (phase 2b
        retired the per-run completions queue): wait() simply sleeps for
        ``WAIT_POLL_INTERVAL_S`` and then sweeps the three S3 channels.
        Liveness, completion, and cancel all live under
        ``runs/<run_id>/`` (cancel-by-event-ts also reads
        ``pr/<pr>/cancel-before-<scope>``).
        """
        if self._s3 is None or self.local_mode:
            return

        # Only block if there are still dispatched jobs in-flight.
        if not any(
            js.status in (JobStatus.QUEUED, JobStatus.RUNNING)
            for js in self.jobs.values()
        ):
            return

        time.sleep(WAIT_POLL_INTERVAL_S)
        self.sweep_cancel()
        self.sweep_completions()
        self.sweep_liveness()

    def cleanup(self):
        """End-of-run hook.

        Phase 2b retired the per-run SQS queue, so this is now a no-op.
        Per-run S3 objects under ``runs/<run_id>/`` (heartbeats, final
        states, cancel flags) are intentionally left in place — they are
        useful for debugging and small enough to be cleaned up by the
        bucket's lifecycle policy rather than by the orchestrator at end
        of run.
        """
        return

    # ------------------------------------------------------------ reporting

    def print_plan(self):
        """Print the static execution plan (levels + dependencies)."""
        total_jobs = sum(len(lv) for lv in self._levels)
        print(f"\n{'=' * 80}")
        print(f"Execution plan for workflow [{self.workflow.name}]")
        print(f"Total jobs: {total_jobs}, Execution levels: {len(self._levels)}")
        print(f"{'=' * 80}")
        for i, level in enumerate(self._levels):
            print(f"\n--- Level {i} ({len(level)} jobs, parallel) ---")
            for name in level:
                job = self.jobs[name].job
                deps = self._deps.get(name, set())
                runs_on = ", ".join(job.runs_on) if job.runs_on else "default"
                dep_str = f" <- [{', '.join(sorted(deps))}]" if deps else ""
                provides_str = (
                    f" -> [{', '.join(job.provides)}]" if job.provides else ""
                )
                print(f"  {name}")
                print(f"    runner: {runs_on}{dep_str}{provides_str}")
        print(f"\n{'=' * 80}\n")

    def print_summary(self):
        """Print a per-status count of jobs at end of run."""
        counts = defaultdict(int)
        for js in self.jobs.values():
            counts[js.status] += 1
        total = sum(counts.values())
        print(f"\n{'=' * 80}")
        print(f"Workflow [{self.workflow.name}] finished — {total} jobs total")
        for status in JobStatus:
            if counts[status]:
                print(f"  {status.value:10s} {counts[status]}")
        print(f"{'=' * 80}\n")

    def any_failed(self):
        """True if any job ended in a non-success terminal state that
        indicates something actually went wrong.

        FAILURE: the job ran and exited non-zero.
        CANCELLED: the run was cancelled, or an upstream failed and this
        job couldn't run.

        SKIPPED does *not* count here — a skipped job was a deliberate
        decision by Config Workflow ("not affected by this diff" / "cache
        hit"), its outputs are already in S3, the run is healthy.
        """
        return any(
            js.status in (JobStatus.FAILURE, JobStatus.CANCELLED)
            for js in self.jobs.values()
        )

    # ------------------------------------------------------------ markdown

    def md_status_summary(self):
        """One-line summary ("2 success, 1 running, 3 pending") for the
        top-level check's `output.summary`. Uses `JobStatus` values so the
        wording matches `md_status` and `print_summary`."""
        counts = defaultdict(int)
        for js in self.jobs.values():
            counts[js.status] += 1
        bits = [f"{counts[s]} {s.value}" for s in JobStatus if counts[s]]
        return ", ".join(bits) or "no jobs"

    def md_status(self):
        """Markdown snapshot of the current run state for the top-level
        workflow check's `output.text`. Designed to be re-rendered every
        time the state changes — the orchestrator PATCHes the check with
        this on every loop iteration, so the PR UI tracks progress live."""
        event = self._event
        sha = (self._head_sha or "")[:12]
        lines = []
        lines.append(
            f"**Event:** `{event.get('type', '')}.{event.get('action', '')}`  "
        )
        if sha:
            lines.append(f"**SHA:** `{sha}`  ")
        pr = event.get("pr_number")
        if pr:
            lines.append(f"**PR:** #{pr}  ")
        lines.append("")
        lines.append(f"**Status:** {self.md_status_summary()}")
        lines.append("")
        lines.append("| Job | Status | Duration |")
        lines.append("|---|---|---|")
        now = time.time()
        for js in self.jobs.values():
            if js.started_at:
                end = js.finished_at or now
                dur = f"{int(end - js.started_at)}s"
            else:
                dur = "—"
            lines.append(f"| `{js.name}` | {js.status.value} | {dur} |")
        return "\n".join(lines)
