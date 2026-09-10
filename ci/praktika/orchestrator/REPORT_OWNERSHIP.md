---
title: Orchestrator-owned workflow report (native path)
description: Make the single orchestrator the sole writer of the workflow report summary in the native (non-GitHub-Actions) path, replacing the concurrent per-job merge + destructive version=0 reset that corrupts the report on restarts.
doc_type: design
---

# Orchestrator-owned workflow report {#orchestrator-owned-report}

Status: **implemented.** Fixes the report-corruption class where a
killed-and-restarted orchestrator run left succeeded jobs wrongly marked
`NOT_FINALIZED`.

## Problem

The workflow report summary (`{report_prefix}/result_<workflow>.json`) is built
the **GitHub-Actions way**, where there is no central process, so every job races
to update a shared file:

- **Config Workflow** writes it with `push_pending_ci_report` at **`version=0`**
  — a *destructive, unconditional reset* (all rows PENDING).
- Each **job's runner** merges its own row via `post_run → update_workflow_results`
  (optimistic version-CAS).
- **Finish Workflow** reads the summary and stamps any still-PENDING row
  `NOT_FINALIZED` (it has no fallback in native mode — the GitHub job-status file
  doesn't exist off GitHub Actions).

On a restart (e.g. the orchestrator killed mid-run, redelivery → a 2nd attempt, a
duplicate Config), a **late `version=0` reset wipes rows for jobs that already
finished**, and nothing re-writes them (the orchestrator considers a job with a
`final.json` *done* and never re-dispatches it — completion and the summary row
are decoupled). Those jobs are then reported `NOT_FINALIZED` even though they
succeeded (their own `result_<job>.json` is OK the whole time).

## Insight

Praktika is **not** GitHub Actions: there is a **single orchestrator per run**
that already tracks every job and already parses each job's **full `Result`** from
`final.json` (`state.py` `sweep_completions` → `js.result`). So the orchestrator
can be the **sole writer** of the report summary:

- **No concurrency** → no version-CAS, no `version=0` destructive reset, no races.
- **Self-healing** → the orchestrator rewrites the whole summary each loop from
  its authoritative state, so a job it knows is terminal is *always* terminal in
  the report, regardless of restarts, resets, or whether the runner re-ran.

## Design

### Gate (native vs GitHub Actions vs local)

Add `ORCHESTRATOR_OWNS_REPORT: bool = False` to `_Environment`. The orchestrator's
`_build_ci_environment` (job_runner) sets it **True** for every dispatched job, so
it flows to jobs via `environment.json`. It is False for local runs and under
GitHub Actions — so those keep the existing per-job writers.

Not gated on `GITHUB_ACTIONS` alone, because a **local** run (`praktika run`) also
lacks `GITHUB_ACTIONS` yet has no orchestrator — it must keep writing its own
report.

### Job side — what actually changed (and why not a full gate)

The clean "gate every job-side writer off" plan hit a constraint: the runner's
`post_run` computes the usage KPIs (storage/compute/pipeline) **inside the same
`update_workflow_results` call** that writes the rows, and Finish Workflow reads
them back for the **CIDB usage insert** (`runner.py`). CIDB stays on the runner,
so `post_run`'s `update_workflow_results` must stay too — you can't gate it off
without first splitting usage aggregation out (a larger change).

So instead of gating the writers, we **neutralise the one destructive
operation** — `push_pending_ci_report`'s `version=0` reset:

- `hook_html.push_pending_ci_report` → on the native path (`ORCHESTRATOR_OWNS_REPORT`),
  **create the summary once; never reset an existing one** (`_report_summary_exists`
  guard). A duplicate/late Config from a restart no longer wipes finished rows.
  GitHub Actions keeps the `version=0` reset (no orchestrator to rebuild rows).
- `hook_html.configure` / `pre_run` / `post_run` → **unchanged** (rows + usage +
  the per-job `result_<job>.json` keep flowing; CIDB untouched).
- `native_jobs._finish_workflow` → **unchanged**: with no destructive reset and
  the orchestrator re-asserting rows, a row that is still non-terminal at Finish
  time is a *genuine* problem, so its `NOT_FINALIZED` marking is now correct
  rather than spurious.

Combined with the orchestrator re-assert (increment 1), the wipe is structurally
impossible: no reset can erase a finished row, and even if one somehow did, the
orchestrator restores it next loop.

### Orchestrator side — re-assert each job's row (increment 1, shipped)

`WorkflowState.publish_report()`:

1. Lazily construct + dump an `_Environment` in the orchestrator process
   (`_ensure_report_env`, from the event: `WORKFLOW_NAME`, `PR_NUMBER`,
   `BRANCH=head_ref`, `SHA=head_sha`, `REPOSITORY`, …) so `_ResultS3` /
   `get_s3_prefix()` resolve. Done on first publish (after `_get_workflows`
   matching) so it can't change the env matching read.
2. For every job with a terminal `js.result` (parsed from `final.json`), call
   `_ResultS3.update_workflow_results(new_sub_results=[Result.from_dict(js.result)])`
   — the same version-CAS merge the runner uses (`drop_nested_results=True`), so
   rows render identically. Only jobs the orchestrator knows finished are
   re-asserted; cached/pending rows are left to the runner's `configure`/plan.
3. Called each `_drive_dag` loop (after `save_snapshot`), so the summary is
   corrected continuously — including while Finish Workflow is running.

Both steps are best-effort — report upkeep never crashes the run.

### Increment 2 (shipped) — remove the destructive reset

See "Job side" above: `push_pending_ci_report` no longer resets an existing
summary on the native path. This + the re-assert makes the wipe impossible.

### Increment 3 (shipped) — usage aggregation moves to the orchestrator

`update_workflow_results` gained `replace_usage` (SET instead of accumulate). On
the native path `publish_report` recomputes the **full** storage/compute/pipeline
aggregate from every finished job's `Result` (`js.result.ext` carries
`storage_usage`/`metrics`; compute is derived from runner + duration) and SETs it
each loop — idempotent, so re-publishing can't multiply the totals. The runner's
`post_run` stops contributing usage (`ORCHESTRATOR_OWNS_REPORT`) but still writes
rows / report messages and still sets `result.ext` for the orchestrator to read.
CIDB usage insertion stays on the runner and reads the totals off the summary.

### Deferred (follow-up, not correctness)

- **Retire the runner's row-writes (true "runner writes nothing").** Now that
  usage is off the runner, only rows/messages remain. Retiring those needs the
  orchestrator to author cached-job (SKIPPED) rows and per-job report messages,
  the top-level ext, and Finish Workflow to read the summary from S3. Larger
  change, no correctness benefit over the current state.
- **Finish Workflow's own usage.** The orchestrator counts a job's usage only
  after its `final.json` lands; Finish Workflow reads the summary for the CIDB
  insert *before* it finishes, so its own (cheap, native) usage isn't counted.
  Minor accuracy nit.
- **Reset reset-jobs' rows on resume.** On a re-run, a reset job's row shows its
  previous terminal result until it completes again. Cosmetic/transient.

## Rollout / risk

Both increments are additive on the native path and easily reverted (flip
`ORCHESTRATOR_OWNS_REPORT` default to False → old per-job path resumes; the
`push_pending` guard then never triggers). GitHub Actions and local runs are
unchanged. Validate on the live pipeline after deploy: confirm a normal run's
report renders identically, then confirm the incident case (a killed+restarted
run) no longer shows `NOT_FINALIZED`.

## Supersedes

- The Finish-Workflow "re-read `result_<job>.json`" fallback (Fix B) — dropped;
  the orchestrator writing truth makes it unnecessary.

## Touch points (shipped)

- `praktika/_environment.py` — `ORCHESTRATOR_OWNS_REPORT` field.
- `praktika/orchestrator/job_runner.py` — set it True in `_build_ci_environment`
  (False for local runs).
- `praktika/orchestrator/state.py` — `publish_report()` + `_ensure_report_env()`.
- `praktika/orchestrator/__init__.py` — call `publish_report()` each `_drive_dag`
  loop.
- `praktika/hook_html.py` — `push_pending_ci_report` is create-once (no
  destructive reset) on the native path; `_report_summary_exists` guard.

Deferred (see above): splitting usage aggregation out of `update_workflow_results`
so `post_run`/`configure`/Finish row-writes can be retired on the native path.
