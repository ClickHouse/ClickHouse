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

So instead of gating the writers, we **move the one destructive operation** —
the `version=0` create/reset — out of the Config job and onto the orchestrator:

- `hook_html.push_pending_ci_report` → on the native path (`ORCHESTRATOR_OWNS_REPORT`)
  it is a **no-op**. The orchestrator, not the Config job, creates the summary.
  GitHub Actions keeps the `version=0` create here (no orchestrator to do it).
- `state.create_initial_report` (called once by `_orchestrate_single` at
  fresh-run start, inside the startup-retry block) is the **sole** `version=0`
  writer on the native path. A resume (`_orchestrate_resume`) never calls it, so
  a re-run keeps the finished run's rows. Because the single owner does the
  create at its own run boundary, there is **no create-once guard and no run_id
  in the report**: a fresh run resets with a new `start_time`, a resume doesn't
  touch it — which also closes the same-sha report-reuse hazard (a new
  orchestrator at the same PR/sha no longer inherits the previous run's stale
  `start_time`/`duration`).
- `hook_html.configure` → **no-op on the native path** (`ORCHESTRATOR_OWNS_REPORT`).
  The orchestrator now authors the cached/filtered SKIPPED rows itself (see
  increment 4 below), so the Config job no longer writes them — one fewer
  concurrent summary writer. On GitHub Actions (no orchestrator) it is unchanged.
- `pre_run` / `post_run` → **unchanged** (rows + usage + the per-job
  `result_<job>.json` keep flowing; CIDB untouched).
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
   rows render identically. Jobs the orchestrator knows finished are re-asserted
   from their `final.json`; **cached/filtered SKIPPED jobs (which never run and
   have no `final.json`) get their rows authored here too** (increment 4).
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

### Increment 4 (shipped) — orchestrator authors the SKIPPED rows

`hook_html.configure` (run inside the Config job) used to write the cached/
filtered SKIPPED rows into the summary, making the runner a concurrent writer
racing the orchestrator's per-loop re-assert. Worse, its
`assert update_workflow_results(...) is None` guard tripped against the
orchestrator-seeded summary: the orchestrator seeds the summary `RUNNING` with
every job (incl. Config) `PENDING` (`config_job_running=False`), so the first
`update_sub_result` recomputed the status `RUNNING → PENDING` and the guard
raised. `configure` now **no-ops on the native path**; instead:

- `WorkflowState.apply_workflow_config` already marks `filtered_jobs` +
  `cache_success` jobs SKIPPED in the DAG. It now also stashes the cache-hit
  report link on the `JobState` (`skip_details_url`, persisted in the snapshot
  so it survives a resume).
- `publish_report` builds a SKIPPED `Result` row for each such job (cache hit →
  its reused-report link + "reused from cache"; filtered → its reason as info)
  and writes them in the same `update_workflow_results` call as the terminal
  rows. SKIPPED rows carry **no usage** (the job never ran) so they are built
  separately and kept out of the usage aggregation.

One fewer runner-side summary writer. See BACKLOG.md "sole summary writer".

### Deferred (follow-up, not correctness)

- **Retire the remaining runner row-writes (`pre_run` / `post_run`).** With
  `configure` done (increment 4), these two are the last runner writers of the
  summary. Retiring them needs the orchestrator to own per-job report messages,
  DROPPED-dependee rows, and the top-level ext — after which the version CAS can
  be dropped (see BACKLOG.md). Larger change, no correctness benefit over the
  current state.
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
- `praktika/orchestrator/state.py` — `publish_report()` + `_ensure_report_env()`;
  `apply_workflow_config`/`JobState.skip` stash `skip_details_url`; `publish_report`
  authors the SKIPPED rows (increment 4).
- `praktika/orchestrator/__init__.py` — call `publish_report()` each `_drive_dag`
  loop.
- `praktika/hook_html.py` — `push_pending_ci_report` and `configure` no-op on the
  native path; the shared skeleton builder (`_build_pending_summary` +
  `_write_summary_to_s3`) is driven by `create_initial_report` (orchestrator) and
  the GHA Config path.
- `praktika/orchestrator/state.py` — `create_initial_report()` is the sole
  native-path `version=0` writer (called from `_orchestrate_single`).

Deferred (see above): retiring `pre_run`/`post_run` row-writes on the native path,
after which the version CAS can be dropped.
