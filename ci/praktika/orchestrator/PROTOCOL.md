---
title: CI Engine — Communication Protocol & Test Scenarios
description: Architecture, message formats, cancel semantics, and test checklist for the standalone CI engine.
sidebar_label: CI Engine Protocol
sidebar_position: 10
slug: /ci-engine/protocol
doc_type: reference
---

# CI Engine — Communication Protocol & Test Scenarios {#ci-engine-protocol}

## Components {#components}

| Component | Runs on | SQS / S3 |
|---|---|---|
| **Lambda** | AWS Lambda | produces → `praktika_clickhouse_workflows`; writes cancel signals to S3 |
| **Orchestrator** | EC2 ASG `praktika-workflow-orchestrator` (×2) | consumes `praktika_clickhouse_workflows`, produces → `praktika-{runner-type}`; polls S3 for job completions / cancel / liveness |
| **Job runner** | EC2 ASG `praktika-{runner-type}` (e.g. `praktika-arm-2xsmall`) | consumes `praktika-{runner-type}`; writes job completions / heartbeats to S3 |

## Queues and channels {#queue-design}

SQS carries only the *forward* dispatch, in two hops:

| Queue | Purpose |
|---|---|
| `praktika_clickhouse_workflows` | Workflow triggers (one message per PR push / rerun). |
| `praktika-{runner-type}` | Job tasks dispatched by the orchestrator to a specific runner pool. |

Everything flowing *back* to the orchestrator — job completions, liveness, and
cancel — goes through **S3**, not SQS, under `s3://<artifacts-bucket>/runs/<run_id>/`
(plus `pr/<pr>/…` for new-push cancels); see [Liveness signals](#liveness-signals).
There is **no per-run SQS queue**: an earlier design used a bidirectional
`praktika-wf-{pr}-{run_id}` queue for `job_completion` and `cancel`; it was retired
(Phase 2b) in favour of durable S3 keys, which survive an orchestrator restart the
way an in-flight SQS message never could.

## Design notes {#design-notes}

- **One S3 prefix per run, not a queue.** Each run owns `runs/<run_id>/` on S3; completions (`<job>/final.json`), liveness (`<job>/heartbeat.json`), and the kill flag (`cancel`) all live under it. Concurrent runs for the same PR (e.g. a rerun while a push is in flight) use disjoint prefixes and never contend.
- **`run_id` = top-level check run ID.** It's the suffix of the run's S3 prefix, so the Lambda can address a specific run's cancel key using only what GitHub puts in the webhook payload. No external run-id ↔ prefix mapping is needed.
- **Cancel is a durable S3 write, not queue routing.** UI Cancel writes exactly one key (`runs/<run_id>/cancel-request`); `synchronize` writes a scoped `pr/<pr>/cancel-before-<scope>` marker that older in-scope orchestrators honour on their next sweep. A freshly pushed run stamps its own `event_ts` and so is excluded by the strict `event_ts <` comparison (see [Cancel semantics](#cancel-semantics)).
- **Re-run (`check_suite` / `check_run.rerequested`) never sends a cancel.** A rerun spawns a new check run (new `run_id`, new S3 prefix). There is no previous run under that prefix to cancel into.
- **Nothing to create or tear down.** There is no per-run queue to provision on start or delete on exit; the run's S3 keys are just written as work progresses and persist afterwards as build artifacts.
- **Infra failures retry on a *fresh* orchestrator, not the same one.** The orchestrator distinguishes "couldn't run the workflow" (startup/infra, `INFRA_EXIT_CODE = 100`) from "ran it, jobs failed" (`rc = 1`) via exit code. On `100` the controller releases the workflow message (visibility → 0) and self-terminates so the ASG launches a replacement, which re-receives the redelivered message — the right cure for instance-local faults (stale runtime venv, corrupt clone, bad AMI) that an in-process retry on the same box would just hit again. Bounded by SQS `ApproximateReceiveCount` vs `PRAKTIKA_INFRA_FAILURE_MAX_RECEIVES` (default 3); past the cap the message is dropped. The orchestrator finalizes its own top-level check as `failure` on **every** attempt (so a crash never leaves the check stuck `in_progress`), and surfaces `attempt N/M`, the orchestrator instance id, and the lifecycle phase (`starting`/`ai_setup`/`planning`/`running`/`finalizing`) in the check output so retries are visible. An `rc = 1` red build is a real result and is **not** retried. (Transient blips are absorbed earlier by a small in-process startup retry, `Settings.MAX_RETRIES_ORCHESTRATOR`.)
- **A job whose runner dies mid-job is re-run by SQS redelivery, not by the orchestrator.** A runner extends its `job_task`'s visibility while it lives and deletes the message on completion; a runner that dies mid-job (crash, OOM, spot reclaim) never deletes it, so the message reappears once the visibility window lapses and a fresh runner re-runs the job against the same `heartbeat.json` / `final.json` keys. RUNNING liveness is two-stage: at `HEARTBEAT_STALL_S = 300` the runner is flagged unresponsive and the check shows a pending retry (fast, visible), and only at `HEARTBEAT_TIMEOUT_S = 900` — held well above the runner queue's `visibility_timeout` (600) so the gap budgets the redelivery wait plus a cold ASG launch and boot, the re-run's first heartbeat being written at pickup *before* checkout — is the job declared dead. So the redelivered run's first heartbeat resets `last_heartbeat_ts` before the timeout, and the per-job check rides through the recovery `in_progress`. It falls to `failure` only when redelivery stops producing heartbeats: the job is genuinely stuck, or SQS has exhausted `maxReceiveCount` (default 3) and dead-lettered it. The pickup path is not covered — an un-received `job_task` is still sitting in the queue, so there is nothing to redeliver. A re-run is not silent: the runner stamps the SQS receive count as `attempt` on every heartbeat, and when it bumps the orchestrator prints a `[RETRY]` line and updates the per-job check output to `re-running ... after the previous runner was lost (attempt N)`, so a recovered job is visible in the checks and the run log rather than looking like one uninterrupted run. The top-level workflow check also surfaces it: `md_status_summary` appends `(N retried, M runner-unresponsive)` and the per-job table carries a Notes column (`attempt N` / `runner unresponsive`).

## Limitations {#limitations}

- **Partial rerun — "re-run all checks" still restarts the whole workflow.** A single-check re-run (`check_run.rerequested`) is now partial — see [Partial re-run](#partial-rerun) — but the check-suite "re-run all / failed checks" button (`check_suite.rerequested`, which carries no per-job `external_id`) still falls back to a full-workflow re-run. Scoping it to just the previously-failed set would mean the Lambda enumerating the suite's failed checks; not done yet.

## Partial re-run {#partial-rerun}

Re-running one failed check re-runs **only that job and its failed downstream**, in place on the existing run — not the whole DAG. It is modelled as a state mutation, and works whether the run is still going or already finished.

- **Self-identifying checks.** Every per-job check carries `{run_id, job}` in its `external_id`, so a `check_run.rerequested` webhook tells the Lambda exactly which run and job to re-run — no GitHub API lookup.
- **Persisted state.** The orchestrator writes `runs/<run_id>/state.json` each loop and at finalize: per-job `{status, check_id, rc, …}`, the cumulative `environment`, and a `finalized` flag. This snapshot is both the manipulable state and the **liveness signal** — the Lambda reads `finalized` (never the GitHub API) to tell a running run from a finished one.
- **The mutation.** Reset the target job + its `FAILURE`/`CANCELLED` transitive dependents (and any terminal `always_run` downstream such as `Finish Workflow`, so merge-readiness/post-hooks re-fire) to `PENDING`, delete their `final.json` **and** `heartbeat.json`, and **drop the old per-job check** so `kick` posts a **new** check run. The normal `get_ready → kick` loop then re-drives them. Config Workflow is **not** re-run; the persisted `WORKFLOW_CONFIG`/environment is reused.
- **Stale-head guard.** Runners check out the live PR head (`refs/pull/N/head`), not the check's sha, so the Lambda refetches the PR and rejects the re-run if the head has advanced (else it would run new, possibly unapproved, code under an old check); fork-PR re-runs additionally require a maintainer.
- **Routing (running vs finished).** Every re-run first records the job under `runs/<run_id>/rerun-request/<delivery>.json` — the single request log both paths drain — **then** reads `finalized`. Writing before reading is what makes the finish-race safe (below).
  - *Running* (`finalized=false`, or no snapshot yet) → nothing more to do; the live orchestrator's `sweep_rerun()` applies the request (and consumes it) each loop. Finalize is a **handshake**: the orchestrator writes `finalized=true` **first**, then does one more `sweep_rerun()`; if it finds a request it un-finalizes and keeps driving. Because the Lambda's request write happens-before its `finalized` read, a request this sweep misses must have had its Lambda read `false` (so this live sweep owns it) or `true` (so it spawns a resume) — never stranded.
  - *Finished* (`finalized=true`) → the Lambda claims the per-run `runs/<run_id>/resume.lock` (atomic conditional create) and, only if it wins, enqueues one `type: "rerun"` message; a fresh orchestrator reopens the top-level check (reusing `run_id`), seeds from the snapshot, batch-drains **all** pending requests, writes `finalized=false`, then deletes the lock. The lock serializes the boot so concurrent clicks spawn exactly one resume (losers' requests are batch-drained by the winner). It needs no TTL — a crashed boot is recovered by SQS redelivery.
- **Only failed jobs.** GitHub only shows the re-run button on completed checks, so the target is always terminal; a still-running job is never re-run.

### Limitations {#partial-rerun-limitations}

- **Per-job re-run checks post a new check run.** GitHub does not allow un-completing a check run (PATCHing a `completed` check back to `queued`/`in_progress` is silently ignored — status/timestamps stay frozen). So a per-job re-run **posts a new check run** with the same name + `external_id`; it goes `queued → in_progress → final`, and GitHub surfaces the latest check per name (the previous same-name check collapses).
- **The top-level check still can't return to in-progress on a finished-run resume.** It's reopened via a PATCH (`retitle`), which GitHub ignores for a completed check, so the overall "CI" check keeps its prior conclusion while the resume runs. Fixing it would need a new top-level check run decoupled from `run_id` (which stays the S3 state prefix). Not done.
- **Concurrent re-runs of a finished run.** Serialized by the per-run `resume.lock` (atomic conditional create), not a throttle: many simultaneous clicks each record a request, exactly one wins the lock and spawns a resume, and that resume batch-drains every pending request. Multi-select "re-run" therefore runs all selected jobs in one resume, with no per-PR rate-limit. Residual: a click landing in the tiny window between an orchestrator publishing `finalized=true` and its recheck sweep can both re-drive on the live orchestrator **and** spawn a resume — a rare redundant run, never a lost one.
- **Repeated full re-runs are not deduplicated — don't mash the button.** The check-suite "re-run all / failed" path (below) mints a brand-new run per click and writes no cancel marker, so two rapid clicks spawn two independent full workflows for the same sha: double the compute and **duplicate per-job checks** (each run's checks carry a different `run_id` in their `external_id`, so GitHub does not collapse them). There is no throttle; a single click is expected.
- **Check-suite "re-run all / failed" is still a full re-run** (that payload carries no per-job `external_id`); scoping it to the failed set is not done.

## Run lifecycle {#run-lifecycle}

```
GitHub webhook
  → Lambda validates HMAC-SHA256 signature
  → [on synchronize] writes pr/<pr>/cancel-before-<scope> to S3 → older in-scope
    orchestrators self-cancel on their next sweep
  → enqueues workflow event to praktika_clickhouse_workflows

Orchestrator (one instance picks up the message)
  → creates top-level check run (status=in_progress) → run_id = check.id
  → clones PR head
  → run_id = check.id → S3 prefix runs/<run_id>/ (nothing to provision)
  → builds DAG from workflow config, prints execution plan
  → Loop per DAG level:
      for each ready job:
        → creates per-job check run (status=queued)
        → dispatches job_task{check_run_id, heartbeat/final/cancel S3 keys, ...}
          to praktika-{runner-type}
        → stub jobs (no matching runner pool): orchestrator drives check lifecycle
      wait() sweeps S3 once per cycle:
        sweep_cancel      → cancel-request / cancel-before → stop loop
        sweep_completions → <job>/final.json → advance DAG
        sweep_liveness    → <job>/heartbeat.json → detect dead runners
      on Config Workflow completion:
        → extracts WORKFLOW_CONFIG.filtered_jobs from returned environment
        → marks filtered jobs as SKIPPED, posts one aggregate "Skipped Jobs" check
  → completes top-level check run (neutral / failure / cancelled)
  → the run's S3 keys persist as build artifacts (nothing to tear down)
  → exit code tells the controller what kind of outcome this was:
      0   = ran OK
      1   = ran the DAG, jobs legitimately failed (a real red build)
      100 = INFRA_EXIT_CODE — could not run the workflow at all
            (bad/unavailable AI provider, plan build couldn't reach S3/GH,
            token mint, …). The DAG never started, so no jobs were dispatched.

Job runner
  → picks up job_task from praktika-{runner-type}
  → clones PR head
  → PATCHes per-job check run → in_progress
  → builds environment.json from task + carried environment (WORKFLOW_CONFIG, etc.)
  → runs Runner.run (praktika job, optionally inside Docker)
  → PATCHes per-job check run → completed (success / failure)
  → writes job_completion{rc, environment, result} to
    runs/<run_id>/<job>/final.json on S3
```

## Message formats {#message-formats}

### `job_task` (orchestrator → runner queue) {#job-task}

```json
{
  "type": "job_task",
  "repo": "ClickHouse/clickhouse-private",
  "pr_number": 55743,
  "head_sha": "abc123",
  "head_ref": "my-branch",
  "base_ref": "master",
  "sender": "maxknv",
  "title": "My PR title",
  "labels": [],
  "workflow_name": "PR",
  "job_name": "Style check",
  "runs_on": ["praktika-arm-2xsmall"],
  "cancel_s3_bucket": "praktika-artifacts-eu-north-1",
  "cancel_s3_key": "runs/72611853552/cancel",
  "heartbeat_s3_bucket": "praktika-artifacts-eu-north-1",
  "heartbeat_s3_key": "runs/72611853552/Style_check/heartbeat.json",
  "heartbeat_interval_s": 30,
  "final_state_s3_bucket": "praktika-artifacts-eu-north-1",
  "final_state_s3_key": "runs/72611853552/Style_check/final.json",
  "check_run_id": 72611853552,
  "environment": { "WORKFLOW_CONFIG": {}, "..." : "..." }
}
```

`environment` is `null` for the first job in a run (Config Workflow) and carries the
serialized `ci/tmp/environment.json` from the previous job for all subsequent jobs,
propagating `WORKFLOW_CONFIG`, `COMMIT_AUTHORS`, `JOB_KV_DATA`, etc.

`cancel_s3_*`, `heartbeat_s3_*`, and `final_state_s3_*` colocate cancel, liveness, and
completion under one S3 prefix per run — see [Liveness signals](#liveness-signals).
Phase 2b retired the per-run completions SQS queue: cancel signals now flow lambda
→ S3 (`runs/<run_id>/cancel-request` for manual cancel, `pr/<pr>/cancel-before-<scope>` for
new-push fan-out) and the orchestrator polls them in `sweep_cancel`.

### `job_completion` (runner → `s3://.../runs/<run_id>/<job>/final.json`) {#job-completion}

```json
{
  "type": "job_completion",
  "job_name": "Style check",
  "rc": 0,
  "ts": 1704067200.123,
  "repo": "ClickHouse/clickhouse-private",
  "pr_number": 55743,
  "head_sha": "abc123",
  "workflow_name": "PR",
  "instance_id": "i-0abc...",
  "details_url": "https://.../praktika.html?...",
  "environment": { "WORKFLOW_CONFIG": {}, "..." : "..." },
  "result": { "name": "Style check", "status": "OK", "results": [], "..." : "..." }
}
```

Written by `orchestrator/job_runner.py` after `Runner.run` returns. It ships the
job's raw `Result` (serialized via `Result.to_dict`) in `result`; the orchestrator
reconstructs it in `sweep_completions`, renders the per-job check output
(`state._build_check_output`), and stashes the raw Result on the `JobState`
(`js.result`) for AI observation. Read once per `wait()` cycle. Idempotent:
`JobState.finish` is a no-op once the job has already moved out of RUNNING, so a
final.json that arrives after `sweep_liveness` already declared the job dead is
harmless.

### Cancel signals (Lambda → S3) {#cancel}

Lambda writes one of two S3 keys depending on what triggered the cancel; the
orchestrator polls both in `sweep_cancel` once per `wait()` cycle.

| Trigger | S3 key | Body |
|---|---|---|
| Manual UI Cancel button (`check_run.requested_action`) | `runs/<run_id>/cancel-request` | `requested` (presence-only) |
| New push to PR (`pull_request.synchronize`) | `pr/<pr>/cancel-before-<scope>` | `{"ts": <event_ts>}` |

The new-push channel uses event timestamp validation: each workflow trigger event
the lambda enqueues carries `event_ts` (the lambda's receive time). On
`synchronize`, the lambda writes a queue-scoped `cancel-before` marker with the
same `event_ts` it stamps on the new run. Older orchestrators in the same scope
see `cancel-before > event_ts` and self-cancel; the freshly enqueued run sees
`cancel-before == event_ts` and stays alive (strict less-than comparison).

## Liveness signals {#liveness-signals}

S3 channels under `s3://<artifacts-bucket>/`:

| Channel | Direction | Path | Purpose |
|---|---|---|---|
| Cancel request | Lambda → orchestrator | `runs/<run_id>/cancel-request` | Manual UI Cancel button — orchestrator's `sweep_cancel` sets `state.cancelled` |
| Cancel-before | Lambda → orchestrators | `pr/<pr>/cancel-before-<scope>` (`{ts}`) | New-push fan-out inside one orchestrator scope — every run with `event_ts < ts` self-cancels |
| Kill flag | Orchestrator → runners | `runs/<run_id>/cancel` | Once written, every running job in the run kills its subprocess |
| Heartbeat | Runner → orchestrator | `runs/<run_id>/<normalized-job>/heartbeat.json` | Periodic `{ts, status, instance_id, phase, attempt}` proves the runner is alive; `attempt` is the SQS receive count, so a bump marks a re-run |
| Final state | Runner → orchestrator | `runs/<run_id>/<normalized-job>/final.json` | `{rc, environment, ...}` on job exit |

**Cancel request / cancel-before** — see [Cancel semantics](#cancel-semantics).

**Kill flag** — written by `WorkflowState.cancel_unfinished_jobs` once
`state.cancelled` is set (and only when there are RUNNING non-always_run jobs,
so a cancel that arrives while only `Finish Workflow` is RUNNING does not kill
it). Each runner has a `CancelWatchdog` thread polling the key every 10 s and
killing the job subprocess on first hit.

**Heartbeat** — written by the runner-side `Heartbeat` thread every
`heartbeat_interval_s` (default 30 s). The orchestrator runs
`WorkflowState.sweep_liveness` once per `wait()` cycle and marks in-flight jobs
dead under two separate timeout rules:

- **Runner pickup timeout** (default 3600 s): job is still `QUEUED`, no
  heartbeat ever observed, and `now - kicked_at > RUNNER_PICKUP_TIMEOUT_S` →
  covers queue/ASG/boot delays or a pool that never picks the job up.
- **Heartbeat stall** (default 300 s): job is `RUNNING` and
  `now - last_heartbeat_ts > HEARTBEAT_STALL_S` → runner flagged unresponsive.
  The check flips to `RUNNING (runner unresponsive)` / `awaiting automatic
  retry` and a `[STALE]` line is logged, but the job is **not** failed; a fresh
  heartbeat clears the flag.
- **Heartbeat timeout** (default 900 s): job is `RUNNING` and
  `now - last_heartbeat_ts > HEARTBEAT_TIMEOUT_S` → runner declared dead.

A mid-job runner death is recovered by SQS, not by the orchestrator: the dead
runner's `job_task` reappears once the runner queue's `visibility_timeout`
(600 s) lapses, and a fresh runner re-runs the job, writing to the same
`heartbeat.json` key. The heartbeat timeout sits above that visibility timeout
plus a re-run's startup cost, so the redelivered run's first heartbeat resets
`last_heartbeat_ts` and the check stays `in_progress` across the recovery — this
is why the two values are tuned together (see [Design notes](#design-notes)).
The heartbeat path completes the per-job check as `failure` only once
redelivery stops producing heartbeats (stuck job, or `maxReceiveCount`
exhausted); the pickup path fails as soon as its grace elapses. Either failure
advances the DAG so downstream jobs cascade-cancel and `Finish Workflow`
(always_run) still fires.

**Final state** — written by `orchestrator/job_runner.py` after `Runner.run`
returns. The runner includes `rc`, `environment`, and optional check
`output`/`details_url`; the orchestrator owns the GitHub Checks lifecycle and
completes the per-job check from this payload. `WorkflowState.sweep_completions`
polls the key every `wait()` cycle and calls `JobState.finish` on hit. Because
`final.json` is durable on S3, an orchestrator that died after dispatch picks
the result up on restart — no in-flight messages get lost the way an SQS
`job_completion` would.

## Cancel semantics {#cancel-semantics}

| Trigger | Target | How it reaches the orchestrator |
|---|---|---|
| New push (`synchronize`) | Every in-flight run for the PR in the same orchestrator scope with `event_ts < new event_ts` | Lambda writes `pr/<pr>/cancel-before-<scope>` with `{ts}`; older orchestrators in that scope self-cancel via `sweep_cancel` |
| Manual Cancel button | Exactly one run | Lambda writes `runs/<run_id>/cancel-request`; that orchestrator's `sweep_cancel` picks it up |
| Re-run (`rerequested`) | — | No cancel written (new run has a new run_id and a new S3 prefix) |

S3 is durable, so cancel signals survive an orchestrator restart — a previously
running orchestrator that comes back picks the flag up on its next sweep.

## Use cases to test {#use-cases}

| # | Action | Expected result |
|---|---|---|
| 1 | Push a new commit while CI is running | Old run cancels (top-level check = `cancelled`); new run starts |
| 2 | Push two commits in quick succession | Both old runs cancel; only the latest SHA runs to completion |
| 3 | Click Cancel button on the `PR` check | That specific run cancels; no new run started |
| 4 | Click Cancel on a run that already finished | Lambda writes `runs/<run_id>/cancel-request` to S3; the run's orchestrator is already gone so nothing consumes it; no effect (the key expires with the run's artifacts) |
| 5 | Click Re-run all checks | Full workflow restarts for the same SHA; no self-cancel |
| 6 | Click Re-run on a specific failed check | Full workflow restarts; new run on the same SHA with a fresh queue |
| 7 | Two re-runs in quick succession | Each run uses its own queue; no cross-run traffic |
| 8 | Config Workflow succeeds with filtered jobs | `Skipped Jobs` check posted with Markdown breakdown grouped by reason |
| 9 | Config Workflow fails | All downstream jobs skipped; top-level check = `failure` |
| 10 | Style check runs inside Docker | `docker run` succeeds; per-job check flips `queued` → `in_progress` → `success/failure` |
| 11 | Runner instance is terminated mid-job | Visibility timeout expires; runner re-queues task; another runner picks it up |
| 12 | Orchestrator instance is terminated mid-run | SQS visibility timeout on the workflow-trigger message expires; another orchestrator re-processes the workflow event and picks up any already-written `final.json` from S3. |
| 13 | Orchestrator startup/infra failure (e.g. bad AI provider, plan build can't reach S3) | Orchestrator exits `100`, finalizes the top-level check as `failure` with the phase + `attempt N/M`; controller releases the message and self-terminates; a fresh orchestrator retries. After `PRAKTIKA_INFRA_FAILURE_MAX_RECEIVES` attempts the message is dropped (last attempt's check shows the failure). A plain red build (`rc = 1`) is **not** retried. |
