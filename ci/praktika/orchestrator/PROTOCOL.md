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

| Component | Runs on | SQS queues |
|---|---|---|
| **Lambda** | AWS Lambda | produces → `praktika_clickhouse_workflows`, `praktika-wf-{pr}-{run_id}` |
| **Orchestrator** | EC2 ASG `praktika-workflow-orchestrator` (×2) | consumes `praktika_clickhouse_workflows`, produces → `praktika-{runner-type}`, owns ↔ `praktika-wf-{pr}-{run_id}` |
| **Job runner** | EC2 ASG `praktika-{runner-type}` (e.g. `praktika-arm-2xsmall`) | consumes `praktika-{runner-type}`, produces → `praktika-wf-{pr}-{run_id}` |

## Queue design {#queue-design}

| Queue | Purpose |
|---|---|
| `praktika_clickhouse_workflows` | Workflow triggers (one message per PR push / rerun). |
| `praktika-{runner-type}` | Job tasks dispatched by the orchestrator to a specific runner pool. |
| `praktika-wf-{pr}-{run_id}` | **Per-run** bidirectional queue owned by one orchestrator. Carries `job_completion` (from runners) and `cancel` (from Lambda). Created when the orchestrator starts, deleted when it finishes. `run_id` is the top-level GitHub check run ID. |

## Design notes {#design-notes}

- **One queue per run, not per PR.** Every message on the queue is addressed to exactly one orchestrator, so `wait` needs no filtering — `cancel` means cancel, `job_completion` means advance the DAG. Concurrent runs for the same PR (e.g. a rerun while a push is in flight) use disjoint queues and never contend.
- **`run_id` = top-level check run ID.** Encoding it in the queue name lets the Lambda address a specific run by name, using only the information GitHub puts in the webhook payload. No external run-id ↔ queue mapping is needed.
- **Cancel dispatch is routing, not filtering.** UI Cancel hits exactly one queue (`praktika-wf-{pr}-{check_id}`); `synchronize` fans out via `list_queues(QueueNamePrefix="praktika-wf-{pr}-")`. A freshly pushed run hasn't created its queue yet, so the fan-out naturally excludes it.
- **Re-run (`check_suite` / `check_run.rerequested`) never sends a cancel.** A rerun spawns a new check run (new `run_id`, new queue). There is no previous run for that same queue to cancel into.
- **Orchestrator owns the queue.** `WorkflowState.__init__` creates it; `WorkflowState.cleanup` (in an `orchestrate` `finally`) deletes it on every exit path — normal, cancelled, or errored.
- **Infra failures retry on a *fresh* orchestrator, not the same one.** The orchestrator distinguishes "couldn't run the workflow" (startup/infra, `INFRA_EXIT_CODE = 100`) from "ran it, jobs failed" (`rc = 1`) via exit code. On `100` the controller releases the workflow message (visibility → 0) and self-terminates so the ASG launches a replacement, which re-receives the redelivered message — the right cure for instance-local faults (stale runtime venv, corrupt clone, bad AMI) that an in-process retry on the same box would just hit again. Bounded by SQS `ApproximateReceiveCount` vs `PRAKTIKA_INFRA_FAILURE_MAX_RECEIVES` (default 3); past the cap the message is dropped. The orchestrator finalizes its own top-level check as `failure` on **every** attempt (so a crash never leaves the check stuck `in_progress`), and surfaces `attempt N/M`, the orchestrator instance id, and the lifecycle phase (`starting`/`ai_setup`/`planning`/`running`/`finalizing`) in the check output so retries are visible. An `rc = 1` red build is a real result and is **not** retried. (Transient blips are absorbed earlier by a small in-process startup retry, `Settings.MAX_RETRIES_ORCHESTRATOR`.)

## Limitations {#limitations}

- **Orphan queues on hard kills.** If an orchestrator crashes or its EC2 instance is terminated between queue creation and `cleanup`, the queue is leaked. There is no periodic sweeper yet: orphan queues sit empty (messages expire after `MessageRetentionPeriod = 1 h`) and cost nothing, but they accumulate in the SQS console over time. A scheduled sweeper (`list_queues` with `praktika-wf-*` prefix + delete by `LastModifiedTimestamp`) is a viable follow-up; the protocol does not depend on one.
- **Cancel dropped in the startup window.** Between `CheckRun.start` (run_id exists in GitHub) and `WorkflowState.__init__` (queue exists in SQS) there is a sub-second window where a UI-triggered cancel hits `NonExistentQueue`. Too narrow to be triggerable in practice.
- **Partial rerun — "Re-run failed checks" restarts the whole workflow.** `check_run.rerequested` and `check_suite.rerequested` currently enqueue a plain `pull_request` trigger and the orchestrator runs every job in the DAG. Intended design: the Lambda captures the rerun job set — the single check's name on `check_run.rerequested`, or the names of every failed/cancelled check from the previous attempt on `check_suite.rerequested` — and passes it in the workflow message as `rerun_jobs`; the orchestrator reruns those jobs plus everything transitively downstream of them (since the target's artifact may change), and marks all other jobs `SKIPPED` (their artifacts are already in S3). Config Workflow always runs regardless so `WORKFLOW_CONFIG` is refreshed.

## Run lifecycle {#run-lifecycle}

```
GitHub webhook
  → Lambda validates HMAC-SHA256 signature
  → [on synchronize] list_queues(prefix=praktika-wf-{pr}-) → fan out cancel to every live run
  → enqueues workflow event to praktika_clickhouse_workflows

Orchestrator (one instance picks up the message)
  → creates top-level check run (status=in_progress) → run_id = check.id
  → clones PR head
  → creates praktika-wf-{pr}-{run_id}
  → builds DAG from workflow config, prints execution plan
  → Loop per DAG level:
      for each ready job:
        → creates per-job check run (status=queued)
        → dispatches job_task{check_run_id, completions_queue_url, ...}
          to praktika-{runner-type}
        → stub jobs (no matching runner pool): orchestrator drives check lifecycle
      wait() long-polls praktika-wf-{pr}-{run_id}:
        cancel         → stop loop
        job_completion → advance DAG
      on Config Workflow completion:
        → extracts WORKFLOW_CONFIG.filtered_jobs from returned environment
        → marks filtered jobs as SKIPPED, posts one aggregate "Skipped Jobs" check
  → completes top-level check run (neutral / failure / cancelled)
  → finally: delete_queue(praktika-wf-{pr}-{run_id})
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
  → sends job_completion{rc, environment} to completions_queue_url
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
| Heartbeat | Runner → orchestrator | `runs/<run_id>/<normalized-job>/heartbeat.json` | Periodic `{ts, status}` proves the runner is alive |
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
- **Heartbeat timeout** (default 300 s): job is `RUNNING` and
  `now - last_heartbeat_ts > HEARTBEAT_TIMEOUT_S` → runner died mid-job.

Either path completes the per-job check as `failure` and advances the DAG so
downstream jobs cascade-cancel and `Finish Workflow` (always_run) still fires.

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
| 4 | Click Cancel on a run that already finished | Lambda sees `NonExistentQueue` (queue was deleted on exit) and logs `[skip]`; no effect |
| 5 | Click Re-run all checks | Full workflow restarts for the same SHA; no self-cancel |
| 6 | Click Re-run on a specific failed check | Full workflow restarts; new run on the same SHA with a fresh queue |
| 7 | Two re-runs in quick succession | Each run uses its own queue; no cross-run traffic |
| 8 | Config Workflow succeeds with filtered jobs | `Skipped Jobs` check posted with Markdown breakdown grouped by reason |
| 9 | Config Workflow fails | All downstream jobs skipped; top-level check = `failure` |
| 10 | Style check runs inside Docker | `docker run` succeeds; per-job check flips `queued` → `in_progress` → `success/failure` |
| 11 | Runner instance is terminated mid-job | Visibility timeout expires; runner re-queues task; another runner picks it up |
| 12 | Orchestrator instance is terminated mid-run | SQS visibility timeout expires; other orchestrator re-processes workflow event. The old run's queue is leaked (see Limitations). |
| 13 | Orchestrator startup/infra failure (e.g. bad AI provider, plan build can't reach S3) | Orchestrator exits `100`, finalizes the top-level check as `failure` with the phase + `attempt N/M`; controller releases the message and self-terminates; a fresh orchestrator retries. After `PRAKTIKA_INFRA_FAILURE_MAX_RECEIVES` attempts the message is dropped (last attempt's check shows the failure). A plain red build (`rc = 1`) is **not** retried. |
