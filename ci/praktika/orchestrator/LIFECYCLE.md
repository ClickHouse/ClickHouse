---
title: CI Engine — Lifecycle & Message/State Flow
description: End-to-end interaction between the webhook Lambda, orchestrator, runners, the two SQS queue tiers, and every S3 sync object (who reads/writes what).
sidebar_label: CI Engine Lifecycle
doc_type: reference
---

# CI Engine — lifecycle & flow {#ci-engine-lifecycle}

How the pieces talk. **SQS carries forward dispatch only** (two hops); **everything
that flows back — completion, liveness, cancel, re-run, run state — goes through
S3.** GitHub Checks is the only channel to the user.

See also [PROTOCOL.md](./PROTOCOL.md) (contracts).

## Components

| Component | Runs on | Consumes | Produces |
|---|---|---|---|
| **Lambda** (`gh-trigger`) | AWS Lambda | GitHub webhooks | workflow-trigger SQS; S3 cancel/rerun/throttle markers; GitHub gate checks |
| **Orchestrator** | EC2 ASG `praktika-workflow-orchestrator[-base]` | workflow-trigger SQS | per-runner-type SQS (`job_task`); S3 run state; GitHub checks |
| **Runner** | EC2 ASG `praktika-<runner-type>` (`…-base`, `pr-…`, `…-bedrock`) | per-runner-type SQS | S3 heartbeat/final (GitHub check transitions are owned by the orchestrator) |

Both orchestrator and runner run the **same** `praktika-controller` poller; its
`praktika_role` instance tag (`workflow_orchestrator` vs `job_runner`) picks the role.

## SQS queues (forward dispatch only)

| Queue | Producer → Consumer | Message |
|---|---|---|
| `praktika_clickhouse_workflows` (workflow-trigger) | Lambda → Orchestrator | `pull_request` / `push` trigger, or `rerun` (finished-run resume) |
| `praktika-<runner-type>` | Orchestrator → Runner | `job_task` |

There is **no** per-run SQS queue (retired). Everything else is S3.

## S3 objects — who writes, who reads

Bucket: `Settings.S3_ARTIFACT_BUCKET` (e.g. `praktika-artifacts-eu-north-1`).

| Key | Writer | Reader | Purpose |
|---|---|---|---|
| `runs/<run_id>/state.json` | Orchestrator (each loop + finalized at end) | Lambda (running-vs-finished routing), resume Orchestrator (seed), Runner (finalized guard) | DAG snapshot: per-job status/check_id/rc/rerun_count, env, `finalized` |
| `runs/<run_id>/<job>/final.json` | Runner (on job exit) | Orchestrator (`sweep_completions`) | `rc`, `environment`, `result` — job completion (replaces SQS completions) |
| `runs/<run_id>/<job>/heartbeat.json` | Runner (every `heartbeat_interval_s`) | Orchestrator (`sweep_liveness`) | `ts`, `phase`, `instance_id`, `attempt` — liveness |
| `runs/<run_id>/cancel` (kill flag) | Orchestrator (`cancel_unfinished_jobs`) | Runner (`CancelWatchdog` + pre-clone guard) | tells running runners to kill the job subprocess |
| `runs/<run_id>/cancel-request` | Lambda (UI Cancel button) | Orchestrator (`sweep_cancel`) | manual cancel of one run |
| `pr/<pr>/cancel-before-<scope>` | Lambda (`synchronize` / new push) | Orchestrator (`sweep_cancel`) | `ts` + `head_sha`; older in-scope runs self-cancel |
| `runs/<run_id>/rerun-request/<delivery>.json` | Lambda (every partial re-run) | Orchestrator (`sweep_rerun`, consume-once) | `jobs` to reset + re-run; single request log both running and resume paths drain |
| `runs/<run_id>/resume.lock` | Lambda (conditional `IfNoneMatch`, finished-run partial re-run) | resume Orchestrator (delete after `finalized=false`) | per-run boot-lease: serializes spawning one resume; no TTL (SQS redelivery recovers a crashed boot) |
| `external-pr-approvals/<repo>/pr/<n>.json` | Lambda (gate approve/store) | Lambda | fork-PR approval state |

## Normal run (push / PR)

```mermaid
sequenceDiagram
    participant GH as GitHub
    participant L as Lambda
    participant WQ as workflow SQS
    participant O as Orchestrator
    participant S3 as S3
    participant RQ as runner SQS
    participant R as Runner
    participant CK as Checks

    GH->>L: webhook pull_request or push
    Note over L: verify HMAC, refetch PR, drop if head advanced
    L->>S3: on synchronize, put pr cancel-before marker
    L->>WQ: enqueue trigger
    WQ->>O: deliver trigger
    O->>CK: open top-level check, run_id is the check id
    Note over O: clone PR head, build DAG
    O->>CK: create per-job check queued, external_id has run_id and job
    O->>RQ: send job_task with state, cancel, heartbeat, final keys and env
    O->>S3: put state.json finalized false, every loop
    RQ->>R: deliver job_task
    R->>S3: read cancel key, skip if cancelled
    R->>S3: read state.json, skip if finalized
    Note over R: clone refs pull N head, run job
    R->>S3: put heartbeat.json while running
    R->>S3: put final.json rc env result
    R->>RQ: delete job_task
    O->>S3: read heartbeat.json, sweep_liveness
    O->>S3: read final.json, sweep_completions
    O->>CK: per-job check in_progress then success or failure
    Note over O: advance DAG until all jobs terminal
    O->>S3: put state.json finalized true
    O->>CK: complete top-level check
    O->>WQ: delete trigger
```

## Cancel

```mermaid
sequenceDiagram
    participant GH as GitHub
    participant L as Lambda
    participant O as Orchestrator
    participant S3 as S3
    participant R as Runner
    participant CK as Checks

    GH->>L: UI Cancel button, or new push synchronize
    L->>S3: put cancel-request for UI cancel, or pr cancel-before for a new push
    O->>S3: sweep_cancel reads cancel-request and cancel-before
    Note over O: cancel if cancel-request present, or cancel-before newer with different sha
    O->>S3: put cancel kill flag
    R->>S3: CancelWatchdog polls the cancel kill flag
    Note over R: kill the job subprocess
    O->>CK: top-level check cancelled
```

## Partial re-run — running workflow

```mermaid
sequenceDiagram
    participant GH as GitHub
    participant L as Lambda
    participant S3 as S3
    participant O as Orchestrator
    participant RQ as runner SQS

    GH->>L: check_run rerequested on a per-job check
    Note over L: parse external_id into run_id and job
    Note over L: refetch PR, reject if head advanced, fork requires maintainer
    L->>S3: put runs rerun-request delivery.json with jobs, first
    L->>S3: read state.json, finalized false means running
    Note over L: running so nothing more to do, live orchestrator will sweep
    O->>S3: sweep_rerun lists and reads rerun-request
    O->>S3: delete each read request, consume once
    Note over O: apply_rerun resets job and failed downstream to pending, capped
    O->>S3: delete final.json and heartbeat.json so stale completion cannot finish it
    O->>RQ: re-kick job_task, then normal runner flow
    Note over O: at finalize write finalized true then one more sweep_rerun as handshake
```

## Partial re-run — finished workflow (resume)

```mermaid
sequenceDiagram
    participant GH as GitHub
    participant L as Lambda
    participant S3 as S3
    participant WQ as workflow SQS
    participant O2 as Orchestrator fresh
    participant CK as Checks

    GH->>L: check_run rerequested on a per-job check
    Note over L: parse external_id, validate head and maintainer
    L->>S3: put runs rerun-request delivery.json with jobs, first
    L->>S3: read state.json, finalized true means finished
    L->>S3: claim runs resume.lock, conditional create, must win to spawn
    L->>WQ: enqueue rerun message with run_id, rerun_jobs, PR meta
    WQ->>O2: deliver rerun message
    O2->>S3: read state.json, load snapshot
    O2->>CK: reopen the same top-level check, reuse run_id
    Note over O2: seed jobs from snapshot statuses check_ids env
    O2->>S3: clear stale cancel-request and cancel
    Note over O2: apply_rerun and sweep_rerun batch-drain all pending requests
    O2->>S3: put state.json finalized false
    O2->>S3: delete resume.lock, boot done, finalized false is now the live signal
    Note over O2: drive DAG loop, re-dispatch reset jobs, normal runner flow
    O2->>S3: put state.json finalized true
    O2->>CK: complete top-level check
```

## Liveness & recovery (S3-only, no SQS completions)

- **Runner dies mid-job** — its `job_task` isn't deleted, so SQS redelivers after
  `visibility_timeout`; a fresh runner re-runs it, writing the same
  `heartbeat`/`final` keys. `sweep_liveness` rides through via the heartbeat timers.
- **Orchestrator dies** — the workflow-trigger message redelivers; a fresh
  orchestrator re-processes and picks up any already-written `final.json` from S3.
- **Guards before a runner does work** — skip if `runs/<run_id>/cancel` exists
  (cancelled), or if `state.json.finalized` is true (no orchestrator will consume
  the result).
