# CI Engine

Standalone CI engine that replaces GitHub Actions scheduling with direct
webhook-driven workflow orchestration + SQS-based job dispatch to pools of
long-lived EC2 runners.

AI advisor design and Bedrock operational notes live in
`praktika/orchestrator/ai/DESIGN.md`.

## Architecture

```
GitHub webhook
    |
    v  (HMAC-verified PR event)
Lambda (ci/praktika/infrastructure/native/lambda_ci_engine.py)
    |
    v  (enqueues {type, repo, pr_number, head_sha, ...})
SQS praktika_clickhouse_workflows
    |
    v
Orchestrator ASG (praktika-workflow-orchestrator)
    user_data -> systemctl enable --now praktika-controller
        |-- clone the PR head
        |-- install Praktika into the shared runtime venv if it is not already there
        |-- subprocess: praktika orchestrate workflow event.json --ci
        |       |-- open per-workflow GitHub check run (`PR`, in_progress)
        |       |-- find_workflow_for_event
        |       |-- build_job_dag
        |       |-- WorkflowState execution loop:
        |       |     for each JobState kicked:
        |       |       open per-job GitHub check run (queued -> in_progress)
        |       |       send {type: "job_task", ...} to SQS queue praktika-<runs_on>
        |       |-- close per-workflow check
    |
    v  (SQS: one queue per runner pool, named praktika-<runs_on>)
Runner ASGs (e.g. praktika-arm-2xsmall)
    user_data -> systemctl enable --now praktika-controller
        |-- clone the PR head
        |-- install Praktika into the shared runtime venv if it is not already there
        |-- subprocess: praktika orchestrate job task.json --ci
        |       |-- job_runner.run_job(task) -> Runner().run(...)
```

### Interaction diagram

```mermaid
flowchart TD
    GH[GitHub PR event] --> L[Lambda webhook handler]
    L --> WQ[SQS: praktika-workflows]

    WQ --> WB[praktika-controller on orchestrator ASG]
    WB --> WC[Clone PR head]
    WC --> WV[Resolve Praktika runtime]
    WV --> WO[praktika orchestrate workflow event.json --ci]

    WO --> C0[Open workflow check run]
    WO --> DAG[Build workflow DAG]

    DAG --> J1Q[SQS: praktika-arm-2xsmall]
    DAG --> J2Q[SQS: praktika-amd-2xsmall]

    J1Q --> R1[praktika-controller on arm runner ASG]
    J2Q --> R2[praktika-controller on amd runner ASG]

    R1 --> RC1[Clone PR head]
    R2 --> RC2[Clone PR head]

    RC1 --> RV1[Resolve Praktika runtime]
    RC2 --> RV2[Resolve Praktika runtime]

    RV1 --> JO1[praktika orchestrate job task.json --ci]
    RV2 --> JO2[praktika orchestrate job task.json --ci]

    JO1 --> JR1[Job result + artifacts]
    JO2 --> JR2[Job result + artifacts]

    JR1 --> S3[S3 artifacts / logs / workflow state]
    JR2 --> S3
    WO --> S3

    JR1 --> C1[Update per-job check]
    JR2 --> C2[Update per-job check]
    WO --> C3[Close workflow check]

    C0 --> GHCS[GitHub checks / statuses]
    C1 --> GHCS
    C2 --> GHCS
    C3 --> GHCS
```

## Code split (intentional)

The orchestrator and runner are each composed of two pieces — one stable
controller bootstrap package that's installed on the EC2 image and enabled by
the launch-time user_data, and one orchestrator
module that ships with each PR:

|                   | Installed on the image (needs LT+ASG redeploy or wheel refresh to change) | Ships with each PR (plain `git push`) |
|-------------------|--------------------------------------------------------|---------------------------------------|
| **Workflow side** | `praktika-controller` — SQS poll, clone, GH App token, cached venv reuse, S3 log | `__init__.py::orchestrate`, `state.py` (`WorkflowState`, `JobState`, `JobCheckRun`) |
| **Job side**      | `praktika-controller` — SQS poll, clone, GH App token, cached venv reuse, S3 log | `job_runner.py::run_job` (maps task -> `praktika.Runner.run`) |

When you want to tweak workflow orchestration or job-execution policy, you
only need `git push`. Only the stable bootstrap layer requires an LT/ASG redeploy
or bootstrap wheel refresh.

## Runtime resolution

Praktika runtime selection is split between three layers:

| Layer | Component | What it owns |
|---|---|---|
| **Image bake** | `ImageBuilder.Config.prebuilt_venvs` in `ci/infrastructure/projects.py` | Creates named base venvs under `/opt/praktika/base-venvs/<name>` and bakes the shared base Praktika wheel |
| **Repo settings** | `ci/settings/settings.py` | Selects the shared base venv |
| **Launch-time user data** | runner/orchestrator pool config in `ci/infrastructure/projects.py` | Optionally force-reinstalls the current Praktika wheel into the shared base venv before starting the controller |
| **Bootstrap** | `praktika-controller` / `praktika_controller` | Resolves the base venv and runs Praktika from it |

Current settings:

| Setting | Used by | Meaning |
|---|---|---|
| `PRAKTIKA_BASE_VENV` | both sides | Shared base venv name for workflow and job dispatches |

Current repo policy in `ci/settings/settings.py`:

- Both sides select base venv `praktika-runtime`
- Base pools bake Praktika `0.1` into that env
- Non-base pools force-reinstall Praktika `0.1.1` into that same env at boot before starting the controller

This means:

- the image provides stable Python/tooling dependencies
- base pools test a published Praktika release baked into the image
- non-base pools update the shared base venv to the current Praktika wheel at boot

### Resolution flow

```mermaid
flowchart TD
    A[GitHub event / SQS message] --> B[praktika-controller]
    B --> C[Clone repo at PR head]
    C --> D[Read ci/settings/settings.py]
    D --> E[Read PRAKTIKA_BASE_VENV]
    E --> F[Resolve /opt/praktika/base-venvs/<name>]
    F --> G[Optional: user data force-reinstalls current Praktika wheel into that venv]
    G --> H{Base imports praktika?}
    H -->|yes| I[python -m praktika ...]
    H -->|no| J[Fail: selected base env must contain Praktika]
```

### Venv layout

| Path | Created by | Purpose |
|---|---|---|
| `/opt/praktika/base-venvs/praktika-runtime` | Image Builder | Shared workflow/job Python base with runtime deps, `pytest`, and the published base Praktika wheel |

### Which component changes what

- Change `ci/infrastructure/projects.py` when you want a different prebaked shared base venv, a different image-level tooling set, or a different launch-time Praktika override for non-base pools.
- Change `ci/settings/settings.py` when you want to select a different shared base venv.
- Change `bootstrap/src/praktika_controller/venv_manager.py` only when base-venv resolution itself needs to change.

## Debugging runner logs

`fetch_job_log.sh` fetches the full `ci-runner` systemd journal from a live
runner via SSM and extracts the log for a specific job run. It uploads the
journal to S3 first to bypass the 48 KB SSM output limit.

```bash
# List all job names seen in the current runner's journal
./ci/praktika/orchestrator/fetch_job_log.sh --list

# Fetch the most recent run of a specific job
./ci/praktika/orchestrator/fetch_job_log.sh -j "Config Workflow"

# Fetch the second-most-recent run
./ci/praktika/orchestrator/fetch_job_log.sh -j "Config Workflow" -n 2

# Target a specific runner instance
./ci/praktika/orchestrator/fetch_job_log.sh -j "Config Workflow" -i i-0abc123

# Save to a file
./ci/praktika/orchestrator/fetch_job_log.sh -j "Config Workflow" > /tmp/cw.log
```

Auto-detects the running `praktika-arm-2xsmall` runner if no `-i` is given.
Requires AWS SSM access and S3 write permission to `clickhouse-test-reports-private`.

## Local testing

### Workflow side (no AWS required)

```bash
praktika orchestrate workflow            # auto-builds the event from git state
praktika orchestrate workflow event.json # or load a pre-built event
praktika orchestrate workflow --name "PR Fast" # run one matching workflow
```

Without `--ci` this runs in local-orchestrator mode: every job is dispatched as a
synchronous subprocess (`praktika orchestrate job task.json`) against the
local-fs S3 backend. Useful for end-to-end smoke tests on your machine.

### Job side (single-job sandbox)

```bash
praktika orchestrate job task.json
```

Invokes `praktika.Runner.run` with `local_orchestrator_run=True` for the job
named in the task.

Task JSON shape (what the orchestrator sends over SQS):

```json
{
  "type": "job_task",
  "repo": "ClickHouse/clickhouse-private",
  "pr_number": 55743,
  "head_sha": "...",
  "head_ref": "ci-engine",
  "workflow_name": "PR",
  "job_name": "Style check",
  "runs_on": ["self-hosted", "praktika-arm-2xsmall"]
}
```

## Naming conventions

Adding a new runner type means one call to `_runner_infra(name, instance_type)`
in `ci/infrastructure/projects.py`. Names derive from a single base:

| Resource | Name |
|----------|------|
| ASG      | `praktika-{name}` |
| LT       | `praktika-{name}-lt` |
| SQS queue | `praktika-{name}` |

A job's `runs_on=[X]` routes to queue `praktika-X`. There's no fallback —
if the queue doesn't exist, the dispatch fails and the job is marked
FAILURE.

## Deploy

```bash
# Workflow runtime infra (changes to praktika-controller or the baked orchestrator image):
python3 -m praktika infrastructure --deploy --only LaunchTemplate AutoScalingGroup
# Plus terminate running orchestrator instances so the ASG relaunches on the new LT.

# Job runtime infra (new runner types, or changes to praktika-controller or the baked runner image):
python3 -m praktika infrastructure --deploy --only LaunchTemplate AutoScalingGroup SQSQueue
# Plus terminate running runners on the affected pool.
```

The Praktika ASG deploy resolves `launch_template_version="$Latest"` to a
concrete version number at deploy time, so an LT bump also needs an ASG
redeploy. `$Latest` is not honored at runtime.

## What works

- Webhook -> Lambda -> SQS -> orchestrator pickup -> clone -> orchestrate
- Per-workflow `PR` GitHub check run with the full execution plan as output,
  the orchestrator instance id, lifecycle phase, and `attempt N/M`; always
  finalized (never left `in_progress`) even on a startup crash
- Infra-failure handling: a startup/infra crash exits `INFRA_EXIT_CODE` and the
  controller retries on a fresh orchestrator instance, bounded by SQS receive
  count — a real red build (`rc=1`) is not retried (see `PROTOCOL.md`)
- Per-job GitHub check runs (`queued` at plan time, `in_progress` on kick,
  `success`/`skipped` on completion)
- DAG-aware execution loop (`WorkflowState.get_ready` / `kick` / `wait`)
- SQS dispatch from `JobState.kick()` to per-type runner queues
- Job runner infra helper (`_runner_infra`), one deployed pool: `praktika-arm-2xsmall`
- Local sandbox: `praktika orchestrate job <task.json>` runs a real job
  end-to-end through `praktika.Runner.run` against the local-fs S3 backend

## TODO

- Completion path: runner posts a "done" event back, orchestrator's `wait()`
  long-polls it and flips `JobState` to SUCCESS/FAILURE. Today the orchestrator
  stubs a job SUCCESS the instant it's dispatched.
- Run `Config Workflow` to filter jobs (file-change / cache-hit).
- Artifact flow via `s3://clickhouse-builds/PRs/<pr>/<workflow>/<job>/...`.
- Workspace cleanup between jobs on long-lived runners (`git clean -ffdx`,
  cache reset).
- Orphan runner sweeper Lambda (mandatory tags + periodic termination).
- Self-termination on job completion (EXIT trap in `Runner.run`).
- Per-runner-type scaling (queue depth -> ASG target tracking) — currently all
  pools are fixed size 1.
