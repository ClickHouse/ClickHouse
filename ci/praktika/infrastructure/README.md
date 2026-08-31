# praktika.infrastructure

Declarative AWS infrastructure for praktika CI: VPCs, S3, runner / orchestrator
pools, and the HTML report page — all defined in `ci/infrastructure/projects.py` and
brought up with one command.

## Typical commands

```bash
# Deploy (or update) all components defined in ci/infrastructure/projects.py
python3 -m praktika infrastructure --deploy

# Deploy only specific component types
python3 -m praktika infrastructure --deploy --only ImageBuilder LaunchTemplate
python3 -m praktika infrastructure --deploy --only AutoScalingGroup

# Roll EC2 instances on every ASG (replace with the latest launch template version)
python3 -m praktika infrastructure --restart-instances

# Destroy project-prefixed execution-plane resources while keeping S3, VPC,
# CIDB, Dedicated Hosts, EC2 instances, and the GitHub webhook wiring intact.
# Destroy commands require --project to match a name in ci/infrastructure/projects.py.
python3 -m praktika infrastructure --destroy-runtime --project praktika

# Destroy every project-prefixed managed resource, including stateful resources.
python3 -m praktika infrastructure --destroy-all --project praktika
```

## Config components (used in `ci/infrastructure/projects.py`)

- **`CloudInfrastructure.Config`** — top-level container; aggregates all
  components below into a single deployable unit. Set
  `min_praktika_version` when the config uses infrastructure features that
  require a newer Praktika runtime.
- **`VPC.Config`** — a VPC + subnets in declared availability zones. Runner
  and orchestrator pools attach to a VPC by name.
- **`Storage.Config`** — an S3 bucket for artifacts and the HTML report,
  with retention policy and public/private access.
- **`Components.OrchestratorPool`** — ASG of EC2 VMs that polls SQS,
  resolves workflow DAGs, and dispatches jobs to runner pools. Supports
  `Scaling.Disabled` and `Scaling.Auto`.
- **`Components.RunnerPool`** — ASG of EC2 VMs that pull job tasks
  from per-pool SQS queues and execute them. Pools are referenced by jobs
  via the `runs_on` label and support `Scaling.Disabled` / `Scaling.Auto`.
- **Implicit pool autoscaler** — when any pool uses `Scaling.Auto`,
  `CloudInfrastructure.Config` synthesizes a scheduled Lambda that watches
  the corresponding SQS queues and scales ASG desired capacity up. Idle
  runner/orchestrator instances then scale themselves back in by
  decrementing ASG desired capacity and terminating the instance. Set
  `capacity_reserve=N` on an auto-scaled pool to keep `N` extra idle
  instances above queue demand, capped by `max_size`.
- **`Components.report_page_config`** — the static HTML page +
  bucket policy that renders a workflow's `result_*.json` files.
- **`ImageBuilder.Config`** — AMI build pipelines. Supports ordinary
  Image Builder components plus `prebuilt_venvs`, which bake named Python
  virtualenvs under `/opt/praktika/base-venvs/<name>` for later selection
  via `Settings.PRAKTIKA_BASE_VENV`. Use `ami_launch_permission`, for
  example `{"userGroups": ["all"]}`, to publish built AMIs publicly.

## TODO

- Pools of dedicated VMs / bare metal (e.g. EC2 Dedicated Hosts, baremetal
  instance types) as a first-class `RunnerPool` mode
- CI DB (ClickHouse) deployment as a managed component
- Private-access gateway (VPN / bastion) deployment for reaching the report
  page and CI DB on private endpoints, and optionally for SSH into runners
