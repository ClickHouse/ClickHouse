# Host metrics

Praktika samples whole-VM resource usage while each job runs and stores it in the
job's `Result.ext["metrics"]`, renders it in `json.html`, labels over/under-utilized
runners, and accumulates a pipeline-wide KPI into the workflow `Result`.

Collection is dependency-free (`/proc/stat`, `/proc/meminfo`, `os.statvfs`,
`/proc/pressure/*`), runs on the host so it covers the whole VM even for
dockerized jobs, and is a no-op on non-Linux. See `host_metrics.py`. Sampling
covers the job command only (not praktika pre/post-run).

## Job level — `Result.ext["metrics"]`

`/proc` is read at a fine cadence (`HOST_METRICS_FINE_INTERVAL_SEC`, default
`1s`) and one aggregated point is emitted (and written) per reporting window
(`HOST_METRICS_SAMPLE_INTERVAL_SEC`, default `5s`), so short bursts survive as
the window's peak instead of being averaged away.

| Field | Meaning |
|---|---|
| `duration` | Sampled command runtime, seconds. |
| `cpu_count` | Logical CPUs (whole-VM `cpu%` denominator). |
| `mem_total_gb` / `disk_total_gb` | Total RAM / workspace-filesystem size. |
| `series.cpu` / `series.iowait` / `series.mem` / `series.disk` | Timelines of `[t, avg, peak]` (percent), decimated to `HOST_METRICS_MAX_POINTS` while preserving the envelope. |
| `averages.{cpu,iowait,mem,disk}` | Whole-run **time-weighted** average utilization %. |
| `peaks.{cpu,iowait,mem,disk}` | Exact max utilization % over every fine sample (never decimated away); `mem_gb` / `disk_gb` are the same in absolute units. |
| `psi.cpu_s` | Seconds at least one task stalled waiting for CPU (PSI `some`). |
| `psi.mem_some_s` / `psi.mem_full_s` | Seconds ≥1 task / **all** tasks stalled on memory. |
| `psi.io_some_s` / `psi.io_full_s` | Seconds ≥1 task / **all** tasks stalled on I/O. |
| `n_raw` / `n_windows` | Fine-sample count / emitted window count. |

`disk` and `psi` are omitted when unavailable (bad `HOST_METRICS_DISK_PATH`,
kernel without PSI). `peaks` are rate-independent; PSI totals are accumulated by
the kernel continuously, so they capture contention even between samples.

`cpu` counts `iowait` as idle (a core waiting on the disk does no work), so
`cpu + iowait <= 100` and a job blocked on I/O is not mistaken for an idle one.
Beware that the kernel charges `iowait` per CPU — only cores that go idle while a
task of theirs is blocked in I/O — so a single stalled task shows up as a small
fraction of a large host's capacity. `psi.io_*` is the fraction-of-time
counterpart and does not dilute: a build that spends 100 s of a 950 s job
flushing a freshly pulled docker image to a slow volume reads as ~0% `cpu`,
single-digit `iowait` and ~100 s of `io_full_s`.

### Utilization labels

Applied via `Result.set_label` to jobs worth right-sizing — those that ran at
least `HOST_METRICS_MIN_LABEL_DURATION_SEC` (30 min) **or** on a runner with more
than `HOST_METRICS_MIN_LABEL_MEM_GB` (15 GB) of RAM — and never to skipped jobs.

| Label | Trigger |
|---|---|
| `under-utilized RAM` | peak RAM < 40% (provision for the peak) |
| `under-utilized CPU` | whole-run avg CPU < 20% (bursts don't justify more cores) |
| `ram-pressure` | peak RAM ≥ 95% **or** memory full-stall > 2% of runtime |
| `cpu-bound` | CPU stall > 50% of runtime |
| `io-bound` | I/O stall > 40% of runtime |
| `disk-almost-full` | peak disk ≥ 85% |

## Workflow level — `Result.ext["pipeline_utilization"]`

Every qualifying job's metrics are accumulated into the workflow `Result` (same
plumbing as `storage_usage` / `compute_usage`), giving one KPI to monitor and be
motivated to improve a whole pipeline. Stored as running sums (associative
merge); percentages are derived by `PipelineUtilization.to_summary`. Stall
percentages are the duration-weighted **average share of qualifying job-time**
under pressure — not a single elapsed-workflow fraction (jobs run in parallel on
different runners). Per-job contributions are reconstructed from rounded,
sampled-window averages, so they are estimates rather than bit-exact:

| Summary field | Meaning & weighting |
|---|---|
| `jobs`, `wall_time_s` | Qualifying jobs counted and their total runtime. |
| `cpu_hours` / `mem_gb_hours` / `disk_gb_hours` | Provisioned capacity-time (cores/GB × duration), in hours. |
| `cpu_util_pct` / `mem_util_pct` | Utilization, weighted by **provisioned resource-time** — core-seconds (`cpu_count × duration`) for CPU, GB-seconds (`mem_total_gb × duration`) for RAM. A true efficiency ratio: used ÷ provisioned. A bigger runner and/or longer job impacts it more. Uses the whole-run averages. |
| `disk_util_pct` | Disk uses the **peak** footprint (you size a disk for its high-water mark), weighted by disk-GB × duration. |
| `cpu_stall_pct` / `mem_stall_pct` / `io_stall_pct` | `some`-stall, a wall-clock pressure fraction → **duration-weighted** ("% of pipeline runtime under pressure"). |
| `mem_full_pct` / `io_full_pct` | `full`-stall wastes every core, so weighted by **core-seconds** — the share of provisioned CPU wasted while the box made no progress. |

Low utilization ⇒ over-provisioned pipeline (wasted spend); high stall ⇒
contention / under-provisioning. The weighting normalizes across
differently-sized runners so the numbers are comparable pipeline-wide.

## CIDB — the `attributes` column

Both levels are also written to the CIDB `checks` table (see `cidb.py`), into the
`JSON` column `attributes`, as a **flat map of scalar leaves** (no nested objects
or arrays — those do not belong in the `attributes` column). Every row also
carries the `workflow_name` column. Values are `NULL`/absent when the underlying
metric is unavailable (e.g. no `disk`/`psi`, or metrics predating the `peaks`
schema).

### Per-job row — host usage

Each job's own row (`check_name` = job name, empty `test_name`) carries its
whole-VM host usage in `attributes`, built by `CIDB._host_usage_attributes` from
`Result.ext["metrics"]`. Test-case sub-rows do not repeat it. Keys mirror the
job-level fields above with a `host_` prefix:

| `attributes` key | Source |
|---|---|
| `host_cpu_count`, `host_duration_s`, `host_mem_total_gb`, `host_disk_total_gb` | capacity / runtime |
| `host_cpu_peak_pct`, `host_iowait_peak_pct`, `host_mem_peak_pct`, `host_disk_peak_pct` | `peaks.*` |
| `host_mem_peak_gb`, `host_disk_peak_gb` | `peaks.mem_gb` / `peaks.disk_gb` (absolute) |
| `host_cpu_avg_pct`, `host_iowait_avg_pct`, `host_mem_avg_pct`, `host_disk_avg_pct` | `averages.*` |
| `host_cpu_stall_s` | `psi.cpu_s` |
| `host_mem_stall_s`, `host_mem_stall_all_s` | `psi.mem_some_s` / `psi.mem_full_s` |
| `host_io_stall_s`, `host_io_stall_all_s` | `psi.io_some_s` / `psi.io_full_s` |

### Workflow summary row — pipeline / storage / compute usage

The Finish-workflow (final) job writes one extra row (`check_name` = workflow
name, empty `test_name`) via `CIDB.insert_workflow_usage`, carrying all three
workflow-level aggregates from the workflow `Result.ext`. This is a **synthetic
metrics row**: its canonical `check_status` / `check_duration_ms` are left
empty/zero so it is not mistaken for the workflow's own status record — the real
workflow status and duration are carried in `attributes` instead.

| `attributes` key | Source |
|---|---|
| `pipeline_status` | the workflow's overall status (legacy CIDB form, e.g. `success`/`failure`) |
| `pipeline_start_time` | actual pipeline start (first job), unlike the row's `check_start_time` which is stamped when the final job writes the summary |
| `pipeline_duration_s` | whole-pipeline wall-clock duration (first job start to last job end), distinct from `pipeline_wall_time_s` which sums job runtimes |
| `pipeline_total_jobs`, `pipeline_run_jobs`, `pipeline_success_jobs`, `pipeline_failed_jobs`, `pipeline_skipped_jobs`, `pipeline_dropped_jobs` | job counts in the pipeline by status (`run` = jobs that actually ran, i.e. not skipped/dropped), unlike `pipeline_jobs` which counts only the qualifying jobs |
| `pipeline_*` (e.g. `pipeline_jobs`, `pipeline_cpu_hours`, `pipeline_mem_gb_hours`, `pipeline_disk_gb_hours`, `pipeline_cpu_util_pct`, `pipeline_cpu_stall_pct`, `pipeline_mem_full_pct`, …) | every field of `PipelineUtilization.to_summary`, prefixed `pipeline_` |
| `storage_uploaded_bytes`, `storage_uploaded_items`, `storage_downloaded_bytes`, `storage_downloaded_items` | `StorageUsage` |
| `compute_usage_seconds` | `ComputeUsage` as a `{runner_type: seconds}` map (provisioned wall-time per runner) |

This replaces an older scheme that encoded storage/compute numbers into the
`check_duration_ms` / `test_name` / `test_context_raw` columns, whose schema
meaning did not match the data.

## Settings

All knobs live in `settings.py` (overridable via the settings directory):
`HOST_METRICS_ENABLED`, `HOST_METRICS_SAMPLE_INTERVAL_SEC`,
`HOST_METRICS_FINE_INTERVAL_SEC`, `HOST_METRICS_MAX_POINTS`,
`HOST_METRICS_FILE`, `HOST_METRICS_DISK_PATH`,
`HOST_METRICS_MIN_LABEL_DURATION_SEC`, `HOST_METRICS_MIN_LABEL_MEM_GB`.
