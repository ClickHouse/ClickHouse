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
| `series.cpu` / `series.mem` / `series.disk` | Timelines of `[t, avg, peak]` (percent), decimated to `HOST_METRICS_MAX_POINTS` while preserving the envelope. |
| `averages.{cpu,mem,disk}` | Whole-run **time-weighted** average utilization %. |
| `peaks.{cpu,mem,disk}` | Exact max utilization % over every fine sample (never decimated away); `mem_gb` / `disk_gb` are the same in absolute units. |
| `psi.cpu_s` | Seconds at least one task stalled waiting for CPU (PSI `some`). |
| `psi.mem_some_s` / `psi.mem_full_s` | Seconds ≥1 task / **all** tasks stalled on memory. |
| `psi.io_some_s` / `psi.io_full_s` | Seconds ≥1 task / **all** tasks stalled on I/O. |
| `n_raw` / `n_windows` | Fine-sample count / emitted window count. |

`disk` and `psi` are omitted when unavailable (bad `HOST_METRICS_DISK_PATH`,
kernel without PSI). `peaks` are rate-independent; PSI totals are accumulated by
the kernel continuously, so they capture contention even between samples.

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

## Settings

All knobs live in `settings.py` (overridable via the settings directory):
`HOST_METRICS_ENABLED`, `HOST_METRICS_SAMPLE_INTERVAL_SEC`,
`HOST_METRICS_FINE_INTERVAL_SEC`, `HOST_METRICS_MAX_POINTS`,
`HOST_METRICS_FILE`, `HOST_METRICS_DISK_PATH`,
`HOST_METRICS_MIN_LABEL_DURATION_SEC`, `HOST_METRICS_MIN_LABEL_MEM_GB`.
