# Known confounds — bench-harness PRs that move dashboard metrics

These are PRs that change `keeper_metrics_ts` numbers without changing Keeper itself. They produce step-changes on a single date that look like Keeper improvements/regressions but are actually load-generator changes. **Always check this list before attributing a metric movement to a Keeper PR.**

When you see a single-day step change, query the time-series and cross-reference the date here.

## #100670 — "keeper-bench: go faster"

- **Merged**: 2026-04-04
- **Files changed**: `programs/keeper-bench/*` only (Generator.cpp, Runner.cpp, Stats.cpp, etc.)
- **What changed**: Removed the producer→queue→consumer architecture in the load generator. Each thread now owns its own request generator with its own RNG seed. The internal queue that previously throttled workers is gone.
- **Visible effects on master nightlies starting 2026-04-04**:
  - **`peak_mem_gb` (`container_memory_bytes`) drops on read-heavy scenarios as a single-day step**:
    - `list-heavy-no-fault[default]`: 0.86 GB → 0.55 GB (−36 %)
    - `read-multi-no-fault[default]`: 0.71 GB → 0.50 GB (−30 %)
    - `read-no-fault[default]`: 0.69 GB → 0.51 GB (−26 %)
    - `churn-no-fault[default]`: 1.74 GB → 1.55 GB (−11 %)
  - **`error_pct` jumps on multi-write scenarios** (~3 % bench-counted client timeouts that were previously hidden by queue backpressure)
  - **`KeeperApproximateDataSize` (Keeper-reported state)**: unchanged. This is the smoking gun — the cgroup memory dropped but Keeper's own state didn't, so the change is in the bench, not Keeper.
- **What's NOT affected**: Server-side failure counters (all stay at 0).
- **When attributing memory deltas around or after this date**, always also pull `KeeperApproximateDataSize` to verify whether the change is real Keeper-state or just page-cache reduction.

## #101801 — "keeper-bench: more features"

- **Merged**: 2026-04-11
- **Files changed**: `programs/keeper-bench/*` only
- **Visible effects on master nightlies starting 2026-04-11**:
  - **`peak_mem_gb` step UP on `write-multi-no-fault[rocks]` and `multi-large-no-fault[rocks]`**:
    - `write-multi-no-fault[rocks]`: 6.03 GB → 9.36 GB (+55 %), znode_delta 23.6 M → 27.3 M (+15 %)
    - Bench is now generating more sub-ops per multi, which RocksDB-backed state stores differently than in-memory state.
  - `default` backend doesn't show this because the workload was already saturating the natural znode cardinality there.
- **What's NOT affected**: rps unchanged (~3060 throughout); server-side failure counters at 0.

## How to detect a new bench-harness confound

If you see a metric that changes as a synchronised step across MULTIPLE scenarios on the SAME date:
1. Pull the time-series for 5+ scenarios on that metric.
2. If the step is on the same date for all of them, it's almost certainly bench-side, not Keeper-side.
3. Check what merged on master that date by date-filtering `git log master --since=DATE --until=DATE+1day` for `programs/keeper-bench/*`.
4. Add the new finding to this file.

## How to detect a Keeper-side change vs a bench-side change

| Signal | Keeper change | Bench change |
|---|---|---|
| Movement on multiple unrelated scenarios on the same day | unlikely (Keeper changes usually affect specific paths) | likely (bench affects all workloads it generates) |
| `KeeperApproximateDataSize` correlates with `container_memory_bytes` | yes | no (bench changes affect cgroup but not Keeper state) |
| Step change vs gradual trend | usually gradual (release → adoption → next release cycle) | usually a step (bench PR landed) |
| Server-side counters move with it | possibly | never (bench can't move server counters) |
| Affects rocks AND default similarly | yes | sometimes asymmetric (rocks has different state shape) |

## Tools for checking

```bash
# Time-series for a metric on a scenario
awk -F'\t' '
NR==1 {next}
$1==SCENARIO && $2==BACKEND {
  date=$5; gsub(/ .*/, "", date)
  printf "%s  %s  metric=%s\n", date, $4, $METRIC_COL
}' merged_metrics.tsv | sort

# Find date when a step occurred — diff adjacent rows
awk -F'\t' '
NR==1 {next}
$1==SCENARIO {
  if (prev != "" && ($METRIC_COL+0)/(prev+0) < 0.85) print "STEP DOWN on", $5
  prev = $METRIC_COL
}' merged_metrics.tsv | sort
```
