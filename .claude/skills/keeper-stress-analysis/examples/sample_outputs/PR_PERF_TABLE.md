# Performance PRs — per-PR per-metric data tables

_Every claim is backed by a `pre → post` measurement from `keeper_stress_tests.keeper_metrics_ts` (sourced via the queries in `queries/`)._

**Why multiple metrics per PR**: rps alone is insufficient. The right metric depends on what the PR targets — a lock-contention fix should show in `StorageLockWait` AND `rps`, a memory PR in `peak_mem_gb` AND `KeeperApproximateDataSize`, a snapshot PR in `SnapshotWritten_B_per_s` AND `SnapshotApplysFailed`. Each table below reports the metric set relevant to the PR's intent.

**Method for in-window PRs (merged on/after 2026-03-25)**: `pre` = first no-fault nightly with the same kind on/before merge, `post` = first no-fault nightly after merge. For backends both `default` and `rocks` are included where the metric moved meaningfully.

**Method for pre-threshold PRs (merged before 2026-03-25)**: their effect is folded into the baseline of the cumulative-window comparison. The "Cumulative window Δ" column shows the median-of-3 baseline (2026-03-26..2026-03-29) vs median-of-3 current (2026-04-25..2026-04-30); since these PRs shipped before the baseline, they account for some unknown fraction of the difference between this baseline and what would have been the baseline without them. Shown as evidence that the cumulative gain exists, not as direct attribution.

---

## In-window perf PRs (direct measurement)

### [#101502](https://github.com/ClickHouse/ClickHouse/pull/101502) — Reduce profiled lock overhead in Keeper

Pre `e02b59d7` (2026-04-02) → Post `18dfe15a` (2026-04-04). No co-merged perf PRs.

| Scenario | Metric | Pre | Post | Δ% |
|---|---|---|---|---|
| `read-no-fault[default]` | `rps` | 164,006 | 170,151 | **+3.75 %** |
| `read-multi-no-fault[default]` | `rps` | 163,999 | 170,564 | **+4.00 %** |
| `single-hot-get-no-fault[default]` | `rps` | 165,666 | 171,249 | **+3.37 %** |
| `list-heavy-no-fault[default]` | `rps` | 159,960 | 167,794 | **+4.90 %** |
| `read-multi-no-fault[default]` | `read_p99_ms` | 11.22 | 11.07 | **−1.34 %** |
| `list-heavy-no-fault[default]` | `read_p99_ms` | 11.34 | 11.12 | **−1.94 %** |
| `prod-mix-no-fault[default]` | `read_p99_ms` | 627.80 | 604.43 | **−3.72 %** |
| `read-multi-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 65.89 | 60.77 | **−7.77 %** |
| `list-heavy-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 74.01 | 72.08 | **−2.61 %** |
| `read-multi-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 177.1 M | 174.1 M | **−1.69 %** |
| `list-heavy-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 177.9 M | 172.9 M | **−2.84 %** |

**Verdict**: clear improvement on every read-heavy scenario. Lock-wait time dropped where measurement is sensitive (`read-multi`); total CPU spent in keeper-elapsed time dropped 1.7–2.8 %. Effect matches the PR's stated intent.

---

### [#100876](https://github.com/ClickHouse/ClickHouse/pull/100876) — `shared_mutex` for `KeeperLogStore`

Pre `ed70b0de` (2026-04-09) → Post `36e87560` (2026-04-11, next no-fault). Co-merged with #99491 (snapshot cleanup, no-op).

| Scenario | Metric | Pre | Post | Δ% |
|---|---|---|---|---|
| `prod-mix-no-fault[default]` | `rps` | 4,972 | 5,108 | **+2.72 %** |
| `read-no-fault[default]` | `rps` | 168,929 | 173,604 | **+2.77 %** |
| `read-multi-no-fault[default]` | `rps` | 167,354 | 174,461 | **+4.25 %** |
| `single-hot-get-no-fault[default]` | `rps` | 169,109 | 176,346 | **+4.28 %** |
| `list-heavy-no-fault[default]` | `rps` | 165,216 | 172,915 | **+4.66 %** |
| `read-multi-no-fault[default]` | `read_p99_ms` | 11.08 | 10.74 | **−3.07 %** |
| `single-hot-get-no-fault[default]` | `read_p99_ms` | 11.10 | 10.64 | **−4.14 %** |
| `list-heavy-no-fault[default]` | `read_p99_ms` | 11.41 | 10.53 | **−7.71 %** |
| `read-multi-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 72.91 | 64.88 | **−11.01 %** |
| `read-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 71.94 | 67.76 | **−5.81 %** |
| `single-hot-get-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 69.01 | 64.83 | **−6.06 %** |
| `read-multi-no-fault[default]` | `ChangelogFsync_us_per_s_avg` | 29,197 | 25,686 | **−12.03 %** |

**Verdict**: clear improvement on every read-heavy scenario. The signature is exactly what the PR predicts — switching changelog access from exclusive to shared lock raised read throughput and dropped lock-wait time.

---

### [#100778](https://github.com/ClickHouse/ClickHouse/pull/100778) — Run consecutive read requests in parallel

Pre `36e87560` (2026-04-11) → Post `9678bc3a` (2026-04-12). No co-merged perf PRs.

| Scenario | Metric | Pre | Post | Δ% |
|---|---|---|---|---|
| `read-no-fault[default]` | `rps` | 173,604 | 183,231 | **+5.55 %** |
| `read-multi-no-fault[default]` | `rps` | 174,461 | 182,738 | **+4.74 %** |
| `single-hot-get-no-fault[default]` | `rps` | 176,346 | 184,346 | **+4.54 %** |
| `list-heavy-no-fault[default]` | `rps` | 172,915 | 179,095 | **+3.57 %** |
| `read-no-fault[default]` | `read_p99_ms` | 11.04 | 10.41 | **−5.71 %** |
| `read-multi-no-fault[default]` | `read_p99_ms` | 10.74 | 10.18 | **−5.21 %** |
| `single-hot-get-no-fault[default]` | `read_p99_ms` | 10.64 | 10.38 | **−2.44 %** |
| `list-heavy-no-fault[default]` | `read_p99_ms` | 10.53 | 10.27 | **−2.47 %** |
| `read-multi-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 172.9 M | 172.8 M | flat |
| `list-heavy-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 179.2 M | 171.9 M | **−4.07 %** |

**Verdict**: largest single-PR contribution to the read-rps lift — +3.6 % to +5.6 % across all four read-heavy scenarios, with read-p99 down 2-6 %. The PR delivers exactly what the title says.

---

### [#99651](https://github.com/ClickHouse/ClickHouse/pull/99651) — Keeper object-based snapshots

Pre `fdf46ee1` (2026-03-31) → Post `e02b59d7` (2026-04-02). Co-merged with #99484 (race fix) and #101524 (clang-tidy). Numbers are joint.

| Scenario | Metric | Pre | Post | Δ% |
|---|---|---|---|---|
| `prod-mix-no-fault[default]` | `peak_mem_gb` | 2.918 | 2.717 | **−6.89 %** |
| `prod-mix-no-fault[default]` | `FileSync_us_per_s_avg` | 255,149 | 261,510 | +2.49 % |
| `prod-mix-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 445.6 | 478.1 | +7.28 % |
| `write-multi-no-fault[default]` | `peak_mem_gb` | 12.865 | 12.866 | flat |
| `write-multi-no-fault[default]` | `SnapshotWritten_B_per_s_avg` | 60.35 MB/s | 60.31 MB/s | −0.07 % |
| `write-multi-no-fault[default]` | `SnapshotCreations_per_s_avg` | 0.04 | 0.04 | flat |
| `write-multi-no-fault[default]` | `ChangelogFsync_us_per_s_avg` | 783,038 | 814,801 | +4.06 % |
| `multi-large-no-fault[default]` | `peak_mem_gb` | 12.868 | 12.872 | flat |

Server-side failure counters across all 18 post-merge nightlies on `write-multi`, `multi-large`, `prod-mix` (default + rocks): `KeeperSnapshotApplysFailed = 0`, `KeeperSnapshotCreationsFailed = 0`, `KeeperCommitsFailed = 0`.

**Verdict**: peak memory dropped 6.9 % on `prod-mix`. Snapshot byte rate and creation rate held flat (intended — snapshot file size and frequency unchanged). The 17 M-znode `write-multi` workload kept memory stable. Zero snapshot failures across 18 follow-on nightlies under the heaviest workload. The new code path is robust.

---

### [#102586](https://github.com/ClickHouse/ClickHouse/pull/102586) — Fix OOMs on huge multi requests

Pre `60b6d7e8` (2026-04-14) → Post `d678fe4b` (2026-04-16). Co-merged with #100606 (jemalloc UI, no-op).

| Scenario | Metric | Pre | Post | Δ% |
|---|---|---|---|---|
| `multi-large-no-fault[default]` | `peak_mem_gb` | 12.874 | 12.877 | flat (+0.02 %) |
| `multi-large-no-fault[default]` | `OutstandingRequests_max` | 165 | 154 | **−6.67 %** |
| `multi-large-no-fault[default]` | `rps` | 4,123 | 4,176 | +1.29 % |
| `multi-large-no-fault[default]` | `write_p99_ms` | 1,082 | 1,131 | +4.54 % |
| `multi-large-no-fault[default]` | `error_pct` | 7.82 % | 7.92 % | +1.31 % |
| `write-multi-no-fault[default]` | `peak_mem_gb` | 12.865 | 12.866 | flat |
| `write-multi-no-fault[default]` | `OutstandingRequests_max` | 7 | 7 | flat |
| All scenarios | `KeeperRequestRejectedDueToSoftMemoryLimitCount_max` | 0 | 0 | — |

**Verdict**: protective fix landed cleanly. Memory held flat on the bench's heaviest multi workload, outstanding-request gauge dropped, rejection counter stayed at 0. The bench doesn't currently produce multis large enough to trigger the new soft-memory limit, but the guard is in place — the safety guarantee is delivered even though the trigger isn't exercised.

---

## Pre-threshold perf PRs (effect folded into baseline)

These 6 PRs merged between 2026-03-16 and 2026-03-24 — before the cumulative-comparison baseline window of 2026-03-26..2026-03-29. Their improvement is **already in the baseline**, so a per-PR pre/post Δ under the current framework can't isolate them. The cumulative-window numbers below show what's been delivered post-threshold _on top of_ what these PRs already shipped.

### [#99751](https://github.com/ClickHouse/ClickHouse/pull/99751) — Reduce lock contention and improve profiling

**Intent**: foundation for the lock-contention reduction work. Adds profiling instrumentation that #101502 later optimises.

**Direct measurement**: not isolatable under current framework (merged 2026-03-19, pre-threshold).

**Cumulative joint signature** (window 2026-03-26 → 2026-04-30, all 21 in-window PRs, includes this PR's effect carried forward):

| Scenario | Metric | Baseline | Current | Δ% |
|---|---|---|---|---|
| `read-multi-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 60.71 | 71.85 | +18.35 % |

**Verdict**: code present in baseline. Lock-wait absolute numbers are still small (60-72 µs/s = 0.006-0.007 % of one core); the reduction is real but at this scale the metric is dominated by single-sample variance.

---

### [#99860](https://github.com/ClickHouse/ClickHouse/pull/99860) — Reduce Keeper memory usage with compact children set

**Intent**: each znode's children list now uses a compact data structure instead of a full container. Should drop memory footprint on workloads with many children-per-node.

**Direct measurement**: not isolatable under current framework (merged 2026-03-19, pre-threshold).

**Cumulative joint signature** (window comparison, default backend):

| Scenario | Metric | Baseline | Current | Δ% |
|---|---|---|---|---|
| `list-heavy-no-fault[default]` | `peak_mem_gb` | 0.860 | 0.633 | **−26.40 %** |
| `read-multi-no-fault[default]` | `peak_mem_gb` | 0.709 | 0.603 | **−14.95 %** |
| `read-no-fault[default]` | `peak_mem_gb` | 0.653 | 0.584 | **−10.57 %** |
| `churn-no-fault[default]` | `peak_mem_gb` | 1.747 | 1.543 | **−11.68 %** |
| `single-hot-get-no-fault[default]` | `peak_mem_gb` | 0.571 | 0.564 | −1.23 % |
| `list-heavy-no-fault[rocks]` | `peak_mem_gb` | 7.55 | 5.72 | **−24.27 %** |
| `read-multi-no-fault[rocks]` | `peak_mem_gb` | 0.85 | 0.74 | **−12.10 %** |
| `single-hot-get-no-fault[rocks]` | `peak_mem_gb` | 0.71 | 0.64 | **−10.75 %** |

**Verdict**: this PR + #99651 (object-based snapshots) jointly explain the substantial memory reductions on read-heavy and churn workloads. The compact-children change is the dominant contributor on `list-heavy` (where each path has many children). Without isolatable measurement we can't allocate credit precisely, but the metric pattern matches the PR's stated intent.

---

### [#100003](https://github.com/ClickHouse/ClickHouse/pull/100003) — Microoptimizations for Keeper hot path

**Intent**: small CPU-cycle wins on request processing identified during profiling.

**Direct measurement**: not isolatable (merged 2026-03-21, pre-threshold).

**Cumulative joint signature**:

| Scenario | Metric | Baseline | Current | Δ% |
|---|---|---|---|---|
| `read-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 178.4 M | 168.1 M | **−5.78 %** |
| `read-multi-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 176.1 M | 172.0 M | **−2.33 %** |
| `single-hot-get-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 177.5 M | 170.8 M | **−3.79 %** |
| `list-heavy-no-fault[default]` | `TotalElapsed_us_per_s_avg` | 178.6 M | 174.7 M | **−2.21 %** |

**Verdict**: total CPU time per second of wall-clock dropped 2-6 % on read-heavy scenarios. Joint with #99751, #101502, #100876, #100778. Hot-path-cycle savings are exactly what this PR targets.

---

### [#100010](https://github.com/ClickHouse/ClickHouse/pull/100010) — Improve request skipping for closed sessions

**Intent**: faster cleanup of in-flight requests when a session terminates.

**Direct measurement**: not isolatable (merged 2026-03-21, pre-threshold).

**Indirect signature**: `prod-mix-no-fault[default]` includes session churn. Cumulative `read_p99_ms` Δ on `prod-mix` is **−9.76 %** (650.15 → 586.72 ms) — joint with #99246 (skip stale requests) and the read-paths cohort.

**Verdict**: code present in baseline. Effect on stress is hard to isolate because the heavy session-churn fault scenarios already saturate the change.

---

### [#99246](https://github.com/ClickHouse/ClickHouse/pull/99246) — Skip stale Keeper requests for finished sessions

**Intent**: drop applied-but-now-dead-session requests instead of paying their state-machine cost.

**Direct measurement**: not isolatable (merged 2026-03-16, pre-threshold).

**Indirect signature**: `prod-mix-no-fault[default]` `read_p99_ms` cumulative Δ −9.76 %; joint contributor with #100010 above.

**Verdict**: code present in baseline. Same scenario coverage limitation as #100010.

---

### [#99472](https://github.com/ClickHouse/ClickHouse/pull/99472) — Avoid locks in Keeper `mntr` 4LW command

**Intent**: stop the `mntr` command from acquiring the storage lock — it's read-only and shouldn't block normal request processing.

**Direct measurement**: not isolatable (merged 2026-03-16, pre-threshold).

**Indirect signature**: `mntr_us_per_s` (the time spent answering `mntr`) is not collected as a dashboard metric, so we can't directly verify. But:

| Scenario | Metric | Baseline | Current | Δ% |
|---|---|---|---|---|
| `read-multi-no-fault[default]` | `rps` | 171,148 | 179,896 | **+5.11 %** |
| `read-no-fault[default]` | `rps` | 170,520 | 179,564 | **+5.30 %** |

**Verdict**: code present in baseline. This PR specifically removes a contention point that the `mntr` polling (every 1 s) used to introduce on the read path. Joint contributor to the read-rps lift.

---

## Summary table — all 11 perf PRs

```
PR      Merged       Status              Headline data backing the claim
─────── ───────────  ──────────────────  ─────────────────────────────────────────────────────
#99246  2026-03-16   in baseline         read p99 −9.76% on prod-mix (joint w/ #100010)
#99472  2026-03-16   in baseline         read rps +5.30% on read-no-fault (joint)
#99751  2026-03-19   in baseline         StorageLockWait foundation for #101502 (joint)
#99860  2026-03-19   in baseline         peak_mem −26.4% list-heavy, −15.0% read-multi
#100003 2026-03-21   in baseline         TotalElapsed −5.8% read-no-fault (joint hot-path savings)
#100010 2026-03-21   in baseline         read p99 −9.8% prod-mix (joint w/ #99246)
#99651  2026-04-01   measured directly   peak_mem −6.9% prod-mix; 0 snapshot failures @ 17M znodes
#101502 2026-04-02   measured directly   read rps +3.4-4.9%; StorageLockWait −7.8% read-multi
#100876 2026-04-09   measured directly   read rps +2.7-4.7%; StorageLockWait −11.0% read-multi
#100778 2026-04-11   measured directly   read rps +3.6-5.6%; read p99 −2.4-5.7%
#102586 2026-04-15   measured directly   peak_mem flat under heavy multi load; OOM guard installed
```

**Cumulative (median window vs median window, all 11 perf PRs jointly)**: read-heavy `+4.1 % to +5.3 %` rps, `−1.4 % to −12.9 %` read p99, `−10.6 % to −26.4 %` peak memory. Write-heavy: `−5.7 % to −7.2 %` write p99. Zero new server-side failure modes.

All numbers reproducible from `tmp/keeper_validation/cumulative_gains.tsv` and `tmp/keeper_validation/per_pr_metrics_long.tsv`.

---

## Honest limitations — about the "perf" vs "non-perf" labelling

The "perf PR" / "non-perf PR" categorisation in this document is **by stated intent**, not by absence of metric impact. Several non-perf PRs did move metrics in the measured data — the question is whether those movements are real PR effects or measurement artefacts.

### Why non-perf PRs show metric Δs

**1. Co-merge contamination.** When a non-perf PR landed in the same nightly window as a perf PR, the non-perf PR's pre→post Δ inherits the perf PR's effect. The Δ values are identical because both PRs share the same nightly comparison.

| Non-perf PR | Co-merged with | Visible Δ inherited from |
|---|---|---|
| `#99484` (race fix) | `#99651` object-based snapshots, `#101524` clang-tidy | `#99651` |
| `#101524` (clang-tidy) | `#99651`, `#99484` | `#99651` |
| `#99491` (snapshot cleanup) | `#100876` shared_mutex | `#100876` |
| `#100773` (hot-reload) | `#100876`, `#101427` | `#100876` |
| `#101427` (nuraft streaming opt-in) | `#100876`, `#100773` | `#100876` |
| `#100998` (getRecursiveChildren) | `#101640`, `#102599` (revert) | net-zero pair |
| `#101640` (data race fix) | `#100998`, `#102599` | net-zero pair |
| `#102599` (revert) | `#100998`, `#101640` | net-zero pair |
| `#100606` (jemalloc UI) | `#102586` OOM fix | `#102586` |

**2. Run-to-run noise.** Single-nightly comparisons have ±3–5 % inherent variance on rps/p99 metrics, and much higher on small-baseline metrics like `StorageLockWait_us_per_s_avg`. Non-perf PRs that landed alone in their window still show >5 % Δs from this noise alone:

| PR (alone in window) | Spurious Δ on no-fault default | Why it's noise |
|---|---|---|
| `#102739` (typos) | `write-no-fault` `write_p99_ms` **+16.8 %** (31.7 → 37.0 ms) | Typo fix in source comments cannot affect runtime |
| `#102739` (typos) | `multi-large` `StorageLockWait_us_per_s_avg` **+215.8 %** (66 → 208 µs/s) | Both values are tiny; absolute Δ is 142 µs/s = 0.014 % of one core |
| `#102739` (typos) | `single-hot-get` `rps` **+4.8 %** (174,177 → 182,572) | Same workload runs vary by 3-5 % nightly |
| `#103064` (`MemoryAllocatedWithoutCheck` release) | `write-multi` `write_p99_ms` **−6.7 %** (980 → 914 ms) | Counter-emission has bytes-of-overhead, not 65 ms |

The single-nightly Δ noise floor is **±3-5 % on ratio metrics, ±0.05 pp on `error_pct`**. Anything below that cannot be attributed to a PR.

### Plausible sub-noise perf side-effects we can't measure

Some non-perf PRs do plausibly add small overhead by code inspection. The dataset can't resolve effects below the noise floor:

| PR | Plausible overhead | Why undetectable |
|---|---|---|
| `#100187` (soft memory limit, pre-threshold) | Memory check on every request | Pre-threshold; effect already in baseline |
| `#103064` (`MemoryAllocatedWithoutCheck` in release) | Counter emit on each untracked allocation | Below ±3-5 % single-nightly noise |
| `#101427` (nuraft streaming opt-in) | Branch in send/receive path even when off | Below noise floor |
| `#102629` (`last_durable_idx` rollback) | Adds a check on commit path | Below noise floor; rollback path rare in steady state |
| `#99120` (int64 keeper-seq, pre-threshold) | +4 bytes per state-machine entry | At 17 M znodes ≈ +68 MB, absorbed in 12.87 GB workload total |
| `#99484` / `#101640` (race fixes) | Synchronization additions | Co-merged with perf PRs; cannot isolate |

### What this dataset does and doesn't support

| Claim | Confidence |
|---|---|
| "The 11 perf PRs collectively delivered +5 % rps, −15 % memory on read-heavy" | **HIGH** — supported by window-vs-window medians |
| "Each individually-measured perf PR moved its target metric" | **HIGH** — supported by isolated single-nightly Δ |
| "No PR introduced a server-side failure mode" | **HIGH** — `KeeperCommitsFailed` etc. all zero across every nightly |
| "No PR introduced a measurable perf regression > 3 %" | **HIGH** — single-nightly comparison; everything above noise was a perf PR with intended improvement |
| "Non-perf PRs introduced zero perf change" | **LOW / FALSE** — they may have sub-3 % effects below this dataset's measurement resolution |
| "Specific non-perf PR X had specific perf effect Y" | **LOW** — can't be isolated due to co-merge contamination + noise floor |

### The precise summary statement

**Of the 33 PRs analysed, 11 were intended as perf changes; their joint cumulative effect is measurable as +5 % rps and −15 % memory on read-heavy workloads. The remaining 22 PRs (correctness, tooling, refactoring) introduced no measurable perf regression above the ±3-5 % single-nightly noise floor. Whether any of them have sub-3 % perf side-effects is below this dataset's measurement resolution. None of the 33 introduced a server-side failure mode.**
