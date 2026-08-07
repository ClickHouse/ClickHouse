# Methodology — how to compare Keeper-stress data correctly

This file is the deep-dive companion to the SKILL.md workflow. Read it when you need to choose between comparison methods, justify a delta, or explain why a number is below noise.

## The two comparison methods

### A. Adjacent-nightly comparison (per-PR)

For a specific PR or for narrow attribution:

- `pre`  = the master nightly that finished **before** the PR merged (matched on kind: no-fault for no-fault scenarios, fault for fault scenarios)
- `post` = the first master nightly that **started after** the PR merged (kind-matched)
- Δ = `(post - pre) / pre × 100`

Use this when:
- The user asks "did PR #X regress anything?"
- You need to localize a delta to a specific commit window

Limitations:
- Single-nightly endpoints carry ±3-5 % run-to-run noise on rps/p99
- If multiple PRs landed in the same nightly window, the Δ is **joint** across all of them — list them as `co_merged` and never credit one PR alone for the whole movement

### B. Median-of-3 window comparison (cumulative)

For "what changed over the past N weeks":

- `baseline` = median over the first 3 no-fault nightlies in the window
- `current`  = median over the last 3 no-fault nightlies in the window
- Δ = `(median_current - median_baseline) / median_baseline × 100`

Use this when:
- The user asks "what's the cumulative effect across many PRs"
- You want to smooth out single-nightly noise

Limitations:
- Cannot attribute to specific PRs at sub-5 % precision
- Bench-harness changes during the window are also captured (use `references/known_confounds.md` to check)

### C. PR-branch isolated comparison (cleanest per-PR but limited scenarios)

PR branches each run a 3-scenario smoke stress (`prod-mix-no-fault[default]`, `read-multi-no-fault[default]`, `write-multi-no-fault[default]`). Their values are systematically offset from master nightlies by ~+12 % because they run on different infra (less-loaded runners, fewer parallel scenarios).

To isolate a single PR's effect:
- Take the PR's branch HEAD value on the 3 scenarios
- Take the median of all OTHER PR-branch runs in the same week (the "pool")
- Δ = `(branch_head - pool_median) / pool_median × 100`

Use this when:
- You want a per-PR effect that's free of co-merge contamination
- Master adjacent-nightly comparison is contaminated by other PRs in the window

Limitations:
- Only 3 scenarios available per PR branch
- Pool size depends on how many PRs were active that week
- Sanity-check: PR `#102739` (typos) shows ±5 % rps Δ with this method, so anything below ±5 % is below the noise floor

### Kind-matched baseline + primary fallback (Method A detail)

In Method A (adjacent-nightly Δ) the baseline is the nightly nearest the PR's
merge time. Some nightlies run only fault-sweep scenarios, others only
no-fault, so a per-PR `pre`/`post` pair sometimes can't be drawn from the
matching kind. `build_per_pr_metrics_tsv.py` resolves this in two steps:

1. **Kind-matched first.** For each row's scenario kind (`fault` or
   `no-fault`), use `pre_fault_sha8` / `post_fault_sha8` (or
   `pre_nofault_sha8` / `post_nofault_sha8`) from `pr_to_nightly.tsv`.
2. **Primary fallback.** When the matching-kind nightly is missing, fall
   back to `pre_sha8` / `post_sha8` (the first post-merge nightly regardless
   of kind). The `baseline_kind` cell in `per_pr_metrics_long.tsv` reflects
   this — e.g. `fault(fallback-primary-pre)` means "scenario is fault,
   pre-baseline came from the kind-agnostic primary because no fault-only
   pre-nightly was available".

This is intentional behavior, not a bug: cross-kind comparisons are noisier
than same-kind, so prefer kind-matched but don't drop the row when only the
primary baseline is usable.

## Significance bands (when to call something "real")

| Metric | Direction | clean | watch | regression |
|---|---|---|---|---|
| `rps` | higher better | Δ ≥ −5 % | Δ −5 % to −15 % | Δ < −15 % |
| `read_p99_ms`, `write_p99_ms` | lower better | Δ ≤ +10 % | Δ +10 % to +30 % | Δ > +30 % |
| `error_pct` | lower better | absolute ΔPP < 0.05 | 0.05 to 0.5 | ≥ 0.5 |
| `peak_mem_gb` | lower better | Δ ≤ +10 % | +10 % to +30 % | > +30 % |
| Hard-failure counters | absolute | exactly 0 | n/a | any non-zero |

**Why these specific numbers.** Calibration came from PR `#102739` (a typos-only PR that cannot affect Keeper performance): it showed ~5 % rps swing and ~10 % p99 swing under PR-branch isolation. So 5 % is the rps noise floor and 10 % is the p99/mem noise floor; the bands above set "clean" at the noise floor and "watch" at 3× the noise floor. If the dataset's noise characteristics change, recalibrate against the next clean-PR control.

**Hard-failure counters** are the trump card: any non-zero value of any of these means the cluster broke a guarantee:
- `KeeperCommitsFailed`
- `KeeperSnapshotCreationsFailed`
- `KeeperSnapshotApplysFailed`
- `KeeperRequestRejectedDueToSoftMemoryLimitCount`

## Choosing the right method per question type

| User asks… | Use |
|---|---|
| "Did PR #X regress anything?" | Adjacent-nightly (kind-matched) + PR-branch isolated as second source |
| "What changed between dates A and B?" | Median-of-3 window |
| "Validate these N PRs" | Per-PR scenario deltas + summary table; honor co-merge contamination |
| "Why did metric M change on date D?" | Time-series check on D, then cross-reference `known_confounds.md` |
| "How big is the noise floor?" | Reference `#102739` typos PR (~±5 % rps) and `large-payload-no-fault` rps stddev (CV 0.2 %) |

## Always do these checks before quoting a number

1. **Memory** — if a peak-memory delta is reported, run BOTH `container_memory_bytes` (cgroup) AND `KeeperApproximateDataSize` (Keeper-state). They can disagree because the cgroup is dominated by page cache. Only the Keeper-reported metric reflects actual Keeper state. If the cgroup moved but `KeeperApproximateDataSize` didn't, the delta is NOT a Keeper change.

2. **CPU** — `container_cpu_usage_usec` rate can spike to spurious values (18-38 cores on prod-mix scenarios) due to counter discontinuities (container restarts inside the run). Use `p95_cpu_cores`, never `max_cpu_cores`.

3. **Step changes** — if you see a single-day step in any metric, cross-reference `known_confounds.md`. The step is usually a bench-harness change, not a Keeper change.

4. **Server-side failures** — confirm zero across the full window. If non-zero anywhere, that overrides any "ship them all" verdict.

5. **Co-merged PRs** — when listing per-PR deltas, always include the `co_merged` field. Don't credit a single PR for a joint window.
