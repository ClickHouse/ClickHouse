---
name: keeper-stress-analysis
description: Analyze ClickHouse Keeper stress-test results from play.clickhouse.com / keeper_stress_tests data warehouse. Use whenever the user asks about Keeper performance, validates Keeper PRs against stress dashboards, investigates regressions or improvements in Keeper nightlies, asks about specific date windows / SHAs / PR-sets in Keeper stress tests, wants per-PR or window-vs-window comparisons, asks "did this PR break Keeper", asks "what changed in Keeper between dates", or wants a summary report of Keeper stress runs. Triggers on terms like "keeper stress", "keeper PR", "keeper p99", "keeper memory", "keeper rps", "keeper nightly", "keeper-stress-tests", "keeper validation", "keeper regression", or any question referencing the keeper-stress Grafana dashboard. ALWAYS prefer this skill over re-deriving the workflow from scratch — it captures hard-learned lessons about cgroup-vs-Keeper memory, bench-harness confounds, noise floors, and per-PR attribution limits.
argument-hint: [<date-window>|<pr-list>|<question>]
disable-model-invocation: false
allowed-tools: Bash(curl:*), Bash(python3:*), Bash(awk:*), Bash(mkdir:*), Bash(ls:*), Bash(wc:*), Bash(grep:*), Bash(sort:*), Bash(cat:*), Bash(gh:*), Bash(realpath:*), Bash(cp:*), Bash(chmod:*), Bash(sed:*), Read, Write, Edit, Glob, Grep
---

# Keeper Stress-Test Analysis Skill

Analyse ClickHouse Keeper stress-test results from `keeper_stress_tests.keeper_metrics_ts` on `play.clickhouse.com` (the same data warehouse the Grafana `keeper-stress-run-details` dashboard reads from). The skill captures a tested end-to-end workflow plus hard-earned methodology lessons — use it instead of re-deriving the analysis each time.

## When to use

This skill triggers on these kinds of requests:
- "Validate these Keeper PRs against the stress tests"
- "What changed in Keeper between 2026-04-01 and 2026-05-01?"
- "Did PR #X cause any regression in Keeper stress?"
- "Why did p99 spike on date Y?"
- "Give me a summary report of Keeper stress runs"
- "How are the Keeper PRs performing on the dashboard?"

The skill does NOT touch any other CI data — it's specific to the Keeper stress framework.

## Skill home + working dir convention

- **Skill home**: the directory containing this SKILL.md. May be either
  - user-level: `~/.claude/skills/keeper-stress-analysis/`, or
  - project-level: `<repo>/.claude/skills/keeper-stress-analysis/`.
  Both work; `scripts/rebuild.sh` resolves the home from its own location.
- **Working dir** (default): `tmp/keeper_stress_skill/` under the user's current directory
- The orchestrator `scripts/rebuild.sh` accepts a working-dir argument as `$1`, a lower-bound TS-filter as `$2`, and an optional upper-bound TS-filter as `$3` (default `9999-12-31` = unbounded)

## Workflow — five phases

### Phase 1 — Capture intent

Parse the user's request into one of these shapes:

| Request shape | Indicators | Pipeline to run |
|---|---|---|
| **Date-range window** | "between A and B", "since X", "last N weeks" | Cumulative-gains pipeline (Method B in `references/methodology.md`) |
| **PR set** | List of PR numbers, "validate these PRs", "33 PRs" | Per-PR + per-nightly pipeline |
| **Single PR drill-down** | One PR number, "did #X cause", "regression from #X" | Per-PR card with adjacent-nightly + PR-branch isolation |
| **Free-form analytical** | "Why did metric M change on date D?" | Time-series check + cross-reference `references/known_confounds.md` |

If the user hasn't given a window, **ask** (date range OR PR set OR specific question). Default window: from `2026-03-25` (when the current framework began) to today.

### Phase 2 — Pull staging data

Always run all 6 SQL queries first into `<work_dir>/staging/`:

```bash
# Locate the skill home (works for either ~/.claude/skills/... or <repo>/.claude/skills/...)
SKILL_HOME="$(find ~/.claude/skills .claude/skills -maxdepth 2 -type d -name keeper-stress-analysis 2>/dev/null | head -1)"
"$SKILL_HOME/scripts/rebuild.sh" tmp/keeper_stress_skill 2026-03-25
```

`rebuild.sh` takes three optional args: `$1` work dir, `$2` lower-bound timestamp filter (default `2026-03-25`, when the current keeper-stress framework went live), and `$3` upper-bound timestamp filter (default `9999-12-31`, i.e. unbounded — same behaviour as a single-arg invocation). The bounds are injected into the SQL via `{{TS_FILTER}}` / `{{TS_FILTER_END}}` placeholders and exported to the Python pipeline as `KEEPER_SKILL_THRESHOLD` / `KEEPER_SKILL_THRESHOLD_END` (read by `build_pr_nightly_map.py` and `compute_deltas.py` for the in-window vs out-of-window split). The upper bound is **exclusive** (`ts < $3`), so for "between A and B" inclusive on B, pass B+1 day. To analyse a different window, pass new values — no source edits needed.

For a closed date-range analysis like "what changed between 2026-04-01 and 2026-05-01?", invoke:

```bash
"$SKILL_HOME/scripts/rebuild.sh" tmp/keeper_stress_skill 2026-04-01 2026-05-01
```

The `rebuild.sh` script:
1. Copies `queries/*.sql` and `scripts/*.py` into the work dir.
2. Runs each query against `https://play.clickhouse.com/?user=play` via `curl --data-urlencode`.
3. Substitutes `{{TS_FILTER}}` placeholder if present in the SQL.
4. Builds `merged_metrics.tsv` (one row per scenario × backend × commit; ~95+ columns covering bench, prom, mntr, container metrics).
5. If `<work_dir>/../pr_meta.tsv` exists, builds the per-PR pipeline too.
6. Builds `cumulative_gains.tsv` and `cumulative_gains_summary.tsv`.

The 6 staging files dropped under `staging/`:
- `bench_summary.tsv` — bench-side summary (rps, p99, errors, ops, mem)
- `prom_rates.tsv` — Keeper prom counters as rates per node
- `prom_gauges.tsv` — Keeper prom gauges + cumulative-failure counters
- `mntr.tsv` — ZK 4LW `mntr` outputs
- `container.tsv` — cgroup CPU + memory
- `pr_branches.tsv` — PR-branch smoke stress runs (3 scenarios per branch)

### Phase 3 — Build derived tables

Pick the right script(s) based on Phase 1's intent:

All Python steps are dispatched through `keeper_stress.py`:

| Intent | Run |
|---|---|
| Date-range window | `python3 keeper_stress.py cumulative` |
| PR set | `python3 keeper_stress.py prmap` + `deltas` + `prmetrics` (requires `pr_meta.tsv`) |
| PR-branch isolation | `python3 keeper_stress.py prisol` (reads PR list + branch from `pr_to_nightly.tsv`) |
| Two-commit Δ | `python3 keeper_stress.py diff <shaA> <shaB>` (8-char prefix or full; reads `merged_metrics.tsv`) |
| Noise calibration | `python3 keeper_stress.py noise` (per-(scenario, backend, metric) median / stddev / cv / p95 across the window) |
| Free-form | none — query `merged_metrics.tsv` directly with `awk`/`python` |

The individual step files (`build_metrics_table.py`, `build_pr_nightly_map.py`, etc.) are still importable modules; `keeper_stress.py` dispatches to their `main()` entrypoints.

The per-PR markdown matrix is **not** generated by a script — Claude composes it directly from `per_pr_metrics_long.tsv` (long-form per-PR Δ rows) plus `per_pr_summary.tsv` (cumulative window numbers), using `examples/sample_outputs/PR_PERF_TABLE.md` as the structural template. Verdicts are Claude's judgment based on the data + Phase 5 caveats — there is no canonical lookup table.

For PR-set work, the user needs to provide a `pr_meta.tsv` mapping PR number → title, mergedAt, mergeCommit, base, headRefName. If absent, generate it via `gh`:

```bash
{
  printf 'pr\ttitle\tmergedAt\tmergeCommit\tbase\theadRefName\n'
  for pr in <numbers>
  do
    out=$(gh pr view "$pr" --repo ClickHouse/ClickHouse \
          --json title,mergedAt,mergeCommit,baseRefName,headRefName \
          -q '[.title,.mergedAt,.mergeCommit.oid,.baseRefName,.headRefName] | @tsv' 2>/dev/null)
    printf '%s\t%s\n' "$pr" "$out"
  done
} > tmp/keeper_stress_skill/../pr_meta.tsv
```

(The `pr_meta.tsv` lives one level above the work dir so all scripts can find it. `headRefName` is required for `build_pr_branch_isolated.py` — without it, that step silently produces no rows.)

### Phase 4 — Generate output

Pick the deliverable that matches the request:

| User wants | Where to look |
|---|---|
| Summary report | `references/report_templates.md` (full / tight / one-liner monospace templates with placeholders) |
| Per-PR Markdown table | `examples/sample_outputs/PR_PERF_TABLE.md` (canonical example: data-backed per-PR table with co-merge attribution and noise-floor caveats) |
| Cumulative-gains write-up | Build from `cumulative_gains_summary.tsv` using `references/report_templates.md` formatting |
| Per-PR mover matrix / progress attribution | Build from `per_pr_metrics_long.tsv` (produced by `build_per_pr_metrics_tsv.py`) and `per_pr_summary.tsv` (from `compute_deltas.py`) |
| Full validation report | Compose from the per-PR table + cumulative-gains + caveats sections; mirror the structure of `examples/sample_outputs/PR_PERF_TABLE.md` |

Cross-reference the canonical example when filling templates. Never invent prose without a backing data source.

#### Free-form recipes

Common questions that don't need a new pipeline — just `awk` over an existing TSV:

```bash
# Did <PR> regress fault scenarios specifically?
awk -F'\t' '$1=="<PR>" && $3 ~ /-fault\[/' tmp/keeper_stress_skill/per_pr_scenario_deltas.tsv

# Single-scenario validation (one row, all metric Δs):
awk -F'\t' '$1=="<PR>" && $3=="prod-mix-no-fault[default]"' tmp/keeper_stress_skill/per_pr_scenario_deltas.tsv

# Co-merge diagnosis when a PR's delta looks suspicious — check the `co_merged`
# column in per_pr_summary; if non-empty, the delta is jointly attributable.
awk -F'\t' 'NR==1 || $1=="<PR>"' tmp/keeper_stress_skill/per_pr_summary.tsv

# Per-commit time-series for one (scenario, backend, metric) — emits
# `run_ended\tsha8\tvalue` rows sorted ascending by ts.
awk -F'\t' 'NR==1 {for(i=1;i<=NF;i++) if($i=="<METRIC>") c=i; next}
            $1=="<scenario>" && $2=="<backend>" && $c!="" {print $5, $4, $c}' \
   tmp/keeper_stress_skill/merged_metrics.tsv | sort
```

These three patterns cover the bulk of "drill into one slice of the data" requests; reach for the pipeline only when the slice doesn't already exist as a column or row in `merged_metrics.tsv` / `per_pr_*.tsv`.

### Phase 5 — Apply learned-the-hard-way checks

**Before quoting any number**, run these checks:

#### Memory check (THE most common trap)

If a memory delta > 5 % is reported, separately query:
- `container_memory_bytes` (the cgroup peak — sensitive to bench page cache)
- `KeeperApproximateDataSize` (Keeper's own state report)

If the cgroup moved but `KeeperApproximateDataSize` did NOT, the delta is **bench-side, not Keeper-side**. See `references/known_confounds.md` for PR #100670 example.

```bash
# Quick check pattern:
awk -F'\t' '
NR==1 {next}
$1==SCENARIO && $2==BACKEND {
  date=$5; gsub(/ .*/, "", date)
  printf "%s  sha=%s  KeeperApproxDataSize=%5.2fGB  container_peak=%5.2fGB\n",
    date, $4, $7/1e9, $72+0
}' merged_metrics.tsv | sort
```

#### Step-change check

If a metric changes as a single-day step across multiple unrelated scenarios, it's almost certainly bench-side. Cross-reference `references/known_confounds.md`:
- `#100670` "keeper-bench: go faster" landed `2026-04-04` — affects read-heavy memory + multi-write `error_pct`
- `#101801` "keeper-bench: more features" landed `2026-04-11` — affects rocks-side write-multi memory

#### Noise-floor check

The single-nightly Δ noise floor is **±3-5 % on rps/p99**. The typos PR `#102739` (which cannot affect Keeper performance) shows ±5 % rps Δ via PR-branch isolation — that's the floor. Never claim sub-3 % per-PR effects without an isolation method.

The PR-branch isolation pool widens to ±1 ISO week if the same-week pool has fewer than 2 entries (using `datetime` arithmetic so year/W01/W52-53 boundaries are handled). Same-branch runs are excluded from the pool so prior WIP commits don't bias the median toward the PR's own change. Full method in `references/methodology.md` Method C and the docstring at the top of `scripts/build_pr_branch_isolated.py`.

#### CPU spike check

`container_cpu_usage_usec` rates can spike to spurious 18-38 cores from counter discontinuities. Always use `p95_cpu_cores`, never `max_cpu_cores`. See `references/metric_glossary.md`.

#### Server-side failure check

Always verify these four counters are ZERO across the entire window:
- `KeeperCommitsFailed`
- `KeeperSnapshotCreationsFailed`
- `KeeperSnapshotApplysFailed`
- `KeeperRequestRejectedDueToSoftMemoryLimitCount`

Any non-zero value overrides any positive verdict.

There are two scopes to check, with different sources:

- **Per-PR (post-merge only)**: `compute_deltas.py` already counts these in
  the first nightly that includes the PR and emits `server_failures_post`
  in `per_pr_summary.tsv`. A non-zero value flips the PR's verdict to
  `regression(server-failure)`.
- **Across the entire window**: run the `awk` recipe below against
  `staging/prom_gauges.tsv` to confirm zero across every master nightly in
  the window. This is the gate; the per-PR check is the per-PR symptom.

```bash
# Across-window gate — must produce no rows
awk -F'\t' '
NR>1 && $4 ~ /(CommitsFailed|SnapshotCreationsFailed|SnapshotApplysFailed|RejectedSoftMemoryLimit)/ && $5+0 > 0 {
  print
}' staging/prom_gauges.tsv
# (empty result = clean across all nightlies)
```

#### Co-merge contamination check

When the user provides a PR list and you compute master adjacent-nightly Δs, the same Δ is jointly attributable to all PRs that landed in the same nightly window. Always include a `co_merged` column in per-PR tables. Never credit a single PR for joint-window deltas at >5 % effect size.

## Reference files (load on demand)

When you need deeper guidance, read these into context:

- **`references/methodology.md`** — comparison-method choice (adjacent-nightly vs median-of-3 vs PR-branch isolation), significance bands, environment-offset correction.
- **`references/known_confounds.md`** — catalog of bench-harness PRs that move dashboard metrics; updated as new ones are observed.
- **`references/metric_glossary.md`** — what every column in `keeper_metrics_ts` measures, and which ones to NOT use (e.g. `max_cpu_cores`, raw `container_memory_bytes` for "Keeper memory").
- **`references/report_templates.md`** — three monospace templates (full / tight / one-liner) with placeholder format.

## Verifying the analysis is correct

Spot-check three known data points (these are all baked into `examples/sample_outputs/`):

1. **Master `e02b59d7` (2026-04-02) on `write-multi-no-fault[default]`** must show `errors=0` (pre-bench-jump). **Master `18dfe15a` (2026-04-04) same scenario** must show `errors≈325k`, `error_pct≈3.67`. If divergent, the bench-summary query is wrong.

2. **All four hard-failure counters across all master nightlies since 2026-03-25 must be zero**. If any non-zero, either the data is corrupt or there's a real failure to report.

3. **`740b4a5` (`keeper-object-based-snapshots` branch) `prod-mix-no-fault[default]`** must show `rps=5,764`, `read_p99=545 ms`, `write_p99=535 ms`, `errors=0`. This was the canonical `#99651` validation point.

## Examples

### Example 1 — single PR drill-down

User: "Did PR #99651 cause any Keeper regression?"

Process:
1. Fetch PR meta: `gh pr view 99651 --repo ClickHouse/ClickHouse --json title,mergedAt,mergeCommit`
2. Run `rebuild.sh tmp/keeper_stress_skill 2026-03-25`
3. Filter merged_metrics for the pre-merge nightly (`fdf46ee1`) vs post-merge nightly (`e02b59d7`) on `prod-mix-no-fault[default]` and `write-multi-no-fault[default]`
4. Apply Phase 5 memory check: pull both `container_memory_bytes` and `KeeperApproximateDataSize`. The `prod-mix peak_mem 2.92→2.72 GB (-6.9%)` shows up on cgroup but `KeeperApproximateDataSize` is flat → conclude this is bench-side noise OR snapshot-timing artifact, not real Keeper improvement.
5. Confirm `KeeperSnapshotApplysFailed=0` across 18 follow-on nightlies.
6. Output: per-PR card with the verdict "**clean** — no regression; the prod-mix peak_mem drop is a single-nightly cgroup artifact, not a Keeper-state reduction".

### Example 2 — date-range window

User: "What changed in Keeper between 2026-04-01 and 2026-05-01?"

Process:
1. Run `rebuild.sh tmp/keeper_stress_skill 2026-04-01 2026-05-01` — both bounds are required for a closed window; the third arg pins the upper bound (`ts < 2026-05-01`) so newer nightlies don't drift into the result.
2. Run `build_cumulative_gains.py` — produces `cumulative_gains_summary.tsv` with median-of-3 vs median-of-3 deltas.
3. Apply Phase 5 checks — flag the bench-harness changes from `known_confounds.md` that landed in this window. For `2026-04-01 → 2026-05-01` both `#100670` (`2026-04-04`) and `#101801` (`2026-04-11`) are in-window, so call them out as confounds for any read-heavy memory or rocks-side write-multi memory deltas.
4. Output: a cumulative-gains write-up built from `cumulative_gains_summary.tsv` using `references/report_templates.md` formatting, with conservative deltas + caveats (always include the bench-harness confound notes from `references/known_confounds.md` if any of those PR dates fall in the window).

### Example 3 — Summary report

User: "Give me a summary of these PRs: ..."

Process:
1. Build `pr_meta.tsv` from the PR list using `gh`.
2. Run full pipeline (rebuild.sh + per-PR scripts).
3. Categorize PRs by intent (perf cohort by code path, correctness, tooling, refactor, net-zero).
4. Fill in `references/report_templates.md` "full" template.
5. Apply Phase 5 caveats — if any in-window bench-harness changes, mention by PR number.

## Output discipline

When the user is asking for analysis (not a templated report), produce:
1. **Headline finding** — 1-2 sentences. State the verdict directly.
2. **Backing table** — every claim has a specific scenario+metric+number.
3. **Caveats** — note any noise-floor, co-merge, or bench-harness limitations.

Never produce confident per-PR percentages below 5 % effect size without explicit isolation evidence.

When the user has been pushing for rigor, default to the conservative method (median-of-3 + PR-branch isolation) and report ranges, not point estimates.

## Editing this skill

If you change anything in `scripts/_common.py` — particularly `classify`, `iso_week`, `CLASSIFY_BANDS`, or `HEADLINE_METRICS` — run the unit-test harness before pushing:

```bash
cd <skill_home>/scripts && python3 -m unittest tests.test_common -v
```

The 29 cases gate the per-metric significance bands, the ISO-year-boundary widening, the SHA-prefix matcher, the `KEEPER_SKILL_THRESHOLD_END` default, and the unmerged-PR guard. If they fail, methodology and code have drifted apart — fix one to match the other (the `references/methodology.md` rubric is the binding contract).
