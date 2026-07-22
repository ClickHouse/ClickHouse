# Monospace report templates

Three templates depending on report scope. Pick whichever matches the audience. Each renders cleanly in any monospace context — terminal output, code-fenced markdown (PR comments, design docs), team-chat code blocks, etc.

## Template 1 — Full validation report (release-readiness or broad audience)

Use this for comprehensive validation reports. Replace placeholders in `{{double-curly}}`.

````
🔍 Keeper Stress-Test Validation — {{n_prs}} PRs reviewed
   Window: {{date_from}} → {{date_to}} · {{n_nightlies}} nightly stress runs · {{n_scenarios}} scenario/backend combos

═══════════════════════════════════════════════════════════════════════════════════════════════
  ✅ SHIP-READINESS — every PR landed clean
═══════════════════════════════════════════════════════════════════════════════════════════════

   • 0 server-side failures across all stress runs
     (KeeperCommitsFailed = 0, KeeperSnapshot{Creations,Applys}Failed = 0,
      KeeperRequestRejectedDueToSoftMemoryLimit = 0)
   • 0 regressions above the ±3-5% measurement noise floor
   • Per-PR smoke stress (3 scenarios) ran clean for every Keeper-touching PR before merge

═══════════════════════════════════════════════════════════════════════════════════════════════
  📊 PERFORMANCE GAINS — what these PRs delivered (median-of-3 baseline → current)
═══════════════════════════════════════════════════════════════════════════════════════════════

   ┌─ Read throughput
   │     {{TABLE: scenario | default Δ% | rocks Δ% }}
   │
   ├─ Read tail latency (p99)
   │     {{TABLE: scenario | default Δ% | rocks Δ% }}
   │
   ├─ Write throughput
   │     {{summary: flat / improved / regressed and by how much}}
   │
   ├─ Write tail latency
   │     {{TABLE: scenario | default Δ% | rocks Δ% }}
   │
   └─ Memory (Keeper-reported state, not cgroup)
       {{e.g. write-multi[default]: KeeperApproximateDataSize 5.04 → 4.83 GB (-4.2%)}}

═══════════════════════════════════════════════════════════════════════════════════════════════
  🚀 PERF COHORT — N PRs that drove the gains
═══════════════════════════════════════════════════════════════════════════════════════════════

   No single PR moved metrics by >5% individually. The improvements are
   cumulative across many small changes:

   ┌─ Lock contention reduction
   │   {{PR list with one-line descriptions}}
   │
   ├─ Read-path parallelism / fast-path
   │   {{PR list}}
   │
   ├─ Memory footprint reduction
   │   {{PR list}}
   │
   └─ Hot-path microoptimisations
       {{PR list}}

═══════════════════════════════════════════════════════════════════════════════════════════════
  ✅ CORRECTNESS / 🔧 TOOLING / 🧹 REFACTOR / ⏪ NET-ZERO PRS
═══════════════════════════════════════════════════════════════════════════════════════════════

   {{categorized lists}}

═══════════════════════════════════════════════════════════════════════════════════════════════
  📋 COUNTS
═══════════════════════════════════════════════════════════════════════════════════════════════

   {{counts table summing to total}}

═══════════════════════════════════════════════════════════════════════════════════════════════
  ⚠️  CAVEATS
═══════════════════════════════════════════════════════════════════════════════════════════════

   • Per-PR attribution at <5% rps precision is below the noise floor.
   • {{any known confound that landed in window — see known_confounds.md}}
   • Best-day nightlies show larger gains than median-of-3; +X% headline is conservative.

🎯 Bottom line:
   {{2-3 line summary based on actual deltas}}
````

## Template 2 — Tight version (for general engineering channels)

Use this for quick status updates. Single paragraph + one-liner per category.

```
Keeper PRs validation — {{n_prs}} PRs, {{verdict_one_word}}.
Window: {{date_from}} → {{date_to}}.

📊 Read paths: +X% rps median, -Y% read p99 on heavy scenarios
✍️  Writes: throughput flat (within ±3-5% noise); -Z% write p99 on default backend
🧠 Memory: +/- N% Keeper-state on {{scenario}} (cgroup deltas excluded — those are bench artifacts)
✅ 0 server-side failures across {{n_nightlies}} nightlies × {{n_scenarios}} scenarios
🚀 Top contributors: {{top 3-4 PRs}}
🔧 Plus N correctness fixes + M tooling features

Full report: {{path}}
```

## Template 3 — One-liner (for status / standup)

```
✅ Keeper {{n_prs}}-PR validation: shipped clean. +{{X}}% read rps, -{{Y}}% write p99, 0 server-side failures across {{n_nightlies}} nightlies. Report in {{path}}.
```

---

## Filling in templates

The skill's pipeline produces these artefacts from which to extract values:

| Need | Source artefact | Field |
|---|---|---|
| Headline rps Δ% | `cumulative_gains_summary.tsv` | `rps_pct` per `read-no-fault[default]` etc. |
| Headline p99 Δ% | `cumulative_gains_summary.tsv` | `read_p99_pct`, `write_p99_pct` |
| Per-PR table | `per_pr_summary.tsv` + `pr_to_nightly.tsv` | one row per PR |
| Server-failure count | `merged_metrics.tsv` | filter on `*Failed_max > 0` across `CommitsFailed_max`, `SnapshotApplysFailed_max`, `SnapshotCreationsFailed_max`, `RejectedSoftMemLimit_max` (should be empty). The raw `staging/prom_gauges.tsv` exposes these as rows with `name = ClickHouseProfileEvents_Keeper*Failed*` and value in `max_value`; filter there if you haven't built `merged_metrics.tsv` yet. |
| Nightly count | `merged_metrics.tsv` | `count(distinct sha8)` |
| Scenario+backend count | `merged_metrics.tsv` | `count(distinct (scenario,backend))` |
| Memory verification | both `container.tsv` AND `prom_gauges.tsv` (look at `KeeperApproximateDataSize`) | confirm Keeper-reported memory matches the claimed Δ |

## Monospace rendering tips

- Wrap the whole block in triple backticks for monospace alignment
- Prefer ASCII box drawing (`═`, `│`, `┌`, `└`) over Unicode tables — handles consistently across terminal, GitHub PR comments, and chat code blocks
- Keep line width ≤ 95 chars to avoid wrapping in narrower viewers
- Don't use markdown tables (`|---|---|`) inside code blocks; they render as literal text, not as a table
- Emojis at section markers (✅ 📊 🚀 ✍️ 🧠 🧹 ⏪ ⚠️ 🎯) are fine and improve scannability
