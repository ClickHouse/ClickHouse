# Verdict rules

The skill's job is to decide whether a performance result is actionable, not merely to restate a dashboard row.

## Evidence checklist

For every suspicious change, try to fill this table:

| Evidence | Question | Strong signal | Weak/noisy signal |
|---|---|---|---|
| Changed metric | Is the raw diff above threshold? | large diff, consistent direction | barely above threshold |
| Repeated PR runs | Does it recur over days/runs? | same query/metric/arch repeats | appears once/disappears/flips |
| Confidence | Does confidence keep or downgrade? | tier/reason supports raw change; named checks explain why | tier says noise/suspect; adaptive threshold or raw samples downgrade |
| Trend/history | Is selected run outside normal spread? | outside p95/recent band, stable history | inside recent p95, frequent spikes/change points |
| Related metrics | Do CPU/real/client/memory agree? | consistent metrics | contradictory metrics |
| Coverage/PR diff | Did PR touch executed code? | intersecting covered changed files | no overlap/unrelated files |
| Flamegraph | Does stack delta explain metric? | hotspot aligns with PR/query | absent/unrelated/mixed |
| Local run | Does controlled local pair reproduce? | repeatable, significant p-value | one-off or toolchain-biased |

## Verdict labels

### `real regression`

Use when most of the following hold:

- CI diff is above threshold.
- Same query/metric/arch repeats across PR runs or confidence supports it.
- History does not show the same random movement frequently.
- Related metrics are consistent.
- PR/coverage relation is plausible, or flamegraph points at changed behavior.

Do not require every item; explain missing evidence.

### `likely noise`

Use when:

- changed row appears once and disappears in later PR runs;
- selected value sits inside normal trend/history spread;
- confidence downgrades raw evidence;
- related metrics contradict the headline;
- no plausible PR/test relation and no profile support.

Still show the data that proves this.

### `needs rerun`

Use when:

- raw diff is large but confidence/history/repeated runs disagree;
- only one run exists;
- result is near threshold;
- setup or infrastructure may have distorted measurements;
- local and CI disagree.

Recommended action: rerun performance CI or run a focused local pair.

### `unstable test`

Use when:

- trend/history shows frequent comparable spikes;
- same query flips slowdown/speedup across runs;
- dashboard marks unstable metrics;
- self-comparisons or repeated runs are noisy.

Do not use this label just because a result is inconvenient.

### `real improvement`

Use when:

- speedup is above threshold;
- repeated runs/confidence/history support it;
- it is not simply a reversal of random instability.

### `local-only evidence`

Use when local `perf.py` shows a meaningful result but CI/dashboard evidence is absent or not checked.

### `not enough evidence`

Use when required identity is missing:

- unknown run;
- unknown query index;
- wrong metric/arch;
- unavailable history and no repeated/local evidence.

## Repeated PR run rules

When checking a PR, list all performance.ci runs returned by:

```bash
curl -fsS "$BASE/runs?q=$PR" | jq .
```

Then compare the same test/query/metric/arch across runs.

Interpretation:

- `N/N` repeated with similar diff: strong.
- `1/N` only: weak; likely rerun/noise unless diff is huge and profile explains it.
- direction flips: unstable.
- only old run has it and latest run is clean: likely resolved/noise.
- latest run newly has it after earlier clean runs: investigate; could be new PR changes or master movement.

## Master/history rules

Use dashboard trend/history instead of guessing.

Evidence for flakiness/noise:

- selected point lies in the recent normal band, e.g. below/near p95;
- comparable spikes appear without the PR;
- confidence adaptive threshold is larger than the raw effect;
- `changePoints` show a master change before/after the PR run;
- `periodComparisons` show frequent similar movements.

Evidence for new PR effect:

- selected point is outside recent p95 by a meaningful factor;
- no comparable historical spikes;
- repeated PR runs show same movement;
- PR touched covered relevant code.

Required history wording:

> History center trend: 976 points over 2026-04-24 → 2026-06-15; all p05/p50/p95 = ..., recent 30 points over 22.7h p05/p50/p95 = ...; candidate is 3.06x p95.

Forbidden history wording:

> min=..., median-ish=..., max=..., last=... .

## Coverage/PR relation rules

Coverage answers: did this test execute code touched by the PR?

It does not answer: did the PR cause the slowdown?

Use coverage to phrase plausibility:

- `intersectingFiles > 0`: plausible relation; inspect changed hunks.
- touched-only files but no coverage overlap: relation less direct.
- covered-only files but PR did not touch them: maybe indirect behavior change.
- coverage unavailable: say unavailable; use PR diff and query text manually.

## Flamegraph rules

Use flamegraph only for the matching:

- run id;
- test;
- query index;
- metric;
- arch;
- trace type.

Interpretation:

- CPU diff stack grows in candidate: compute hotspot candidate.
- Real grows but CPU does not: wait/I/O/lock/scheduler likely.
- Memory trace grows: allocation pressure likely.

Do not claim root cause if stack delta does not connect to query/PR changes.

Required flamegraph wording:

- include the UI query-page link first (`/runs/<runId>/tests/<test>/queries/<queryIndex>`), because that opens the Flamegraphs card;
- include raw API link only as supporting machine-readable evidence;
- list top sample deltas;
- say whether frames are absent.

Forbidden flamegraph wording:

> flamegraph diff exists and contains relevant samples.

That is too vague to review.

## Local perf.py rules

Meaningful local result requires:

- enough runs;
- p-value <= 0.05;
- diff above test threshold;
- repeat stability when result is important;
- identical datasets and comparable server settings.

Local-only result should be reported as local-only until checked against CI/history.

## Report wording

Good:

> The latest PR run shows `aggregation_in_order_2/4` `client_time/arm` at `+23.2%`, above a `21.7%` threshold. The same query appears in 3/4 PR runs, history does not show comparable spikes, and coverage intersects `src/AggregateFunctions/...`; verdict: real regression, medium confidence.

Bad:

> This is a regression because the table says slower.

Bad:

> Confidence: `M1:downgrade, M2:neutral, M3:downgrade`.

Good:

> Confidence tier is `noise`: History Adaptive Threshold downgraded because observed 47.2% is below the 94.8% threshold from recent master noise; Raw Sample Evidence also downgraded.

Bad:

> Cross-run behavior is unstable.

Good:

> Cross-run chart oldest→newest: -15.3%, +19.8%, +6.6%, -10.7%, +15.3%, -15.6%, -3.1%, +22.5%; direction flips repeatedly, so verdict: unstable.

Good:

> The latest rerun no longer reproduces the earlier `+24%` row: the same test/query/metric/arch is now `+1.2%`, below threshold. The older row is superseded, so verdict: likely noise or resolved.

Bad:

> Shows an old `+24%` measurement table after a newer clean rerun with no explanation.
