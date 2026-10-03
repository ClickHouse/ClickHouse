# `LowCardinality` `countmin` crossover measurements

## Recommendation

Use separate thresholds: **2 for dense counting, 10 for bounded sparse counting**.
For each column passed to `StatisticsCountMinSketch::build`, let `N` be the row
count and `D` the retained dictionary size:

1. Use `countKeys` when `D <= N / 2 + dictionary_special_values`.
2. Otherwise, count touched dictionary indexes until their number exceeds
   `N / 10`, as in the current implementation.
3. Flush the counted prefix with its frequencies, then process only the remaining
   rows individually. Do not recount the prefix or treat `D` as the number of
   referenced values.

This is a conservative choice supported by the workloads below, not a universal
crossover. The experiments do **not** justify lowering the sparse threshold to
4 or 8 globally, nor choosing a precise tiny-block cutoff. The production
implementation now uses these separate thresholds without experimental dispatch.

## Maintained scenarios

The single `insert_statistics_countmin_crossover.xml` suite is runnable by
`tests/performance/scripts/perf.py`. The broad calibration sweep below was first
reduced from 250 to 22 queries, then consolidated into **10 regression cases**:

| Group | Expanded queries | Retained coverage |
| --- | ---: | --- |
| Dense crossover | 4 | 1,048,576 rows; reuse 1 and 2; numeric and 16-byte strings; permuted order |
| Retained dictionary | 4 | 65,536 rows retaining 524,289 dictionary entries; reuse 1 and 10; numeric and 128-byte strings |
| Distributions | 2 | 65,536 rows; unique-prefix/common-suffix skew; repeated non-NULL values among 90% NULLs |

This keeps the dense crossover, costly low-reuse fallback, retained-dictionary
sparse path, ordering sensitivity and NULL behavior. Million-row crossover inputs
remain large enough to expose hash-table allocation/cache costs; shorter strings
avoid long-string hashing dominating the suite. Consolidation avoids three
separate setup and synchronization phases.

Collection-disabled baselines were useful for calibration but are omitted from
the ongoing before/after regression suite, along with the mostly-distinct NULL
case. Intermediate reuse sweeps, redundant orderings and noisy tiny-block timings
remain investigation data. The earlier 22-case suite is archived locally under
`tmp/countmin-implementation/22-case-suites/`.

## Consolidated 10-case validation

On the same ARM host, the final suite completed 13 measured runs per case on
each of two servers: **260 measurements and 20 successful warm-ups**. No cases
were skipped or partial, and both stateless tests still matched their references.

The harness reported **22.838 seconds total**, versus **57.316 seconds** for the
intermediate 22-case suite. The run stage decreased from 45.397 to 22.091 seconds.
This compares suite footprints, not identical workloads. Global synchronization
also varied: 11.063 seconds across the previous three invocations versus 0.058
seconds for the consolidated invocation, so the total reduction is not entirely
due to query changes.

The production implementation remained faster at reuse 2: numeric values went
from 148.423 to 97.276 ms (34.5%), and 16-byte strings from 222.670 to 163.397 ms
(26.6%). The skew case improved 48.3%; the remaining cases were within 1.8%.
Both comparison servers shut down successfully afterward.

Logs: `/workspace/build-default/test_countmin_compact_perf_crossover.log`,
`test_countmin_compact_comparison.log`, and
`test_countmin_compact_{04490_statistics_countmin_low_cardinality_prewhere,05182_statistics_countmin_low_cardinality_reuse}.log`.

## Intermediate 22-case implementation validation

The production threshold split and intermediate suites were validated against the
original, uninstrumented `639a67ec2f6` binary on the same ARM host. All 22 cases
completed 13 measured runs on each server (**572 executions**), with successful
warm-ups and no partial or skipped cases:

- Numeric reuse 2: **148.90 to 97.27 ms**, a **34.7%** improvement.
- Long-string reuse 2: **391.49 to 273.13 ms**, a **30.2%** improvement.
- Unique-prefix/common-suffix skew: **47.4%** faster.
- Low-reuse and retained-dictionary cases remained within 1.5% of the original.
  The NULL-heavy cases remained within 3%, with similar variation in baselines.

The build succeeded. Both `04490_statistics_countmin_low_cardinality_prewhere`
and the new `05182_statistics_countmin_low_cardinality_reuse` matched their
references. The new functional test covers nonuniform frequencies in the newly
eligible dense range and full sparse counting over a retained dictionary.

Logs: `/workspace/build-default/build_countmin_implementation.log`,
`test_implementation_04490_statistics_countmin_low_cardinality_prewhere.log`,
`test_implementation_05182_statistics_countmin_low_cardinality_reuse.log`, and
`test_countmin_implementation_perf_{crossover,retained_dictionary,distributions}.log`.

## Original calibration matrix

The measurements in the following sections used these larger, pre-trimming
suites (archived locally under `tmp/countmin-implementation/original-suites/`):

| Suite | Expanded queries | Coverage |
| --- | ---: | --- |
| `insert_statistics_countmin_crossover.xml` | 96 | 65,536 and 1,048,576 rows; reuse 1, 2, 4, 8, 10, 16; `UInt32`, 16-byte `String`, `FixedString(16)`, 128-byte `String`; permuted row order |
| `insert_statistics_countmin_retained_dictionary.xml` | 28 | 65,536 filtered rows retaining 524,289 dictionary entries; touched keys spread across that dictionary; reuse 1, 2, 4, 8, 10, 16, and three touched values; numeric and long strings |
| `insert_statistics_countmin_distributions.xml` | 126 | 6, 256, 65,536 rows; cyclic and clustered order; common-prefix/common-suffix skew; 90% NULL with mostly distinct or deliberately repeated non-NULL values; all NULL |

Half the queries disable `materialize_statistics_on_insert` to provide separate
no-statistics targets. Source construction and value conversion happen during
setup, not in the timed query. Merges are stopped. Insert squashing is disabled,
source block sizes are explicit, and the targets use `ORDER BY tuple()` to avoid
changing the tested ordering. The `reuse` substitution is a generator parameter:
rounding, skew and NULLs mean it is not always exactly `N / U`.

Untimed instrumentation at `StatisticsCountMinSketch::build` confirmed the
intended block sizes, including `N=65536`, `D=524289` in the filtered scenario.
This is important: table-wide cardinality alone would not validate these tests.

## Method

Measured on **2026-09-16**, on an ARM Neoverse-V2 host with 32 cores and 61 GiB RAM,
using the existing `/workspace/build-default` build, based on `639a67ec2f6`.
The compile command ends with `-O3` and includes `-DNDEBUG`.

A temporary, subsequently removed dispatch in `StatisticsCountMinSketch::build`
selected each process's strategy through `COUNTMIN_BENCHMARK_STRATEGY`:

| Strategy | Dense threshold | Sparse counting |
| --- | ---: | --- |
| `row` | Never | Never |
| `dense` | Always | Never |
| `sparse` | Never | Full counting, without early abort |
| `auto10` | 10 | Original `N / 10` bound |
| `auto4` | 4 | `N / 10` bound |
| `auto2` | 2 | `N / 10` bound |
| `auto2_sparse4` | 2 | `N / 4` bound |
| `auto2_sparse8` | 2 | `N / 8` bound |

Servers ran on loopback with separate data directories. Each query used one
thread. The unmodified XML framework performed a warm-up followed by 9 measured
runs per server, or 13 for the retained-dictionary and sparse-guard follow-ups,
alternating server order. The first six strategies shared one instrumented
binary; the two sparse-cutoff alternatives used a second build differing only
in the temporary dispatch. Trace output was disabled during timing.

The original calibration suites plus the selected near-unique follow-up produced **15,732
measured executions**, excluding warm-ups, smoke runs and superseded exploratory
runs. All query/server pairs had the requested sample count, without partial or
skipped cases. Comparisons used the framework's `ch_median` and
`stat_threshold`: upper median, 99th-percentile balanced-label randomization
threshold, and a minimum 5% material-change threshold. This was a local XML
framework run, not a full CI HTML-report/container run.

## Results

### Dense counting: use 2, not 10

Total `INSERT` medians for 1,048,576 rows and reuse 2, in milliseconds:

| Value type | Row-wise | Previous `auto10` | Implemented `auto2` | Improvement versus previous |
| --- | ---: | ---: | ---: | ---: |
| `UInt32` | 146.931 | 149.814 | 98.467 | 34.3% |
| 16-byte `String` | 216.968 | 222.093 | 163.553 | 26.4% |
| `FixedString(16)` | 212.391 | 217.098 | 159.470 | 26.5% |
| 128-byte `String` | 377.266 | 389.255 | 269.600 | 30.7% |

Across both block sizes and all four types, `auto2` improved total `INSERT` time
versus `auto10` by **24.8–34.3% at reuse 2**, **36.1–50.1% at reuse 4**, and
**45.1–58.2% at reuse 8**. Dense counting at reuse 1 was within 3% of row-wise:
that is insufficient evidence to recommend unconditional dense counting.

Baseline subtraction supports the same conclusion. At one million rows and
reuse 2, the incremental cost of statistics with `auto10` versus `auto2` was:

| Value type | `auto10` minus its baseline, ms | `auto2` minus its baseline, ms |
| --- | ---: | ---: |
| `UInt32` | 98.886 | 47.763 |
| 16-byte `String` | 106.532 | 48.191 |
| `FixedString(16)` | 104.949 | 47.600 |
| 128-byte `String` | 218.089 | 98.694 |

These are differences of separately measured medians, not isolated timings of
`build`; they include statistics serialization and other changed insert work.

### Sparse counting: preserve the guard

Unconditional sparse counting regressed total `INSERT` time by **23–91%** on the
compact, no-reuse cases. At reuse 2 it still regressed the million-row short-string
case by 9%. Array increments and hash-table counting should not share a threshold.

Lower sparse cutoffs also have measurable costs even though they abort early.
The 13-run near-unique follow-up gave these total `INSERT` medians for 1,048,576 rows:

| Type | Row-wise, ms | Dense 2 / sparse 10 | Dense 2 / sparse 4 | Dense 2 / sparse 8 |
| --- | ---: | ---: | ---: | ---: |
| `UInt32` | 146.858 | 148.763 (+1.3%) | 160.201 (+9.1%) | 156.117 (+6.3%) |
| 16-byte `String` | 217.486 | 222.769 (+2.4%) | 245.440 (+12.9%) | 230.860 (+6.1%) |
| `FixedString(16)` | 213.405 | 217.893 (+2.1%) | 236.679 (+10.9%) | 225.915 (+5.9%) |
| 128-byte `String` | 378.360 | 391.628 (+3.5%) | 425.260 (+12.4%) | 404.576 (+6.9%) |

Percentages are versus row-wise. The sparse-4 and sparse-8 regressions in this
table exceeded both the 5% threshold and the randomization noise threshold.

Lowering the sparse cutoff does buy gains on retained dictionaries: at reuse 4,
sparse-4 took 10.811 ms versus 12.931 ms row-wise for numeric values, and
25.068 versus 30.746 ms for long strings. However, retained low-reuse inputs also
regressed more. Even the retained sparse-10 bound has roughly 5–7% overhead on
some no-reuse cases; it is a compromise, not a no-regression guarantee.

Do not discard sparse counting altogether. With three touched values and a
524,289-entry retained dictionary, `auto2` took **3.324 versus 9.125 ms** row-wise
for numeric values, and **3.709 versus 16.539 ms** for long strings.

### Ordering, NULLs and tiny inputs

The prefix/suffix fixtures confirm that early abort is order-sensitive. A common
prefix lets the original `auto10` implementation recover substantial work before abort;
a unique prefix followed by a common suffix can miss the opportunity. Lowering
the dense threshold removes many such misses without increasing the sparse map
budget.

NULL-heavy inputs with mostly unique non-NULL values show little benefit: NULL
rows already avoid sketch updates. With repeated non-NULL values, dense counting
helps. This supports testing NULLs explicitly rather than treating all repeated
rows as saved hashing work.

The 6- and 256-row `INSERT` queries take about 2 ms, dominated by non-statistics work.
Some similarly sized differences also occur in no-statistics baselines. They do
not establish an allocation crossover or a statistically reliable tiny-row
cutoff; do not use them to justify an elaborate adaptive policy.

## Validation and limitations

- All three original calibration XML suites ran successfully through `perf.py`.
- The existing `04490_statistics_countmin_low_cardinality_prewhere` test matched
  its reference for all eight strategies and again with the restored production
  binary. This is a correctness sanity check, not exhaustive sketch equivalence.
- At the end of calibration, the temporary source changes were removed and the
  normal `clickhouse` binary was rebuilt. The subsequent production change only
  splits the thresholds; no experimental setting is shipped.
- Only this ARM host was measured; repeat on x86 before claiming a cross-platform
  optimum. The tests cover inserts, not a complete merge/materialization study.
- Small queries, shared-host scheduling and allocation/layout effects limit
  precision. Strong, repeated differences matter more than small median shifts.
- Hardware perf counters were unavailable due to host permissions. Timing still
  worked. Shared-cgroup accounting was disabled consistently for these local
  servers so each server did not count the other processes against its own limit.

## Reproduction and local evidence

Ordinary execution against a server needs no experimental code:

```sh
python tests/performance/scripts/perf.py \
  tests/performance/insert_statistics_countmin_crossover.xml \
  --host 127.0.0.1 --port 9000 --min-runs 9 --cap 9 --cap-fast 9
```

This one XML file includes all ten cases. For paired comparisons, pass both
server ports. The original calibration used six ports for the initial sweep and
eight for the final retained-dictionary run; statistical comparisons were
calculated afterward from the framework's raw `query` records.

Local experiment artifacts, not required by the XML suites:

- `/workspace/clickhouse/tmp/countmin-e2e/instrumentation.patch`: temporary
  dispatch used for forced-strategy comparisons against `639a67ec2f6`; it predates
  the production threshold split and is not intended to apply to the new source.
- `/workspace/clickhouse/tmp/countmin-e2e/start_servers.py`: process configuration.
- `/workspace/clickhouse/tmp/countmin-e2e/analyze.py`: median and noise analysis,
  reusing the framework's statistical functions.
- `/workspace/clickhouse/tmp/countmin-e2e/{crossover,retained_final,distributions_final,sparse_guard}.{json,tsv}`:
  per-query results, including baselines and noise thresholds.
- `/workspace/build-default/test_countmin_e2e_crossover.log`,
  `test_countmin_e2e_retained_dictionary_final.log`,
  `test_countmin_e2e_distributions_final.log`, and `test_countmin_sparse_guard.log`:
  raw framework output.
- `/workspace/build-default/test_countmin_shape_trace_final.log`: untimed input
  shapes; `test_countmin_correctness_*.log`: reference-test results.
