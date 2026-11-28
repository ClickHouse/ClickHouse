# AArch64 regression follow-up, 2026-11-08

All 19 suites containing the 56 displayed slowdown candidates from the original broad comparison were rerun, including every enabled query in each suite. This is 301 queries, measured in both endpoint orders with nine samples per side: 602 comparisons and 10,836 timing samples. All saved row-count/result-digest pairs match. The user excluded TPC families. This targeted follow-up is not a new whole-suite or application aggregate.

## Build and measurement identities

The native host is Linux AArch64 Neoverse V2, Clang 21.1.8, glibc 2.35, with 16-byte SVE vectors. Payload/control work uses CPUs 20–21. Fuzzing and other optimization sessions continue on the shared host; no empty-server or per-query foreign-load exclusion is claimed.

Both baseline and candidate retain master `0ba40ae088e975ca4b1cb36236db8c39d1ddf037`, original full-feature build settings and preserved nonprovider link inputs. The candidate changes the experimental provider archive. The generated helper-directory counter is the sole permitted build-metadata difference. The option remains disabled by default.

| Identity | Baseline | Candidate |
|---|---|---|
| Build ID | `844C8C552EE0D1BC9921CF97996A1CC1FFD2C2CA` | `B9080B220E8BC7AEA5B9727A913E13086D2E77B9` |
| Binary SHA-256 | `8068cc40fa8b967208024fc39e8d36fcdace0b49e1bbbff9e3bf3ef3b9563acc` | `b2b40e88feebd59b1cb07fe85f20b1878da769960d942cde36225289a890b417` |

The candidate public archive SHA-256 is `00ee793414dc66abb4ca1fb9998a139f55cdd133ebea65be6a38d72180115976`. The collected main-cohort evidence archive SHA-256 is `c85d7497ba81cea58295a6770fa66bab74af7b8863b31bcf964375531f48d821`.

Original timed SQL, settings and scale are retained. `parse_engine_file` and `sparse_column` are generated once through original baseline setup, with identical immutable parts registered independently under candidate UUIDs. File inputs share physical files and page cache. The format-suite adapter extends only prewarm to 180 seconds by removing the XML root override; the original 60-second timed-statement budget remains. Failed attempts and the earlier `21150DDAAA53A58AAF97D2ED50634F7366A9E0B4` cohort remain preserved separately, without pooling.

## Measured portfolio

Positive percentages mean longer candidate runtime. Ranges below include every query and both endpoint orders; they are descriptive, including noisy extremes. Each slower count uses the ordinary CI change-failure gate with a 15% floor. Each unstable count uses its 25% failure floor. Exact balanced median permutation quantiles replace sampled quantiles; unavailable historical adaptive thresholds are not invented. The ordinary CI renderer preserves its conservative numerical flooring, while this note rounds raw medians normally.

| Suite | Queries per order | Raw runtime range | Slower, orders 1 / 2 | Unstable, orders 1 / 2 |
|---|---:|---:|---:|---:|
| `bit_positions_to_array_big_int` | 2 | -1.89% to +0.30% | 0 / 0 | 0 / 0 |
| `codecs_float_select` | 28 | -12.21% to +9.40% | 0 / 0 | 0 / 3 |
| `parquet_read` | 6 | -12.83% to +31.43% | 0 / 0 | 3 / 2 |
| `parquet_read_with_index` | 6 | -30.66% to +60.11% | 0 / 0 | 3 / 3 |
| `group_by_multiple_strings` | 30 | -41.76% to +64.80% | 0 / 1 | 19 / 16 |
| `short_circuit_functions` | 22 | -26.01% to +16.70% | 0 / 0 | 3 / 5 |
| `length_utf8_long_strings` | 2 | -7.51% to +8.61% | 0 / 0 | 2 / 0 |
| `enum16_dense` | 2 | -1.98% to +45.66% | 0 / 0 | 0 / 1 |
| `decimal_to_string` | 11 | -5.20% to +7.85% | 0 / 0 | 0 / 2 |
| `array_reverse_generic` | 8 | -9.69% to +24.75% | 0 / 0 | 3 / 1 |
| `min_max_wide_integers` | 15 | -9.20% to +15.72% | 0 / 0 | 7 / 3 |
| `window_functions_sliding_frames` | 13 | -5.46% to +18.68% | 0 / 1 | 2 / 0 |
| `replace_string_fast_path` | 5 | -5.29% to +15.10% | 0 / 0 | 2 / 3 |
| `json_string_value_formatting` | 6 | -14.58% to +8.23% | 0 / 0 | 0 / 0 |
| `external_distinct_header_constants` | 9 | -12.09% to +54.37% | 0 / 0 | 1 / 5 |
| `sparse_column` | 90 | -3.92% to +3.65% | 0 / 0 | 0 / 0 |
| `formats_hits_read` | 19 | -1.83% to +4.85% | 0 / 0 | 0 / 0 |
| `parse_engine_file` | 17 | -4.36% to +3.07% | 0 / 0 | 0 / 0 |
| `hierarchical_dictionaries` | 10 | -5.71% to +5.69% | 0 / 0 | 0 / 0 |

The original UInt256 query moves from roughly 42.5% slower in the broad run to 0.21% and 1.30% faster in the two current orders. FPC decoding moves from roughly 26.5% slower to 1.27% and 2.22% faster; the second order still has 21.56% q99 noise and an unstable-show flag, so this does not establish tight equivalence. Hierarchical descendants range from 5.71% faster to 0.94% slower across both orders, without change or instability flags. The earlier 54–60% slowdown is absent in this cohort. ArrowStream queries 10 and 11 are within 0.49% of baseline; parsing query 15 is +2.33% / −1.94%.

Important adverse evidence remains. Sliding-window `quantilesExact` query 2 is +10.80% / +15.14%, with q99 noise 26.59% / 15.14%; the first order is unstable and the second crosses the change-failure gate. Multi-string grouping query 28 is +0.26% / +17.73%, crossing the change-failure gate only in the second order. Several short-circuit and other grouping cases remain slower or uncertain. No query crosses the change-failure gate in both orders. Parquet has substantial order reversals and unstable flags. None of those cases is treated as fixed merely because a CI change gate fails to reject. A separate, fixed 121-sample Parquet cohort is recorded independently below.

A bounded historical `system.query_log` lookup recovers all 36 timed `QueryFinish` records for window query 2, without rerunning the workload. Both sides read 1,007,616 rows, 8,060,928 bytes and 123 marks, but the baseline selects one part/range while the candidate selects two, in both orders. Median virtual CPU time is approximately 1.05 seconds versus 1.24–1.25 seconds; candidate CPU-wait time is lower. This establishes a physical-layout confound in the original independently generated fixture. This lookup records no part-name or content hashes; equal read counts and bytes do not prove logical input identity. It does not establish that layout explains the entire slowdown, that `str` is responsible, or that shared-host interference is absent. The measured adverse result remains in the report.

## Separate Parquet repeat

After auditing the main cohort, a separate protocol fixes all 12 original Parquet queries, both endpoint orders and 121 samples per side. It preserves the same binaries, original SQL/settings/full scale and immutable files. These observations are not pooled with the nine-sample main cohort. Larger samples do not guarantee power, independence or freedom from shared-host interference.

The first controller completed both normal-Parquet orders, then failed its post-suite zero-merge guard before analysis or indexed-Parquet timing. The merge rows were not captured, so the historical table and scope are unknown; timing contamination is not excluded. The failed outer state remains unchanged. Its complete normal-Parquet output is independently hash-verified and analyzed from a new copy: six queries, 12 comparisons and 2,904 positive finite timings, with matching digests throughout. Raw differences span −2.22% to +0.96%, with q99 noise 0.83–1.77% and no change or instability flags. Those are retained measurements, not a successful verdict for the failed controller.

An explicit indexed-only continuation preserves the original absolute deadline and the same zero-query/merge/mutation criterion, with recorded bounded idle checks. It completes the remaining six queries, 12 comparisons and 2,904 timings without rerunning any successful normal-Parquet query. Together the two recorded origins cover the registered 12 queries, 24 comparisons and 5,808 timings. All digests match. Across all 24 comparisons, raw differences span −2.22% to +0.96% and q99 noise 0.82–1.88%, with no change or instability flags. Indexed queries 0, 2 and 3 measure −0.45% / −0.66%, +0.18% / −0.98%, and +0.63% / −1.43%, respectively. The large initial order reversals do not recur; the historical failed boundary and unknown merge interference remain part of the evidence. These observations do not establish equivalence or causality.

## Kernel investigations and selected changes

The [hierarchical dictionary investigation](hierarchy-2026-11-08.md) previously minimized repeated `HashSet` clears to 1 MiB zero fills and selected bounded `DC ZVA` zeroing. Its historical binary and measurements remain distinct from this later candidate.

The compiled FPC decoder and compiled Parquet byte-array decoder were exercised with complete output validation and independent host-libc comparisons. Tiny predicated SVE copies improve these minimized callers by approximately 29–30% and 42%, respectively, against the previous provider. The previous provider is near glibc in both minimizers, so those results do not alone explain the integrated slowdowns. The selected helper touches only active bytes, uses at most 16 lanes and is selected after Linux SVE feature initialization; pre-constructor calls retain NEON. The exact 16-byte NEON specialization precedes that check.

The actual `UInt256` shift expression generates one 24-byte fill per row. Isolating only the public fill leaf-frame policy improves all 18 paired runs by 7.9–11.4%, with a median improvement of 9.1%; its median is within 0.3% of glibc. A 48-cell broad control screen retains one adverse aggregate cell around 8.7% versus the previous public entry. More elaborate public short-fill tiers were rejected after mixed-size and fallback costs were measured. The selected change preserves compiler asynchronous unwind information and the bulk fill body.

For 512-byte string serialization, unconditional source alignment improves one layout and substantially harms another. The selected policy changes alignment only when a 16-byte-aligned source removes an entire redundant 128-byte loop iteration, and only through 1024 bytes. A balanced 816-case matrix contains 275,808 samples; its worst independent-cell median regression is 3.94%, while all six paired caller layouts improve. Earlier adverse policies and failed attempts are preserved. The overlap-sensitive move kernel retains its separate directional implementation.

## Correctness and proof limits

All eleven public APIs pass differential contracts on native SVE and forced NEON, plus emulated non-SVE and 32-/48-byte SVE vector lengths. Preinitialization, protected pages, overlaps, unchanged surrounding bytes and oversized early-match `memchr` are exercised. Runtime traces of one untimed validation workload include 703 weighted samples with public `memcpy` or its tail-called SVE helper and `FunctionRepeat` below it. All 30 validation queries per side report zero query-local profiler errors; this does not establish universal unwind coverage. These checks are targeted evidence, not universal correctness claims.

The new exact object/archive/linked-byte snapshot passes bounded proofs of the tiny SVE helper through 16 bytes and public `memset` through 128 bytes, with concrete counterexamples for deliberate instruction mutations. The tiny proof models 16-byte SVE. A separate public-copy proof through 256 bytes applies only to zero-initialized pre-constructor state. Caller buffers must be separate from code and provider-private globals. Private-global non-aliasing is an external assumption, not an SMT-encoded obligation in that pre-constructor experiment. The post-constructor public-copy router, feature constructor, longer kernels and complete executable are outside those certificates. Earlier proof snapshots are not silently reused for changed code.

Matching prewarm digests compare row counts and normalized, order-insensitive hashes. They do not prove result ordering, bitwise floating equivalence or correctness on all inputs. The seven original broad-run mismatch suites remain separately documented; this follow-up does not exonerate them. Shared-host noise, query-time random inputs and shared page cache limit inference. There is no familywise statistical guarantee or claim of performance equivalence from absence of CI flags.
