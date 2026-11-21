# AArch64 primitive tuning, 2026-11-09

This follow-up targets the four primitives that were slower than host glibc in the original PG trace measurement of the B908 provider. It preserves the B908 implementation and every unfavorable trial. The selected source changes only `memchr`; the other ten public entries retain their original implementation.

## Finite experiments

Measurements use native Neoverse V2, glibc 2.35, Clang 21.1.8 and 16-byte SVE on shared `dev`. CPUs 20–21 are used for owned work, with timing on CPU 20. Fuzzing and the other optimization sessions continue. No empty-host, statistical-dominance or whole-application claim follows.

The first wave fixes eleven variants before testing. Four original seed/stand cases with nine passes produce 1,008 library rows. The complete size/alignment/scenario matrix produces 38,646 rows. The second wave fixes five narrower designs and an unchanged compiled search control before testing, producing 728 library rows and all 29,412 matrix rows. Both matrices retain 57 lengths from zero through 4 MiB, throughput and dependency-latency regimes, and all original scenarios. Original overlapping moves use only source offsets `0`, `1`, `7`; this is not an exhaustive alignment sweep.

Each executable contains the untouched B908 provider, host glibc, noop, candidate one-slot tables and four one-slot glibc controls. Code and constructor bindings, source/object/archive identities, complete workload hashes, unique cells, explicit digests, finite timings and drained processes are independently checked. PG scoring uses the original best minimum minus same-run noop minimum, with its 0.01 floor; no row reaches the floor. Paired ratios are retained separately. Primitive improvements cannot be added to predict the mixed trace.

| Second-wave candidate | Primitive PG / B908 | Primitive PG / glibc | Mixed PG / B908 | Retained matrix finding |
|---|---:|---:|---:|---|
| `chr_align16` | 0.849684 | 1.094912 | 1.011696 | No cells above +5%; largest ratio 1.0394 across 1,368 cells |
| `move_sve4_15` | 0.875924 | 0.994283 | 0.977346 | Tiny overlap up to 3.44 times slower; exact 16-byte median ratio 1.322 |
| `move_sve8_15` | 1.028757 | 1.167768 | 1.004178 | Tiny overlap up to 3.45 times slower; exact 16-byte median ratio 1.325 |
| `move_disjoint15` | 1.025399 | 1.163957 | 1.004915 | Added selection cost and small disjoint/dependency cases up to 2.34 times slower |
| `cmp_small32` | 0.906473 | 0.981507 | 0.999978 | No cells above +5% through 32; equal 96-byte cases up to 1.338 times slower |
| `set_17_32` | 1.021969 | 1.095106 | 1.001162 | Target band improves, but 160/192 cells at 33–128 bytes exceed +5%, up to 1.54 times slower |

These are screening results, not final provider results. The mixed search gain from the first executable does not repeat in the second: paired mixed median ratio 1.00853, range 0.94694–1.01492. This adverse observation remains part of the evidence. The compiler duplicates the exact 16-byte loads/stores in two move controls despite preserving the original source tier; their pre-timing codegen receipt records that deviation. The first fill-tail proposal also did not produce the intended tail call and remains an adverse compiled control.

## Selected search implementation

The public search entry and its SVE helper use separate translation units with leaf-frame omission and compiler-generated asynchronous unwind information. The initial function pointer remains NEON before constructors run. Linux SVE availability selects the new helper once; the string-copy and concatenation dispatch policies are retained.

The helper scans a predicated prefix up to the next 16-byte boundary, then aligned groups. Every group has at most sixteen active bytes, including when the thread uses larger SVE vectors. This removes the old per-entry vector-width wrapper and redundant framed calls. Groups do not cross a page boundary; an oversized count can return an early match before a protected page. Zero-length searches access no bytes, and the character is converted to an unsigned byte.

The other designs are rejected as general replacements because their coherent adverse bands remain. The changed source does not silently substitute glibc for those entries.

## Validation scope

The scratch candidates pass strict native and non-SVE validation, protected-page and read-only checks, pre-constructor execution, zero lengths, integer-character truncation, oversized early-match counts and additional emulated vector lengths. Both the 168-byte trial helper and the 188-byte actual CMake helper pass a bounded machine-code model with stopping offset at most 64, including arbitrary counts with a mandatory match at offsets 0 through 64. The model uses untagged 48-bit addresses and 4 KiB pages at VL16. A wrong-return-offset machine-code mutation produces a concrete counterexample. This is not an unbounded certificate, router-state proof or whole-library guarantee.

An unbounded attempt fails its intermediate frontier invariant. Checked diagnostics identify a compiler-peeled aligned block loaded before the loop header; the generic detector treats its capped symbolic stride as variable, chooses step one and a page-before-frontier invariant. Three auxiliary solver checks establish the concrete continuation stride and aligned look-ahead bound, but do not prove all loop obligations. The original failed attempt is preserved. A separate checked-stride proof-search experiment also fails after state generalization; it is not an accepted proof or a concrete native counterexample. No instruction semantics or safety obligation is waived.

## Actual provider and separate caller

The selected source is compiled through the real provider CMake recipe with the captured production options, including 64-byte function and 32-byte loop alignment. The prefixed archive SHA-256 is `4645b46075009efff1ec5828ace943e38f1770b508464ca03b0ee4590958b608`; the public archive is `7da674f509fe7fda2204f6d2b88f4e31931b52b6ec1bb87bd6b48cdb6156f426`. The same-process PG executable is `f3b05f0551a598e60a97d303eb1caba23126a5a8ef15675181d12785802e0771`. The preserved B908 archive is renamed by symbol names only, with unchanged code/data/relocation/unwind payloads and external imports.

All ten unchanged public bodies and 42 total functions match B908 after resolving logical relocation targets. Eleven unchanged translation units retain exact unwind payloads. The combined harness passes 33 strict native and 33 non-SVE validations (11 selected, 11 B908 and 11 glibc kernels), five full API modes and five two-provider search modes. The new public entry is 12 bytes and the SVE helper 188 bytes; all 188 bytes match the exact object, bounded proof ELF and measured PG executable. Production loop alignment adds five NOPs relative to the trial helper. The bounded proof and rejected return-offset mutation are repeated on the actual object. The formal model accepts only VL16; a requested VL32 run is rejected before proof execution, and no VL48 proof is claimed. Wider-vector runtime tests remain separate.

The actual-provider matrix includes all 24,624 cells for B908, selected provider and glibc. Search again has no observed cell above +5%, with a maximum ratio of 1.0363. Unchanged bodies do not imply equal measured timing: the retained selected-provider controls have 95/2,394 move, 12/3,078 comparison and 536/1,368 fill cells above +5%. All 384 fill cells at 17–32 bytes are slower in both minima and medians, with median ratio 1.19859 and range 1.17366–1.38678. The original thread-CPU timer, roughly 250-microsecond sample batching and shuffled sample order are retained. Code/helper address offsets differ. Layout, predictor and shared-host effects remain hypotheses; no causal attribution or whole-archive performance equivalence is asserted.

A separate diagnostic uses the real compiled `DB::skipToNextRowOrEof` and delimiter search through `ReadHelpers`, with the other ten provider slots held common. The fixed 8,410,842-byte corpus has 16,384 records cycling lengths 1, 7, 15, 16, 31, 63, 127, 255 and 4,095, followed by a 13-byte unmatched tail. Each mode makes exactly 16,385 searches and validates every consumed position and the independent input checksum. Actual linked assembly and probes establish the path through the common wrapper to independent host glibc, B908 or the two exact new search objects.

| Caller mode | Median wall time, nine rotated rounds |
|---|---:|
| Host glibc | 0.466117 ms |
| B908 | 2.418532 ms |
| Selected search | 0.928020 ms |

The selected caller has 61.63% lower median time than B908; all nine paired ratios lie between 0.379586 and 0.385489. It still takes 1.990959 times glibc's time on this explicit synthetic corpus. These wall timings are separate from PG's original timer and score, and do not represent the original SQL distribution or application throughput. One setup attempt stopped before compilation on an old captured archive pin; the reviewed continuation changes only that pin to the preserved B908 runtime. Source, corpus, other link inputs and all 27 scheduled timings remain fixed.

Earlier B908 ClickHouse query results remain separate; this kernel experiment does not rerun the complete application suite.


## Fresh PG confirmation

After selection, eight frozen cases use seeds 3 and 4, stands 0 and 3, two oppositely ordered cycles and nine passes per case. All 448 rows, 14 traces and 264 strict kernel validations pass; all requested cases are retained, with no floor hits. The unchanged upstream scorer reproduces all 42 best net times. No screening or older laboratory results enter these scores.

| Trace | Selected / glibc PG | B908 / glibc PG | Selected / B908 PG |
|---|---:|---:|---:|
| `mixed` | 0.769029 | 0.783616 | 0.981385 |
| `memchr` | 1.082476 | 1.261830 | 0.857862 |
| `memcpy` | 0.896985 | 0.898561 | 0.998247 |
| `memmove` | 1.147116 | 1.149546 | 0.997886 |
| `memcmp` | 1.098905 | 1.097309 | 1.001454 |
| `memset` | 1.094857 | 1.085827 | 1.008316 |
| `strlen` | 0.813953 | 0.809798 | 1.005131 |
| `strcpy` | 0.582116 | 0.585498 | 0.994224 |
| `strcat` | 0.626376 | 0.627833 | 0.997678 |
| `strcmp` | 0.406914 | 0.409179 | 0.994464 |
| `strspn` | 0.241560 | 0.241752 | 0.999208 |
| `strcspn` | 0.788223 | 0.784952 | 1.004167 |
| `strspn_long` | 0.442615 | 0.442971 | 0.999195 |
| `strcspn_long` | 0.472144 | 0.473549 | 0.997033 |

The original PG mixed score is 0.769029 versus glibc: 23.097% lower calibrated net time, equivalent to about 30.03% higher reciprocal net rate. This reciprocal is a conversion of the benchmark ratio, not measured application throughput. The same-executable B908 score is 0.783616. The selected/B908 independent-best ratio is 0.981385, but the paired mixed net ratios have median 1.006219 and range 0.964564–1.028929. A consistent mixed improvement over B908 is therefore not established. Gross per-run median timing uses a different statistic: its paired selected/glibc median ratio is 0.864737, and selected/B908 is 0.993622.

The changed `memchr` trace is consistently faster than B908 across all eight cases: original PG ratio 0.857862 (14.214% less net time), paired median 0.853499 with range 0.846165–0.865879. It still has a PG ratio of 1.082476 versus glibc. The untouched move, comparison and fill slots also remain slower than glibc. These results select a search improvement while retaining the remaining optimization targets and all contradictory mixed/control observations.

All owned native measurement controllers have exited. Original and selected archives, measured executable and glibc identities remain unchanged at postflight; the pause flag is absent. This task does not change services, aliases or fuzzer controls. Raw heldout evidence archive SHA-256 is `1eb2a381fa03012fccaccac13ced37547c1d22a848506d04e0ba330d232d7643`; actual-caller timing evidence is `e27a6d4d58767793222777b3b0eff155573fec57dbdf66bac4be15b3800d1dad`. Native artifacts remain under `build_perf_full/regressions-2026-11-08/pg-slow-primitives-2026-11-09` in the preserved integration run. Local raw data, protocols and independent receipts are indexed by `tmp/pg83-str/integration-support/pg-slow-primitives-2026-11-09/STATE.md`.
