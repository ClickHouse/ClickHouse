# AArch64 hierarchical dictionary regression, 2026-11-08

`hierarchical_dictionaries.xml` reproduces a large `dictGetDescendants` regression with the original experimental provider. A bounded `DC ZVA` zero-fill path removes that regression. The option remains disabled by default.

## Setup

The native host is Linux AArch64 Neoverse V2 with Clang 21.1.8, glibc 2.35, 64-byte zeroing blocks and 16-byte SVE vectors. Both servers and the benchmark use CPUs 20–21 on the shared host; fuzzing and other optimization sessions continue. These targeted comparisons do not establish a whole-suite or production-wide effect.

The baseline and original candidate are preserved full-feature binaries from master `0ba40ae088e975ca4b1cb36236db8c39d1ddf037`, differing only in `ENABLE_PG83_STR`. The fixed binary reuses their compiled runtime objects and Ninja's exact expanded link command, with the changed provider archive. Its compiler, feature and runtime CMake options match the original candidate. The generated helper-directory counter is the only permitted metadata difference. Original binaries are unchanged.

| Binary | Build ID |
|---|---|
| Baseline | `844C8C552EE0D1BC9921CF97996A1CC1FFD2C2CA` |
| Original provider | `BF87BE0A6E039EE5DA40E069FCB2E303D613DFD4` |
| Fixed provider | `6DB7C24015C82233D4A4DC622C4DBD3E2B5912D3` |

The original create/fill/query/settings definitions are retained. Only teardown statements are removed in a generated adapter so the same owned fixtures can be reused. Each original and fixed comparison has two rounds with reversed endpoint order and nine samples per side per query. The source tables each contain 1,000,000 rows and share the same commutative row hash, `14586736518224588812`. All measured result digests match within and across these valid rounds. Invalid launcher attempts are retained and excluded; the accepted fixed run asserts the exact server build IDs before timing.

## Attribution and minimization

`getDescendants` reserves a `HashSet<UInt64>` for a query block and clears its entire buffer after each non-leaf request, including already-empty sets. Source analysis predicts a 1 MiB buffer and 50,000 clears for this fixture: 48.8 GiB of zeroing per query. Candidate profiles place 3,057/3,647 CPU leaf samples (83.8%) in `kl_memset_neon_bulk128`. Other major caller sample counts closely match the baseline. ClickHouse clamps the requested 100 µs profiler period to 1 ms.

A standalone repeated 1 MiB zero fill measures 11.713 µs for the original provider and 6.283 µs for host libc. The 5.430 µs gap multiplied by 50,000 predicts 0.2715 seconds, matching the observed flat descendants penalty of 0.2714 seconds. The final patched provider measures approximately 6.283 µs for an aligned 1 MiB fill. Baseline traces have leaf address zero while unwinding the frameless libc routine; that value is not a sampled instruction address that can be symbolized.

Three zeroing-loop variants, with one-, four- and eight-block unrolling, were compared over zero/nonzero values and aligned/unaligned addresses. Four-block unrolling reaches the same throughput as eight-block unrolling at 1 MiB. The size sweep finds zero-by-address beneficial through 3 MiB but slower than NEON at 4 MiB and above. The actual production entry-point sweep then found an unaligned 256-byte regression, despite gains in the isolated prototypes. The final lower bound is therefore 512 bytes; existing tests cover 511/512/513-byte boundaries. A conservative 2 MiB upper bound retains the previous streaming path. Sizes at most 128 bytes keep the previous path without a new zeroing-eligibility branch. The final sweep removes the 25% unaligned 256-byte regression. It still observes about 5–8% slower unaligned 512-byte fills, alongside a roughly 28% aligned 512-byte gain; improvement is not uniform across every size and alignment.

The actual `HashSet` caller was also compiled with the repository C++ expression helper. It confirms 131,072 cells of 8 bytes, or exactly 1,048,576 buffer bytes. A reduced sequence inserts ten keys for 5,000 iterations and then performs 45,000 empty clears, checking emptiness, zero-key removal and unchanged capacity. Across five alternating rounds in the same executable, median times are 0.587680 s for the preserved provider, 0.325627 s for host libc and 0.324012 s for the fixed provider. This independently reproduces the bottleneck without dictionaries or SQL. This caller run used the first zero-fill revision; the final lower-cutoff adjustment retains the same selected 1 MiB loop, with the final public entry point and complete XML retested separately.

## Original test results

All percentages below are candidate runtime divided by the paired baseline runtime, minus one. Endpoint order is normalized back to logical baseline/candidate in round 2.

| Query | Layout | Original round 1 | Original round 2 | Fixed round 1 | Fixed round 2 |
|---|---|---:|---:|---:|---:|
| 1 | `flat` | +57.10% | +54.53% | -0.47% | -0.06% |
| 3 | `hashed, 1 shard` | +53.62% | +53.21% | +0.31% | -3.67% |
| 5 | `hashed, 16 shards` | +54.79% | +47.36% | -0.31% | -0.19% |
| 7 | `hashed_array, 1 shard` | +52.41% | +51.18% | +0.62% | -1.52% |
| 9 | `hashed_array, 16 shards` | +56.80% | +50.34% | -3.37% | -1.34% |

All five descendants comparisons move from 47.36–57.10% slower to between 3.67% faster and 0.62% slower than baseline. No query crosses CI's 15% change-failure threshold or 25% instability-failure threshold in either fixed round.

The flat `dictGetHierarchy` control remains 7.33% and 7.74% slower in the two fixed rounds, versus 5.50% and 5.19% with the original provider. It remains below CI's lower changed-show threshold and is not claimed fixed. The other hierarchy controls are within about 2.9% of baseline. These limits must be retained when interpreting the targeted improvement.

## Correctness and proof scope

All eleven public entry points pass native and forced-NEON differential contracts. The contract suite adds 560 large-fill cases, covering the 256-byte, 512-byte and 2 MiB boundaries, unaligned starts, protected end pages, values 0/256/-256/127/-1, original-pointer returns and unchanged bytes outside the fill range. Separately, the three prototype variants passed 688,296 guarded cases.

The new `DC ZVA` path checks `DCZID_EL0` permission and block size, requires at least two blocks, and writes only complete aligned blocks inside the requested range. Exact stores handle the prefix and suffix. The new longer path is outside the earlier machine-code proof snapshots; no whole-provider or whole-ClickHouse correctness proof is claimed.

The private comparison services are stopped and their original binary aliases restored after measurement. The fuzzer is never paused. Raw timings, profiles, failed attempts, object archives, source identities and audit receipts are retained in the task's `hierarchy-2026-11-08` artifacts.
