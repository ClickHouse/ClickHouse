# Experimental string and memory functions

`ENABLE_PG83_STR=ON` replaces the libc entry points for `memcpy`, `memmove`,
`memset`, `memcmp`, `strlen`, `strcpy`, `strcat`, `strcmp`, `strspn`, `strcspn`,
and `memchr` with the [ClickHouse fork of pg83/str](https://github.com/ClickHouse/pg83str).
The option defaults to on for Linux x86-64 and AArch64 without sanitizer,
coverage, fuzzing or PGO-generation instrumentation, so release performance CI exercises the
provider. It defaults to off elsewhere; explicitly enabling it on an unsupported
configuration is an error. Set `ENABLE_PG83_STR=OFF` to restore the previous
providers. Other LLVM libc functions,
including `bcmp`, `memmem`, and the math functions, keep their existing providers.

The submodule pins a commit from `ClickHouse/pg83str`. The fork retains upstream
base `21ad0a33534c63096e2d2ee879a9f435cd474a3c` and adds the ARM workbench and
selected provider. The implementation and its MIT license live in the library
fork; this directory contains the ClickHouse build integration and benchmark
notes.

Each translation unit is compiled for its own CPU level. The dispatcher and
SSE2 implementation retain the x86-64 baseline. Upstream selects the appropriate
configuration at runtime, with an immutable SSE2 table available before constructors.
Public entries acquire an atomic pointer to the selected immutable table;
initialization publishes that pointer instead of overwriting live function slots.

The upstream Python post-pass is required to produce a linkable archive. It
specializes implementations for each dispatch configuration and rewrites their
machine code. This integration builds PIC objects for ClickHouse's PIE links,
disables LTO for these objects, and renames the public symbols after the pass
so that the pass still recognizes its internal calls. The archive is loaded
before the LLVM libc archive, whose overlapping memory definitions are weak.

The pass regenerates `.eh_frame` rows for the final instruction layout,
including configuration copies and duplicated tails. It checks preserved compiler
frame effects and the final control-flow graph, and rejects unsupported unwind
instructions or inconsistent frame states. Functions retain asynchronous unwind
information. The compiler may omit leaf frames; nonleaf frame pointers remain,
and the post-pass frame-removal optimization stays disabled. Source-level debug
information is disabled because the pass does not
remap it to the rewritten code. Function symbols remain available. Native
profiling must also verify stacks through the actual linked provider.

Archive generation round-trips the actual x86 compiler objects before applying
the rewriting passes. On target-native Linux builds it also runs contracts
against the actual prefixed archive and independent host libc before publishing
the public archive. The x86 checks cover every executable ISA configuration,
one-slot mixes, cached-copy/fill thresholds and overlapping ranges, and
publication with an early constructor worker. ARM checks run
both the selected hardware path and forced NEON. Cross builds explicitly skip
execution; they do not report a native contract pass. Logs and the archive hash
are retained below the build directory's `contract-tests` directory.

## Experimental AArch64 provider

The selected AArch64 implementation lives in
`contrib/str/aarch64/selected/`, with its source hashes and history in
`provenance.json`. ClickHouse compiles those submodule sources directly.
The library retains its separate historical 20-slot development API and
workbench; the selected provider exposes the eleven libc-shaped entry points.

The ARM objects use the compiler output directly, retaining unwind information.
Most entries retain frame pointers. The leaf public `memcpy`, `memmove`,
`memset`, and `memchr` entries and their small SVE helpers omit their frames
and retain compiler-generated asynchronous unwind tables; bulk kernels,
other entries, and nonleaf constructors keep their frames. Baseline
objects use `-march=armv8-a`; separate SVE objects use `-march=armv8-a+sve`
and scalable vector length. Valid NEON implementations are available before
constructors run. Constructors check Linux `AT_HWCAP` and atomically publish
SVE availability. The bounded tiny-copy and search helpers predicate every
memory access and only use the first 16 byte lanes. Other SVE wrappers retain their tuned
16-byte vector-length gate and use NEON outside it. The library changes no
process or thread vector-length setting.

The public `memcpy` entry uses one predicated SVE load/store for lengths
below 16 on SVE hosts after initialization. Its exact 16-byte NEON load/store
stays before the feature check. The NEON path handles other short copies and
pre-constructor calls. `memmove` retains its bounded NEON path through 128
bytes, loading every source byte before the first store. Larger copies and
moves call their separate NEON kernels directly. `memset` handles up to 16
bytes inline and uses NEON above that boundary.
The search helper scans a bounded prefix to a 16-byte boundary, followed by
aligned groups of at most 16 bytes. It supports larger per-thread vector
lengths without the old search wrapper, while preserving protected-page
early matches for oversized counts.
The copy kernel also has bounded tiers through 192 and 256 bytes and avoids
recopying a final 128-byte block already covered by its tail. Through 1024
bytes, it selects source alignment only when the source is 16-byte aligned and
that phase removes an entire redundant 128-byte loop iteration. Other copies
retain destination alignment. The move kernel retains its separate overlap
and direction handling.

Zero fills from 512 bytes through 2 MiB use `DC ZVA` when `DCZID_EL0`
permits it and the range contains at least two zeroing blocks. The prefix
and suffix use exact stores; zeroing instructions touch only complete aligned
blocks inside the requested range. The byte conversion is applied before
testing the fill value, including values such as 256 and -256. Larger fills
retain NEON stores because the native Neoverse V2 size sweep found a crossover
between 3 MiB and 4 MiB. The conservative 2 MiB limit retains the existing
streaming path. This addresses repeated hash-table clears in
`dictGetDescendants`; it does not change the dictionary implementation.
The new zeroing path has native guarded differential tests and is outside the
earlier machine-code proof snapshots.

The overlapping strong definitions are excluded from the existing Arm
optimized-routines archive only while this option is enabled. Its `strchr`
and `strnlen` remain in use. LLVM libc math and other memory routines retain
their existing providers. Sanitizer, coverage, fuzzing and PGO-generation builds default the option to off and reject
an explicit enablement so their interceptors and instrumentation retain the
existing providers.

The workbench has partial machine-code proof coverage. The preserved native
`21150DDAAA53A58AAF97D2ED50634F7366A9E0B4` object snapshot passed bounded
proofs of `memcpy` through 256 bytes and `memmove` and `memset` through 128
bytes, within an explicit 48-bit-address, 16-byte-SVE model. The proof domain
assumes caller buffers are separate from code and provider-private globals.
Seven deliberately mutated instruction cases produced counterexamples. The subsequent native
`B9080B220E8BC7AEA5B9727A913E13086D2E77B9` snapshot passed fresh bounded
proofs of the tiny SVE helper through 16 bytes and the public `memset` entry
through 128 bytes, bound to its compiler objects, archives and linked function
bytes. The helper proof uses a 16-byte SVE model; native and emulated API tests
also exercise NEON and additional vector lengths. These checks do not certify
the changed public copy router or its constructor state. Constructors, longer
paths and the complete ClickHouse executable remain outside those proofs.
The provider remains experimental. Its default enablement in eligible builds
is intended to obtain ordinary CI correctness and performance comparisons;
the bounded proofs do not certify the entire library.

See the [2026-11-07 native AArch64 comparison](benchmarks/aarch64-2026-11-07.md)
for the build configuration, query results, and limits of the measurements.
The [2026-11-08 hierarchical dictionary investigation](benchmarks/hierarchy-2026-11-08.md)
records the repeated regression, zero-fill minimization, fix, and remaining
flat-hierarchy control difference.
The [2026-11-08 regression portfolio](benchmarks/regressions-2026-11-08.md)
records all 19 affected suites on the later candidate, minimized caller
experiments, retained timing concerns and exact proof limits.
The [2026-11-09 primitive tuning](benchmarks/primitive-tuning-2026-11-09.md)
records the selected search change and the retained adverse move, comparison
and fill trials.

On a native AArch64 glibc host, test the prefixed archive against the independent
host libc, including protected pages and oversized early-match `memchr`:

```sh
clang -O2 -UNDEBUG -fno-builtin contrib/str/tst/aarch64_selected.c \
    build_candidate/contrib/str-cmake/libstr-prefixed.a \
    -o build_candidate/str-contract-test
build_candidate/str-contract-test > build_candidate/test_str_contract.log 2>&1
clang -O2 -UNDEBUG -fno-builtin -DSTR_TEST_NO_SVE -Wl,--wrap=getauxval \
    contrib/str/tst/aarch64_selected.c \
    build_candidate/contrib/str-cmake/libstr-prefixed.a \
    -o build_candidate/str-contract-neon-test
build_candidate/str-contract-neon-test > build_candidate/test_str_contract_neon.log 2>&1
```

The second test masks the SVE feature in the archive's feature-detection call
and exercises its actual NEON dispatch path. The test also calls the provider
from `.preinit_array`, before constructors run. An optional numeric argument to
the test requests that SVE vector length with `prctl`; unavailable lengths
return status 77. Test other vector lengths on supporting hardware or an
emulator for correctness; use native hardware for performance measurements.

## Validation and CI comparison

Build two release binaries from the same revision and toolchain, changing only
`ENABLE_PG83_STR`. Preserve each binary and its CMake cache in a separate
directory, and use isolated server data directories. Separate build directories
work, as does preserving the baseline before toggling the option and reusing
the same compiled objects. Keep LLVM libc enabled in both builds so the baseline uses the
current ClickHouse implementations rather than the system libc.

Keep ICU and jemalloc enabled for the UTF-8 tests and the normal allocator.
`ENABLE_LIBRARIES=OFF` disables both unless `ENABLE_ICU=ON` and
`ENABLE_JEMALLOC=ON` are supplied explicitly. Without ICU, `lowerUTF8` and
`upperUTF8` are absent, so their performance tests cannot run.

On x86-64, run upstream's full correctness suite against the prefixed archive retained at
`contrib/str-cmake/libstr-prefixed.a` in the candidate build directory. It uses
the `str_` entry points and can therefore compare against an independent libc.
Use `-fno-builtin` for the test. Exercise AVX-512 on hardware that supports it,
as well as the baseline and AVX2 configurations. Run the decoder/emitter
round-trip checks for the compiler used in the build.

For example, from the repository root with Clang on the native x86-64 dev host:

```sh
clang -O3 -fno-builtin -Icontrib/str contrib/str/tst/test.c \
    build_candidate/contrib/str-cmake/libstr-prefixed.a \
    -o build_candidate/str-upstream-test
build_candidate/str-upstream-test > build_candidate/test_str_upstream.log 2>&1
CC=clang python3 contrib/str/tst/roundtrip.py \
    --out build_candidate/str-roundtrip > build_candidate/test_str_roundtrip.log 2>&1
```

Compare existing ClickHouse performance tests with `perf.py`, using at least
seven runs per query and repeating material differences. Include
`string_deserialization.xml`, `substring_identity.xml`,
`group_by_string_key_count.xml`, `string_join.xml`, and `array_concat_single.xml`,
plus broader workloads to detect changes beyond string functions. Record CPU,
build revision, compiler, settings, binary paths, and old/new timings. The
upstream synthetic results alone do not establish a ClickHouse improvement.

Eligible release CI builds use the provider by default; reference binaries
from master retain their own default. Use the `pr-performance` label to request
both AMD and ARM performance jobs. Normal CI query limits and long-test
exclusions still apply; this does not turn a PR run into a complete local
suite run. Performance under CPU emulation is not usable timing evidence.

The dated benchmark notes preserve the build settings used for each historical
measurement, including their then-disabled default. Their results predate this
submodule migration and current-master CI comparison.
