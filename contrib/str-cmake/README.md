# Experimental string and memory functions

`ENABLE_PG83_STR=ON` replaces the libc entry points for `memcpy`, `memmove`,
`memset`, `memcmp`, `strlen`, `strcpy`, `strcat`, `strcmp`, `strspn`, `strcspn`,
and `memchr` with [pg83/str](https://github.com/pg83/str). The option is off by
default and requires Linux x86-64 or AArch64 without sanitizers. Other LLVM libc functions,
including `bcmp`, `memmem`, and the math functions, keep their existing providers.

The submodule pins upstream commit `21ad0a33534c63096e2d2ee879a9f435cd474a3c`.
Upstream is MIT licensed. No implementation changes are made in the submodule.

Each translation unit is compiled for its own CPU level. The dispatcher and
SSE2 implementation retain the x86-64 baseline. Upstream selects the appropriate
configuration at runtime, with a valid SSE2 table available before constructors.

The upstream Python post-pass is required to produce a linkable archive. It
specializes implementations for each dispatch configuration and rewrites their
machine code. This integration builds PIC objects for ClickHouse's PIE links,
disables LTO for these objects, and renames the public symbols after the pass
so that the pass still recognizes its internal calls. The archive is loaded
before the LLVM libc archive, whose overlapping memory definitions are weak.

The pass discards unwind metadata. Every function is compiled with frame
pointers, and its frame-removal pass is disabled to preserve frame-chain
profiling. Source-level debug information is disabled because the pass does not
remap it to the rewritten code. Function symbols remain available. Native
profiling must verify that stacks traverse the rewritten functions correctly.

## Experimental AArch64 provider

The AArch64 extension lives in `aarch64/` while the upstream submodule retains
its published x86 revision. Only selected kernel bodies and their helpers are
included from our ARM workbench; `aarch64/provenance.json` records source hashes
and the selection. The MIT license is included. This makes an ARM build
reproducible without depending on an unpublished fork commit.

The ARM objects use the compiler output directly, retaining unwind information.
Most entries retain frame pointers. The leaf public `memcpy`, `memmove`, and
`memset` entries omit their frames and retain compiler-generated asynchronous
unwind tables; bulk kernels, other entries, and nonleaf constructors keep their
frames. Baseline
objects use `-march=armv8-a`; separate SVE objects use `-march=armv8-a+sve`
and scalable vector length. Valid NEON implementations are available before
constructors run. Constructors check Linux `AT_HWCAP` and atomically publish
SVE availability. The bounded tiny-copy helper predicates every memory access
and only uses the first 16 byte lanes. Other SVE wrappers retain their tuned
16-byte vector-length gate and use NEON outside it. The library changes no
process or thread vector-length setting.

The public `memcpy` entry uses one predicated SVE load/store for lengths
below 16 on SVE hosts after initialization. Its exact 16-byte NEON load/store
stays before the feature check. The NEON path handles other short copies and
pre-constructor calls. `memmove` retains its bounded NEON path through 128
bytes, loading every source byte before the first store. Larger copies and
moves call their separate NEON kernels directly. `memset` handles up to 16
bytes inline and uses NEON above that boundary.
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
their existing providers. Sanitizer and sanitizer-coverage builds reject the
experimental option so their interceptors remain authoritative.

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
This option remains experimental and disabled by default.

See the [2026-11-07 native AArch64 comparison](benchmarks/aarch64-2026-11-07.md)
for the build configuration, query results, and limits of the measurements.
The [2026-11-08 hierarchical dictionary investigation](benchmarks/hierarchy-2026-11-08.md)
records the repeated regression, zero-fill minimization, fix, and remaining
flat-hierarchy control difference.
The [2026-11-08 regression portfolio](benchmarks/regressions-2026-11-08.md)
records all 19 affected suites on the later candidate, minimized caller
experiments, retained timing concerns and exact proof limits.

On a native AArch64 glibc host, test the prefixed archive against the independent
host libc, including protected pages and oversized early-match `memchr`:

```sh
clang -O2 -fno-builtin contrib/str-cmake/test_aarch64.c \
    build_candidate/contrib/str-cmake/libstr-prefixed.a \
    -o build_candidate/str-contract-test
build_candidate/str-contract-test > build_candidate/test_str_contract.log 2>&1
clang -O2 -fno-builtin -DSTR_TEST_NO_SVE -Wl,--wrap=getauxval \
    contrib/str-cmake/test_aarch64.c \
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

## Validation before enabling by default

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

Use the same option in a candidate CI build after native correctness and dev
comparisons pass. Regular CI builds retain the default until this experimental
implementation is explicitly enabled. Performance under CPU emulation is not
usable evidence for enabling it.
