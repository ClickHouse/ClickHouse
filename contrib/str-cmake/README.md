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

The ARM objects use the compiler output directly, retaining unwind information
and frame pointers. Baseline objects use `-march=armv8-a`; separate SVE objects
use `-march=armv8-a+sve` and scalable vector length. Valid NEON implementations
are available before constructors run. A constructor checks Linux `AT_HWCAP`
and atomically installs SVE entry points on supported hosts. Each SVE entry
checks the calling thread's current vector length: the tuned kernels run only
at 16 bytes, the measured and modeled length. Other lengths use NEON. No
process or thread vector-length setting is changed by the library.

The public `memcpy`, `memmove`, and `memset` entry points handle up to 16 bytes
inline. Sizes above 64 bytes call NEON directly: the selected SVE kernels
already use those same NEON implementations above that boundary at a vector
length of 16 bytes. Only sizes from 17 through 64 bytes need runtime selection
for these three functions. This avoids dispatch overhead in these size ranges.

The overlapping strong definitions are excluded from the existing Arm
optimized-routines archive only while this option is enabled. Its `strchr`
and `strnlen` remain in use. LLVM libc math and other memory routines retain
their existing providers. Sanitizer and sanitizer-coverage builds reject the
experimental option so their interceptors remain authoritative.

The workbench has partial machine-code proof coverage. The native validation
also proved the public `memcpy`, `memmove`, and `memset` paths for lengths from
0 through 16 from the actual integration object files, within the model's
assumptions. Constructors, longer paths, and the complete ClickHouse executable
remain outside that proof. This option remains experimental and disabled by default.

See the [2026-11-07 native AArch64 comparison](benchmarks/aarch64-2026-11-07.md)
for the build configuration, query results, and limits of the measurements.

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
