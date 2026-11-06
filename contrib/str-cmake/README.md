# Experimental string and memory functions

`ENABLE_PG83_STR=ON` replaces the libc entry points for `memcpy`, `memmove`,
`memset`, `memcmp`, `strlen`, `strcpy`, `strcat`, `strcmp`, `strspn`, `strcspn`,
and `memchr` with [pg83/str](https://github.com/pg83/str). The option is off by
default and requires Linux x86-64 without sanitizers. Other LLVM libc functions,
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

## Validation before enabling by default

Build two release binaries from the same revision and toolchain, changing only
`ENABLE_PG83_STR`. Use separate build directories and isolated server data
directories. Keep LLVM libc enabled in both builds so the baseline uses the
current ClickHouse implementations rather than the system libc.

Run upstream's full correctness suite against the prefixed archive retained at
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
`string_deserialization.xml`, `string_filters_during_scan.xml`,
`group_by_string_key_count.xml`, `string_join.xml`, and `array_concat_single.xml`,
plus broader workloads to detect changes beyond string functions. Record CPU,
build revision, compiler, settings, binary paths, and old/new timings. The
upstream synthetic results alone do not establish a ClickHouse improvement.

Use the same option in a candidate CI build after native correctness and dev
comparisons pass. Regular CI builds retain the default until this experimental
implementation is explicitly enabled. Performance under CPU emulation is not
usable evidence for enabling it.
