# zxc - an asymmetric LZ codec optimized for decompression throughput.
# Upstream: https://github.com/hellobertrand/zxc
#
# This is a curated build. We deliberately do NOT use zxc's own CMakeLists.txt
# because it enables -march=native, LTO and PGO by default and pulls in a CLI,
# tests and packaging targets. ClickHouse manages its own compiler flags and
# needs a portable binary with runtime CPU dispatch, so we mirror only the parts
# of zxc's build that matter: the per-ISA "variant" objects plus the scalar
# core, combined into a single static library.

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/zxc")
set(SRC_DIR "${LIBRARY_DIR}/src/lib")

set(ZXC_INCLUDE_DIRS
    "${SRC_DIR}"
    "${SRC_DIR}/vendors"    # vendored rapidhash.h
)

# Scalar core, compiled once. zxc_dispatch.c performs runtime CPU detection and
# routes compress/decompress/huffman to the best available ISA variant below.
set(ZXC_CORE_SOURCES
    "${SRC_DIR}/zxc_common.c"
    "${SRC_DIR}/zxc_pivco_tables.c"
    "${SRC_DIR}/zxc_dict.c"
    "${SRC_DIR}/zxc_driver.c"
    "${SRC_DIR}/zxc_dispatch.c"
    "${SRC_DIR}/zxc_pstream.c"
    "${SRC_DIR}/zxc_seekable.c"
)

# zxc_compress.c / zxc_decompress.c / zxc_huffman.c are recompiled once per ISA
# variant, each with its own -m flags and a ZXC_FUNCTION_SUFFIX that renames the
# emitted symbols (e.g. zxc_compress_chunk_wrapper_avx2). zxc_dispatch.c declares
# these suffixed symbols unconditionally per architecture, so the exact set of
# variants below must match what the dispatcher expects for each target:
#   x86-64  -> _default, _avx2, _avx512
#   aarch64 -> _default only (NEON is the AArch64 baseline)
#   other   -> _default only
# SSE2 is the x86-64 baseline and NEON the AArch64 one, so `_default` already
# compiles those code paths; zxc dropped the dedicated `_sse2` and `_neon`
# variants in v0.14.0 and the dispatcher no longer references them. The
# remaining `_neon32` variant is for 32-bit ARM, which ClickHouse does not
# target.
set(ZXC_VARIANT_OBJECTS "")

macro(zxc_add_variant suffix)
    foreach(_src compress decompress huffman)
        set(_tgt "_zxc_${_src}${suffix}")
        add_library(${_tgt} OBJECT "${SRC_DIR}/zxc_${_src}.c")
        target_include_directories(${_tgt} PRIVATE ${ZXC_INCLUDE_DIRS})
        # ZXC_STATIC_DEFINE: no dllexport/visibility attributes (static build).
        # ZXC_FUNCTION_SUFFIX: variant symbol suffix used by the dispatcher.
        target_compile_definitions(${_tgt} PRIVATE ZXC_STATIC_DEFINE "ZXC_FUNCTION_SUFFIX=${suffix}")
        if (${ARGC} GREATER 1)
            target_compile_options(${_tgt} PRIVATE ${ARGN})
        endif ()
        list(APPEND ZXC_VARIANT_OBJECTS $<TARGET_OBJECTS:${_tgt}>)
    endforeach()
endmacro()

if (OS_DARWIN AND ARCH_AMD64)
    # Historically zxc's x86-64 runtime dispatch called `__builtin_cpu_init` /
    # `__builtin_cpu_supports`, which reference compiler-rt's `__cpu_model`. That
    # symbol is not linked into ClickHouse's macOS cross-build, so linking failed
    # with "undefined ___cpu_model". Since v0.14.0 the dispatcher issues `CPUID`
    # and `XGETBV` directly and no longer needs `__cpu_model`, so this is now a
    # conservative restriction rather than a required one: macOS builds are for
    # local development only and are never test-run in CI, so there is nothing
    # to gain from the x86 SIMD variants there. Lifting it is a separate change.
    set(ZXC_FORCE_SCALAR TRUE)
elseif (SANITIZE STREQUAL "memory")
    # clang's MemorySanitizer has no precise shadow model for the x86 byte-shuffle
    # intrinsics (`pshufb` and friends): the shadow of the inputs is OR-ed
    # positionally instead of being routed through the shuffle, so a poisoned lane
    # that the shuffle discards still poisons the logical result. zxc's SIMD
    # kernels deliberately do speculative 16/32-byte loads whose tail lanes fall
    # into malloc'ed scratch buffers or not-yet-written output and are then
    # discarded by such shuffles (e.g. `zxc_decode_copy_overlap_run32`,
    # `zxc_pivco_merge`), which triggers false `use-of-uninitialized-value`
    # reports. The scalar core branches only on initialized bytes, so build only
    # that under MSan. (AArch64 NEON `tbl` gets a precise shadow, but keep all
    # MSan builds scalar for consistency.)
    set(ZXC_FORCE_SCALAR TRUE)
endif ()

# The default (baseline) variant inherits ClickHouse's target flags, which are
# already x86-64-v3 (AVX2) / armv8.2-a+simd (NEON). That is a safe minimum: every
# CPU the ClickHouse binary runs on supports it.
zxc_add_variant(_default)

if (ZXC_FORCE_SCALAR)
    # `ZXC_DISABLE_SIMD` strips the hand-written intrinsics from the kernels,
    # forces the dispatcher to the `_default` variant and drops the
    # `__builtin_cpu_*` calls; the per-ISA variant objects are not built at all.
    # It must be defined on the `_default` kernel objects as well as on the core
    # (which is handled below), otherwise the kernels keep their SIMD paths.
    foreach(_tgt _zxc_compress_default _zxc_decompress_default _zxc_huffman_default)
        target_compile_definitions(${_tgt} PRIVATE ZXC_DISABLE_SIMD)
    endforeach()
elseif (ARCH_AMD64)
    # The AVX2 flags are redundant with the x86-64-v3 baseline but harmless;
    # AVX512 is NOT in the baseline, so those flags are required for that variant
    # (only ever entered at runtime on AVX-512 capable CPUs). The flag sets must
    # stay in sync with `zxc_detect_cpu_features`, which admits a CPU to the AVX2
    # and AVX-512 tiers only after proving BMI1, BMI2 and LZCNT as well. Note
    # that it does not probe FMA, so `-mfma` must not be added here: the
    # compiler would be free to emit FMA instructions into a variant that runs
    # on any AVX2 CPU.
    zxc_add_variant(_avx2   -mavx2 -mbmi -mbmi2 -mlzcnt)
    zxc_add_variant(_avx512 -mavx512f -mavx512bw -mavx512vbmi -mavx512vbmi2 -mbmi -mbmi2 -mlzcnt)
endif ()

add_library(_zxc ${ZXC_CORE_SOURCES} ${ZXC_VARIANT_OBJECTS})

if (ZXC_FORCE_SCALAR)
    target_compile_definitions(_zxc PRIVATE ZXC_DISABLE_SIMD)
endif ()

target_compile_definitions(_zxc PUBLIC ZXC_STATIC_DEFINE)
target_include_directories(_zxc PRIVATE ${ZXC_INCLUDE_DIRS})
target_include_directories(_zxc SYSTEM PUBLIC "${LIBRARY_DIR}/include")

# zxc_driver.c uses pthreads for its multi-threaded frame path.
target_link_libraries(_zxc PRIVATE Threads::Threads)

add_library(ch_contrib::zxc ALIAS _zxc)
