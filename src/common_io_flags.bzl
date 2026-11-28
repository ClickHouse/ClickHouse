"""Compile definitions of clickhouse_common_io and dbms, including their dependencies."""

load("@clickhouse//bazel/rules:ch_cc_library.bzl", "CXX_RUNTIME_DEFINES")

# CMake orders compile definitions by name, including definitions from dependencies.
_COMMON_IO = CXX_RUNTIME_DEFINES + [
    "BOOST_ASIO_HAS_STD_INVOKE_RESULT=1",
    "BOOST_ASIO_STANDALONE=1",
    "BOOST_TIMER_ENABLE_DEPRECATED=1",
    "ENABLE_DISTRIBUTED_CACHE=0",
    "FIU_ENABLE",
    "HAVE_ATTRIBUTE_ALIGNED",
    "HAVE_BUILTIN_CTZ",
    "HAVE_BUILTIN_CTZLL",
    "HAVE_CONFIG_H",
    "HAVE_POSIX_MEMALIGN",
    "HAVE_VISIBILITY_HIDDEN",
    "HAVE_VISIBILITY_INTERNAL",
    "JEMALLOC_NO_RENAME",
    "PICOJSON_USE_LOCALE=0",
    "POCO_ENABLE_CPP11",
    "POCO_OS_FAMILY_UNIX",
    "UNALIGNED_OK",
    "WITH_COVERAGE=0",
    "WITH_COVERAGE_DEPTH=0",
    "WITH_GZFILEOP",
    "X86_64",
    "ZLIB_COMPAT",
]

_COMMON_IO_X86_64 = _COMMON_IO + [
    "X86_AVX2",
    "X86_FEATURES",
    "X86_PCLMULQDQ_CRC",
    "X86_SSE2",
    "X86_SSE42",
    "X86_SSSE3",
]

_COMMON_IO_AARCH64 = _COMMON_IO + [
    "ARM_ACLE",
    "ARM_ASM_HWCAP",
    "ARM_AUXV_HAS_CRC32",
    "ARM_AUXV_HAS_NEON",
    "ARM_FEATURES",
    "ARM_NEON",
    "ARM_NEON_HASLD4",
    "HAVE_ARM_ACLE_H",
    "HAVE_SYS_AUXV_H",
]

_MULTITARGET = ["ENABLE_MULTITARGET_CODE=1"]

def common_io_defines(extra = []):
    """Compile with common IO's definitions and the target's extra definitions in CMake order."""
    return select({
        "@platforms//cpu:x86_64": sorted(_COMMON_IO_X86_64 + extra),
        "@platforms//cpu:aarch64": sorted(_COMMON_IO_AARCH64 + extra),
    })

# These definitions come from the private minizip and stringzilla dependencies.
_COMMON_IO_PRIVATE = [
    "MZ_COMPAT_VERSION=110",
    "SZ_DYNAMIC_DISPATCH=1",
]

CLICKHOUSE_COMMON_IO_CONSUMER_DEFINES = common_io_defines(_MULTITARGET)

CLICKHOUSE_COMMON_IO_LOCAL_DEFINES = common_io_defines(_MULTITARGET + _COMMON_IO_PRIVATE)

# The DBMS consumers also compile with the AI SDK, Arrow, and usearch definitions.
_USEARCH_DEFINES = [
    "SIMSIMD_DYNAMIC_DISPATCH",
    "SIMSIMD_NATIVE_BF16=0",
    "USEARCH_CAN_COMPILE_BF16=0",
    "USEARCH_CAN_COMPILE_FLOAT16",
    "USEARCH_USE_FP16LIB",
    "USEARCH_USE_SIMSIMD",
]

_DBMS = [
    "AI_SDK_HAS_ANTHROPIC=1",
    "AI_SDK_HAS_OPENAI=1",
    "USE_CLICKHOUSE_THREADS=1",
]

_DBMS_X86_64 = _COMMON_IO_X86_64 + _DBMS + [
    "ARROW_HAVE_AVX2",
    "ARROW_HAVE_BMI2",
    "ARROW_HAVE_RUNTIME_AVX2",
    "ARROW_HAVE_RUNTIME_BMI2",
    "ARROW_HAVE_RUNTIME_SSE4_2",
    "ARROW_HAVE_SSE4_2",
]

_DBMS_AARCH64 = _COMMON_IO_AARCH64 + _DBMS + ["ARROW_HAVE_NEON"]

DBMS_INTERFACE_X86_64_DEFINES = sorted(_DBMS_X86_64 + _USEARCH_DEFINES)
DBMS_INTERFACE_AARCH64_DEFINES = sorted(_DBMS_AARCH64 + _USEARCH_DEFINES)

def dbms_defines(extra = []):
    """Compile with the DBMS consumer definitions and the target's extra definitions in CMake order."""
    return select({
        "@platforms//cpu:x86_64": sorted(DBMS_INTERFACE_X86_64_DEFINES + _MULTITARGET + extra),
        "@platforms//cpu:aarch64": sorted(DBMS_INTERFACE_AARCH64_DEFINES + _MULTITARGET + extra),
    })

DBMS_CONSUMER_DEFINES = dbms_defines()

_DBMS_LOCAL = _MULTITARGET + [
    "SNAPPY_CODEC_AVAILABLE",
    "XXH_INLINE_ALL",
]

DBMS_LOCAL_DEFINES = select({
    "@platforms//cpu:x86_64": sorted(DBMS_INTERFACE_X86_64_DEFINES + _DBMS_LOCAL),
    "@platforms//cpu:aarch64": sorted(DBMS_INTERFACE_AARCH64_DEFINES + _DBMS_LOCAL),
})

# dbms__compile_0 receives the USearch definitions from its implementation dependency.
DBMS_USEARCH_LOCAL_DEFINES = select({
    "@platforms//cpu:x86_64": sorted(_DBMS_X86_64 + _DBMS_LOCAL),
    "@platforms//cpu:aarch64": sorted(_DBMS_AARCH64 + _DBMS_LOCAL),
})
