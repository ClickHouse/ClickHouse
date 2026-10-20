"""Compiler definitions for ClickHouse Common and IO translation units."""

CLICKHOUSE_COMMON_IO_LOCAL_DEFINES = select({
    "@platforms//cpu:x86_64": [],
    "@platforms//cpu:aarch64": [
        "ARM_ACLE",
        "ARM_ASM_HWCAP",
        "ARM_AUXV_HAS_CRC32",
        "ARM_AUXV_HAS_NEON",
        "ARM_FEATURES",
        "ARM_NEON",
        "ARM_NEON_HASLD4",
    ],
}) + [
    "BOOST_ASIO_HAS_STD_INVOKE_RESULT=1",
    "BOOST_ASIO_STANDALONE=1",
    "BOOST_TIMER_ENABLE_DEPRECATED=1",
    "ENABLE_DISTRIBUTED_CACHE=0",
    "ENABLE_MULTITARGET_CODE=1",
    "FIU_ENABLE",
] + select({
    "@platforms//cpu:x86_64": [],
    "@platforms//cpu:aarch64": [
        "HAVE_ARM_ACLE_H",
    ],
}) + [
    "HAVE_ATTRIBUTE_ALIGNED",
    "HAVE_BUILTIN_CTZ",
    "HAVE_BUILTIN_CTZLL",
    "HAVE_CONFIG_H",
    "HAVE_POSIX_MEMALIGN",
] + select({
    "@platforms//cpu:x86_64": [],
    "@platforms//cpu:aarch64": [
        "HAVE_SYS_AUXV_H",
    ],
}) + [
    "HAVE_VISIBILITY_HIDDEN",
    "HAVE_VISIBILITY_INTERNAL",
    "JEMALLOC_NO_RENAME",
    "MZ_COMPAT_VERSION=110",
    "PICOJSON_USE_LOCALE=0",
    "POCO_ENABLE_CPP11",
    "POCO_OS_FAMILY_UNIX",
    "STD_EXCEPTION_HAS_STACK_TRACE=1",
    "SZ_DYNAMIC_DISPATCH=1",
    "UNALIGNED_OK",
    "WITH_COVERAGE=0",
    "WITH_COVERAGE_DEPTH=0",
    "WITH_GZFILEOP",
    "X86_64",
] + select({
    "@platforms//cpu:x86_64": [
        "X86_AVX2",
        "X86_FEATURES",
        "X86_PCLMULQDQ_CRC",
        "X86_SSE2",
        "X86_SSE42",
        "X86_SSSE3",
    ],
    "@platforms//cpu:aarch64": [],
}) + [
    "ZLIB_COMPAT",
    "_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS",
    "_LIBCPP_REMOVE_TRANSITIVE_INCLUDES=1",
    "_LIBUNWIND_IS_NATIVE_ONLY",
]
