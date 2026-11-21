# Declare this after instrumentation options, before selecting default libraries.
set (PG83_STR_SUPPORTED OFF)
if (OS_LINUX AND (ARCH_AMD64 OR ARCH_AARCH64)
    AND NOT SANITIZE AND NOT SANITIZE_COVERAGE AND NOT WITH_COVERAGE
    AND NOT ENABLE_FUZZING AND NOT FUZZER AND NOT ENABLE_CLICKHOUSE_PGO_GENERATE)
    set (PG83_STR_SUPPORTED ON)
endif ()

option (ENABLE_PG83_STR "Use pg83/str string and memory functions" ${PG83_STR_SUPPORTED})
if (ENABLE_PG83_STR AND NOT PG83_STR_SUPPORTED)
    message (FATAL_ERROR "ENABLE_PG83_STR requires an x86-64 or AArch64 Linux build without sanitizer, coverage, fuzzer or PGO generation instrumentation")
endif ()

message (STATUS "Using pg83/str: ${ENABLE_PG83_STR}")
