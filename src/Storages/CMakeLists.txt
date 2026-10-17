add_subdirectory(System)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif ()

if (ENABLE_FUZZER_TARGETS)
    add_subdirectory(fuzzers)
endif()
