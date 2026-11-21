if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(Formats/fuzzers)
endif()
