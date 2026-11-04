if (ENABLE_TESTS)
    add_subdirectory(tests)
endif()

if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(HashJoin/benchmarks)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
