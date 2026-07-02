add_subdirectory(System)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()

if (ENABLE_BENCHMARKS)
    add_subdirectory(MergeTree/benchmarks)
endif()
