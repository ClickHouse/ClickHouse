add_subdirectory(System)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

add_subdirectory(benchmarks)

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
