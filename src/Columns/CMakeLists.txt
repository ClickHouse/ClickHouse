if (ENABLE_EXAMPLES)
    add_subdirectory (examples)
endif ()

if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif ()
