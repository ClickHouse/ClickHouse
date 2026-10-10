if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
    add_subdirectory(Executors/Runtime/benchmarks)
endif()
