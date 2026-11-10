if (ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_MYSQL)
    add_subdirectory(mysqlxx)
endif ()
