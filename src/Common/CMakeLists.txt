if (ENABLE_TESTS OR ENABLE_BENCHMARKS)
    add_subdirectory(benchmarks)
endif ()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

add_subdirectory (Wyrd)

if (ENABLE_MYSQL)
    add_subdirectory(mysqlxx)
endif ()
