add_library (clickhouse-performance-test-lib ${LINK_MODE} PerformanceTest.cpp)
target_link_libraries (clickhouse-performance-test-lib PRIVATE dbms clickhouse_common_io clickhouse_common_config ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-performance-test-lib SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-performance-test clickhouse-performance-test.cpp)
    target_link_libraries (clickhouse-performance-test PRIVATE clickhouse-performance-test-lib)
endif ()
