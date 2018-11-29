add_library (clickhouse-benchmark-lib ${LINK_MODE} Benchmark.cpp)
target_link_libraries (clickhouse-benchmark-lib PRIVATE clickhouse_aggregate_functions clickhouse-client-lib clickhouse_common_config clickhouse_common_io ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_include_directories (clickhouse-benchmark-lib SYSTEM PRIVATE ${PCG_RANDOM_INCLUDE_DIR})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-benchmark clickhouse-benchmark.cpp)
    target_link_libraries (clickhouse-benchmark PRIVATE clickhouse-benchmark-lib clickhouse_aggregate_functions)
endif ()
