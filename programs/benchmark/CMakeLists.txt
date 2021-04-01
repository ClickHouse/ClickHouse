set (CLICKHOUSE_BENCHMARK_SOURCES Benchmark.cpp)

set (CLICKHOUSE_BENCHMARK_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        dbms
)

clickhouse_program_add(benchmark)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries (clickhouse-benchmark-lib PRIVATE clickhouse-client-lib)
endif()
