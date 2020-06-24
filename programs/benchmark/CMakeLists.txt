set(CLICKHOUSE_BENCHMARK_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Benchmark.cpp)
set(CLICKHOUSE_BENCHMARK_LINK PRIVATE dbms clickhouse_aggregate_functions clickhouse_common_config ${Boost_PROGRAM_OPTIONS_LIBRARY})

clickhouse_program_add(benchmark)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries (clickhouse-benchmark-lib PRIVATE clickhouse-client-lib)
endif()
