add_library (clickhouse-local-lib LocalServer.cpp)
target_link_libraries (clickhouse-local-lib clickhouse_common_io clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-local clickhouse-local.cpp)
    target_link_libraries (clickhouse-local clickhouse-local-lib)
endif ()
