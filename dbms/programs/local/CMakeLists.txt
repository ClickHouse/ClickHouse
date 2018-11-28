add_library (clickhouse-local-lib ${LINK_MODE} LocalServer.cpp)
target_link_libraries (clickhouse-local-lib PRIVATE clickhouse_common_io clickhouse-server-lib clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions ${Boost_PROGRAM_OPTIONS_LIBRARY})

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-local clickhouse-local.cpp)
    target_link_libraries (clickhouse-local PRIVATE clickhouse-local-lib)
endif ()
