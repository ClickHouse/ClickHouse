set (CLICKHOUSE_LOCAL_SOURCES LocalServer.cpp)

set (CLICKHOUSE_LOCAL_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_dictionaries
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
)

clickhouse_program_add(local)

if(NOT CLICKHOUSE_ONE_SHARED)
    target_link_libraries(clickhouse-local-lib PRIVATE clickhouse-server-lib)
endif()
