set (CLICKHOUSE_LOCAL_SOURCES LocalServer.cpp)

set (CLICKHOUSE_LOCAL_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
)

clickhouse_program_add(local)

target_link_libraries(clickhouse-local-lib PRIVATE clickhouse-server-lib)

if (TARGET ch_rust::skim)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_rust::skim)
endif()
if (TARGET ch_contrib::azure_sdk)
    target_link_libraries(clickhouse-local-lib PRIVATE ch_contrib::azure_sdk)
endif()

# Always use internal readpassphrase
target_link_libraries(clickhouse-local-lib PRIVATE readpassphrase)
