set(CLICKHOUSE_SERVER_SOURCES
    MetricsTransmitter.cpp
    Server.cpp
)

set (CLICKHOUSE_SERVER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions

    PUBLIC
        daemon
)
if (TARGET ch_contrib::jemalloc)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::jemalloc)
endif()
if (TARGET ch_contrib::azure_sdk)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::azure_sdk)
endif()

clickhouse_program_add(server)

install(FILES config.xml users.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-server" COMPONENT clickhouse)

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
