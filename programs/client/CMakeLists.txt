set (CLICKHOUSE_CLIENT_SOURCES
    Client.cpp
)

set (CLICKHOUSE_CLIENT_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
)

if (TARGET ch_rust::skim)
    list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE ch_rust::skim)
endif()

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
