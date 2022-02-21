set (CLICKHOUSE_CLIENT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/Client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ConnectionParameters.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/QueryFuzzer.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Suggest.cpp
)

set (CLICKHOUSE_CLIENT_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        string_utils
)

# Always use internal readpassphrase
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-client" COMPONENT clickhouse-client RENAME config.xml)
