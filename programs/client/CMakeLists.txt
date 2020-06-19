set(CLICKHOUSE_CLIENT_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/Client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ConnectionParameters.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/Suggest.cpp
)

set(CLICKHOUSE_CLIENT_LINK PRIVATE clickhouse_common_config clickhouse_functions clickhouse_aggregate_functions clickhouse_common_io clickhouse_parsers string_utils ${Boost_PROGRAM_OPTIONS_LIBRARY})

# Always use internal readpassphrase
add_subdirectory(readpassphrase)
list(APPEND CLICKHOUSE_CLIENT_LINK PRIVATE readpassphrase)

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)
