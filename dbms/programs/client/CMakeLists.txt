set(CLICKHOUSE_CLIENT_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/Client.cpp)
set(CLICKHOUSE_CLIENT_LINK PRIVATE clickhouse_common_config clickhouse_functions clickhouse_aggregate_functions clickhouse_common_io ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
set(CLICKHOUSE_CLIENT_INCLUDE SYSTEM PRIVATE ${READLINE_INCLUDE_DIR})

clickhouse_program_add(client)

install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)
