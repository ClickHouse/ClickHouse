add_library (clickhouse-client-lib Client.cpp)
target_link_libraries (clickhouse-client-lib clickhouse_functions clickhouse_aggregate_functions ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
if (READLINE_INCLUDE_DIR)
    target_include_directories (clickhouse-client-lib SYSTEM PRIVATE ${READLINE_INCLUDE_DIR})
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-client clickhouse-client.cpp)
    target_link_libraries (clickhouse-client clickhouse-client-lib)
endif ()

install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)

