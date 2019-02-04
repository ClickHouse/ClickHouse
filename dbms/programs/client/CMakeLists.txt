add_library (clickhouse-client-lib ${LINK_MODE} Client.cpp)
target_link_libraries (clickhouse-client-lib PRIVATE clickhouse_common_config clickhouse_functions clickhouse_aggregate_functions clickhouse_common_io ${LINE_EDITING_LIBS} ${Boost_PROGRAM_OPTIONS_LIBRARY})
if (READLINE_INCLUDE_DIR)
    target_include_directories (clickhouse-client-lib SYSTEM PRIVATE ${READLINE_INCLUDE_DIR})
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-client clickhouse-client.cpp)
    target_link_libraries (clickhouse-client PRIVATE clickhouse-client-lib)
    install (TARGETS clickhouse-client ${CLICKHOUSE_ALL_TARGETS} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif ()

install (FILES clickhouse-client.xml DESTINATION ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse-client RENAME config.xml)
