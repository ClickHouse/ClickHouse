include(${ClickHouse_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

set (CLICKHOUSE_ODBC_BRIDGE_SOURCES
    ColumnInfoHandler.cpp
    IdentifierQuoteHandler.cpp
    MainHandler.cpp
    ODBCBlockInputStream.cpp
    ODBCBlockOutputStream.cpp
    ODBCBridge.cpp
    ODBCHandlerFactory.cpp
    PingHandler.cpp
    SchemaAllowedHandler.cpp
    getIdentifierQuote.cpp
    odbc-bridge.cpp
    validateODBCConnectionString.cpp
)

clickhouse_add_executable(clickhouse-odbc-bridge ${CLICKHOUSE_ODBC_BRIDGE_SOURCES})

target_link_libraries(clickhouse-odbc-bridge PRIVATE
    daemon
    dbms
    bridge
    clickhouse_parsers
    ch_contrib::nanodbc
    ch_contrib::unixodbc
)

set_target_properties(clickhouse-odbc-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)
target_compile_options (clickhouse-odbc-bridge PRIVATE -Wno-reserved-id-macro -Wno-keyword-macro)

if (SPLIT_DEBUG_SYMBOLS)
    clickhouse_split_debug_symbols(TARGET clickhouse-odbc-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH ../clickhouse-odbc-bridge)
else()
    clickhouse_make_empty_debug_info_for_nfpm(TARGET clickhouse-odbc-bridge DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/../${SPLITTED_DEBUG_SYMBOLS_DIR})
    install(TARGETS clickhouse-odbc-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
endif()

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
