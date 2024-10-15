include(${ClickHouse_SOURCE_DIR}/cmake/split_debug_symbols.cmake)

set (CLICKHOUSE_ODBC_BRIDGE_SOURCES
    ColumnInfoHandler.cpp
    IdentifierQuoteHandler.cpp
    MainHandler.cpp
    ODBCSource.cpp
    ODBCSink.cpp
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

install(TARGETS clickhouse-odbc-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
