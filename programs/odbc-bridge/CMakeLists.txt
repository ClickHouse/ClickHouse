set (CLICKHOUSE_ODBC_BRIDGE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/ColumnInfoHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/getIdentifierQuote.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/HandlerFactory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/IdentifierQuoteHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MainHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ODBCBlockInputStream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ODBCBlockOutputStream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ODBCBridge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/PingHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/SchemaAllowedHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/validateODBCConnectionString.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/odbc-bridge.cpp
)

if (OS_LINUX)
    # clickhouse-odbc-bridge is always a separate binary.
    # Reason: it must not export symbols from SSL, mariadb-client, etc. to not break ABI compatibility with ODBC drivers.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic")
endif ()

add_executable(clickhouse-odbc-bridge ${CLICKHOUSE_ODBC_BRIDGE_SOURCES})

target_link_libraries(clickhouse-odbc-bridge PRIVATE
    daemon
    dbms
    bridge
    clickhouse_parsers
    nanodbc
    unixodbc
)

set_target_properties(clickhouse-odbc-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)
target_compile_options (clickhouse-odbc-bridge PRIVATE -Wno-reserved-id-macro -Wno-keyword-macro)

if (USE_GDB_ADD_INDEX)
    add_custom_command(TARGET clickhouse-odbc-bridge POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} ../clickhouse-odbc-bridge COMMENT "Adding .gdb-index to clickhouse-odbc-bridge" VERBATIM)
endif()

install(TARGETS clickhouse-odbc-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
