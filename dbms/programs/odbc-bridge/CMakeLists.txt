set(CLICKHOUSE_ODBC_BRIDGE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/ColumnInfoHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/getIdentifierQuote.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/HandlerFactory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/IdentifierQuoteHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/MainHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ODBCBlockInputStream.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ODBCBridge.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/PingHandler.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/validateODBCConnectionString.cpp
)

set(CLICKHOUSE_ODBC_BRIDGE_LINK PRIVATE dbms clickhouse_parsers PUBLIC daemon)
set(CLICKHOUSE_ODBC_BRIDGE_INCLUDE PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

if (USE_POCO_SQLODBC)
    set(CLICKHOUSE_ODBC_BRIDGE_LINK ${CLICKHOUSE_ODBC_BRIDGE_LINK} PRIVATE ${Poco_SQLODBC_LIBRARY})
    set(CLICKHOUSE_ODBC_BRIDGE_INCLUDE ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE} SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_SQLODBC_INCLUDE_DIR})
endif ()
if (Poco_SQL_FOUND)
    set(CLICKHOUSE_ODBC_BRIDGE_LINK ${CLICKHOUSE_ODBC_BRIDGE_LINK} PRIVATE ${Poco_SQL_LIBRARY})
    set(CLICKHOUSE_ODBC_BRIDGE_INCLUDE ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE} SYSTEM PRIVATE ${Poco_SQL_INCLUDE_DIR})
endif ()

if (USE_POCO_DATAODBC)
    set(CLICKHOUSE_ODBC_BRIDGE_LINK ${CLICKHOUSE_ODBC_BRIDGE_LINK} PRIVATE ${Poco_DataODBC_LIBRARY})
    set(CLICKHOUSE_ODBC_BRIDGE_INCLUDE ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE} SYSTEM PRIVATE ${ODBC_INCLUDE_DIRS} ${Poco_DataODBC_INCLUDE_DIR})
endif()
if (Poco_Data_FOUND)
    set(CLICKHOUSE_ODBC_BRIDGE_LINK ${CLICKHOUSE_ODBC_BRIDGE_LINK} PRIVATE ${Poco_Data_LIBRARY})
    set(CLICKHOUSE_ODBC_BRIDGE_INCLUDE ${CLICKHOUSE_ODBC_BRIDGE_INCLUDE} SYSTEM PRIVATE ${Poco_Data_INCLUDE_DIR})
endif ()

clickhouse_program_add_library(odbc-bridge)

if (OS_LINUX)
    # clickhouse-odbc-bridge is always a separate binary.
    # Reason: it must not export symbols from SSL, mariadb-client, etc. to not break ABI compatibility with ODBC drivers.
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-export-dynamic")
endif ()

add_executable(clickhouse-odbc-bridge odbc-bridge.cpp)
set_target_properties(clickhouse-odbc-bridge PROPERTIES RUNTIME_OUTPUT_DIRECTORY ..)

clickhouse_program_link_split_binary(odbc-bridge)

if (USE_GDB_ADD_INDEX)
    add_custom_command(TARGET clickhouse-odbc-bridge POST_BUILD COMMAND ${GDB_ADD_INDEX_EXE} ../clickhouse-odbc-bridge COMMENT "Adding .gdb-index to clickhouse-odbc-bridge" VERBATIM)
endif()

install(TARGETS clickhouse-odbc-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
