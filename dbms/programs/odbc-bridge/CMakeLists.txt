add_library (clickhouse-odbc-bridge-lib ${LINK_MODE}
    PingHandler.cpp
    MainHandler.cpp
    ColumnInfoHandler.cpp
    IdentifierQuoteHandler.cpp
    HandlerFactory.cpp
    ODBCBridge.cpp
    getIdentifierQuote.cpp
    validateODBCConnectionString.cpp
)

target_link_libraries (clickhouse-odbc-bridge-lib PRIVATE clickhouse_common_io daemon dbms)
target_include_directories (clickhouse-odbc-bridge-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

if (USE_POCO_SQLODBC)
    target_link_libraries (clickhouse-odbc-bridge-lib PRIVATE ${Poco_SQLODBC_LIBRARY})
    target_include_directories (clickhouse-odbc-bridge-lib SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_SQLODBC_INCLUDE_DIR})
endif ()
if (Poco_SQL_FOUND)
    target_link_libraries (clickhouse-odbc-bridge-lib PRIVATE ${Poco_SQL_LIBRARY})
endif ()

if (USE_POCO_DATAODBC)
    target_link_libraries (clickhouse-odbc-bridge-lib PRIVATE ${Poco_DataODBC_LIBRARY})
    target_include_directories (clickhouse-odbc-bridge-lib SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_DataODBC_INCLUDE_DIR})
endif()
if (Poco_Data_FOUND)
    target_link_libraries (clickhouse-odbc-bridge-lib PRIVATE ${Poco_Data_LIBRARY})
endif ()


if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

# clickhouse-odbc-bridge is always a separate binary.
# Reason: it must not export symbols from SSL, mariadb-client, etc. to not break ABI compatibility with ODBC drivers.

add_executable (clickhouse-odbc-bridge odbc-bridge.cpp)
target_link_libraries (clickhouse-odbc-bridge PRIVATE clickhouse-odbc-bridge-lib)
install (TARGETS clickhouse-odbc-bridge RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
