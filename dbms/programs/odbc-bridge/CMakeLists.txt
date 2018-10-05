add_library (clickhouse-odbc-bridge-lib ${LINK_MODE}
    PingHandler.cpp
    MainHandler.cpp
    ColumnInfoHandler.cpp
    HandlerFactory.cpp
    ODBCBridge.cpp
    validateODBCConnectionString.cpp
)

target_link_libraries (clickhouse-odbc-bridge-lib clickhouse_common_io daemon dbms)
target_include_directories (clickhouse-odbc-bridge-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

if (USE_POCO_SQLODBC)
    target_link_libraries (clickhouse-odbc-bridge-lib ${Poco_SQLODBC_LIBRARY})
    target_include_directories (clickhouse-odbc-bridge-lib SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_SQLODBC_INCLUDE_DIR})
endif ()
if (Poco_SQL_FOUND)
    target_link_libraries (clickhouse-odbc-bridge-lib ${Poco_SQL_LIBRARY})
endif ()

if (USE_POCO_DATAODBC)
    target_link_libraries (clickhouse-odbc-bridge-lib ${Poco_DataODBC_LIBRARY})
    target_include_directories (clickhouse-odbc-bridge-lib SYSTEM PRIVATE ${ODBC_INCLUDE_DIRECTORIES} ${Poco_DataODBC_INCLUDE_DIR})
endif()
if (Poco_Data_FOUND)
    target_link_libraries (clickhouse-odbc-bridge-lib ${Poco_Data_LIBRARY})
endif ()


if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-odbc-bridge odbc-bridge.cpp)
    target_link_libraries (clickhouse-odbc-bridge clickhouse-odbc-bridge-lib)
endif ()
