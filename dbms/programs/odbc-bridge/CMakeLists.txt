add_library (clickhouse-odbc-bridge-lib
    Handlers.cpp
    HandlerFactory.cpp
    ODBCBridge.cpp
)

target_link_libraries (clickhouse-odbc-bridge-lib clickhouse_common_io daemon dbms)
target_include_directories (clickhouse-odbc-bridge-lib PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)

if (CLICKHOUSE_SPLIT_BINARY)
    add_executable (clickhouse-odbc-bridge odbc-bridge.cpp)
    target_link_libraries (clickhouse-odbc-bridge clickhouse-odbc-bridge-lib)
endif ()
