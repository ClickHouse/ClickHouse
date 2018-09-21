if (CLICKHOUSE_SPLIT_BINARY)
    set (TEST_USE_BINARIES CLICKHOUSE_TESTS_SERVER_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/server/clickhouse-server CLICKHOUSE_TESTS_CLIENT_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/server/clickhouse-client)
else()
    set (TEST_USE_BINARIES CLICKHOUSE_TESTS_SERVER_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/server/clickhouse CLICKHOUSE_TESTS_CLIENT_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/server/clickhouse-client)
endif()

add_test (NAME integration WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} COMMAND env ${TEST_USE_BINARIES} "CLICKHOUSE_TESTS_BASE_CONFIG_DIR=\${CLICKHOUSE_TESTS_BASE_CONFIG_DIR:=${ClickHouse_SOURCE_DIR}/dbms/programs/server/}" ${PYTEST_STARTER} pytest ${PYTEST_OPT})
