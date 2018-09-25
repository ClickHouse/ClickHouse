if (CLICKHOUSE_SPLIT_BINARY)
    set (TEST_USE_BINARIES CLICKHOUSE_TESTS_SERVER_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/clickhouse-server CLICKHOUSE_TESTS_CLIENT_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/clickhouse-client)
else()
    set (TEST_USE_BINARIES CLICKHOUSE_TESTS_SERVER_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/clickhouse CLICKHOUSE_TESTS_CLIENT_BIN_PATH=${ClickHouse_BINARY_DIR}/dbms/programs/clickhouse)
endif()

# will mount only one binary to docker container - build with .so cant work
if (MAKE_STATIC_LIBRARIES)
    add_test (NAME integration WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} COMMAND env ${TEST_USE_BINARIES} "CLICKHOUSE_TESTS_BASE_CONFIG_DIR=${ClickHouse_SOURCE_DIR}/dbms/programs/server/" ${PYTEST_STARTER} pytest ${PYTEST_OPT})
endif()
