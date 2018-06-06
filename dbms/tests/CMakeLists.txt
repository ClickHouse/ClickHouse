enable_testing()

# Run tests with "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS clickhouse-test clickhouse-test-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
install (
    DIRECTORY queries performance external_dictionaries
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse-test
    USE_SOURCE_PERMISSIONS
    COMPONENT clickhouse
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

install (FILES server-test.xml DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server COMPONENT clickhouse)
install (FILES client-test.xml DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse)

add_subdirectory (external_dictionaries)

if (ENABLE_TESTS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake ${CMAKE_BINARY_DIR})

    # maybe add --no-long ?
    # if you want disable some tests: env TEST_OPT0='--skip compile'
    add_test(NAME with_server COMMAND bash -c "env BUILD_DIR=${ClickHouse_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-test-server")
endif ()
