enable_testing()

# Run tests with "ninja check" or "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS clickhouse-test clickhouse-test-server DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
install (
    DIRECTORY queries performance config
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse-test
    USE_SOURCE_PERMISSIONS
    COMPONENT clickhouse
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

install (FILES server-test.xml DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-server COMPONENT clickhouse)
install (FILES client-test.xml DESTINATION  ${CLICKHOUSE_ETC_DIR}/clickhouse-client COMPONENT clickhouse)

if (ENABLE_TESTS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake ${ClickHouse_BINARY_DIR})

    # maybe add --no-long ?
    # if you want disable some tests: env TEST_OPT0='--skip compile'
    if(TEST_CMAKE_PARALLEL)
        # NUMBER_OF_LOGICAL_CORES
        if (TEST_CMAKE_PARALLEL GREATER 1)
            set(TOTAL_TESTS ${TEST_CMAKE_PARALLEL})
        else()
            set(TOTAL_TESTS ${NUMBER_OF_LOGICAL_CORES})
        endif()
        foreach(proc RANGE 1 ${TOTAL_TESTS})
            add_test(NAME with_server${proc} COMMAND bash -c "env BUILD_DIR=${ClickHouse_BINARY_DIR} TEST_OPT0=--parallel=${proc}/${TOTAL_TESTS} ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-test-server")
        endforeach()
    else()
        add_test(NAME with_server COMMAND bash -c "env BUILD_DIR=${ClickHouse_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/clickhouse-test-server")
    endif()

endif ()

if (ENABLE_TEST_INTEGRATION)
    add_subdirectory (integration)
endif ()
