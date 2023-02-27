enable_testing()

# Run tests with "ninja check" or "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

option (ENABLE_CLICKHOUSE_TEST "Install clickhouse-test script and relevant tests scenarios" OFF)

if (ENABLE_CLICKHOUSE_TEST)
    install (PROGRAMS clickhouse-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
    install (
        DIRECTORY queries performance config
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse-test
        USE_SOURCE_PERMISSIONS
        COMPONENT clickhouse
        PATTERN "CMakeLists.txt" EXCLUDE
        PATTERN ".gitignore" EXCLUDE
    )
endif ()

if (ENABLE_TEST_INTEGRATION)
    add_subdirectory (integration)
endif ()
