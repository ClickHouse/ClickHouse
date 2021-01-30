enable_testing()

# Run tests with "ninja check" or "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS clickhouse-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
install (
    DIRECTORY queries performance config
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse-test
    USE_SOURCE_PERMISSIONS
    COMPONENT clickhouse
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

if (ENABLE_TEST_INTEGRATION)
    add_subdirectory (integration)
endif ()
