enable_testing()

# Run tests with "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

install (PROGRAMS clickhouse-test DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT clickhouse)
install (
    DIRECTORY queries performance external_dictionaries
    DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/clickhouse-test
    USE_SOURCE_PERMISSIONS
    COMPONENT clickhouse
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN ".gitignore" EXCLUDE
)

add_subdirectory (external_dictionaries)

if (ENABLE_TESTS)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/CTestCustom.cmake ${CMAKE_BINARY_DIR})

    # maybe add --no-long ?
    add_test(NAME with_server COMMAND bash -c "env TEST_OPT='--skip 00281' ${CMAKE_CURRENT_SOURCE_DIR}/server_wrapper.sh")
endif ()
