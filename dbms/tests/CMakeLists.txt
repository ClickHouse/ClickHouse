enable_testing()

# Run tests with "make check"
if (TARGET check)
    message (STATUS "Target check already exists")
else ()
    include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()

include (${PROJECT_SOURCE_DIR}/cmake/find_gtest.cmake)

if (USE_INTERNAL_GTEST_LIBRARY)
    # Google Test from sources
    add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/googletest/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
    # avoid problems with <regexp.h>
    target_compile_definitions (gtest INTERFACE GTEST_HAS_POSIX_RE=0)
    target_include_directories (gtest INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/googletest/include)
endif ()

macro(grep_gtest_sources BASE_DIR DST_VAR)
    # Cold match files that are not in tests/ directories
    file(GLOB_RECURSE "${DST_VAR}" RELATIVE "${BASE_DIR}" "gtest*.cpp")
endmacro()

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
