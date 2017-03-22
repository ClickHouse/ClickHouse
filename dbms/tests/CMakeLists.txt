enable_testing()

# Run tests with "make check"
if (TARGET check)
	message (STATUS "Target check already exists")
else ()
	include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)
endif ()


# Google Test from sources
add_subdirectory(${ClickHouse_SOURCE_DIR}/contrib/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)
# avoid problems with <regexp.h>
target_compile_definitions (gtest INTERFACE -DGTEST_HAS_POSIX_RE=0)
target_include_directories (gtest INTERFACE ${ClickHouse_SOURCE_DIR}/contrib/googletest/include)


macro(grep_gtest_sources BASE_DIR DST_VAR)
	# not CMake-way
	execute_process(COMMAND find "${BASE_DIR}" -path "*/tests/gtest*.cpp" -printf "%p;" OUTPUT_VARIABLE ${DST_VAR})
endmacro()
