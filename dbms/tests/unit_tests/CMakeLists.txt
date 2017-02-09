# Google Test unit tests

# gather all **/gtest*.cpp files with unit tests into single gtest executable. Not CMake-way
execute_process(COMMAND find ${CMAKE_SOURCE_DIR}/dbms -path "*/tests/gtest*.cpp" -printf "%p;" OUTPUT_VARIABLE sources_with_gtest)

if (ENABLE_TESTS AND sources_with_gtest)
	add_definitions(-DGTEST_HAS_POSIX_RE=0)
	add_executable(unit_tests ${sources_with_gtest})
	target_link_libraries(unit_tests gtest_main dbms)
	target_include_directories(unit_tests PRIVATE
		${ClickHouse_SOURCE_DIR}/dbms/include
	)
	add_test(unit_tests unit_tests)
endif()
