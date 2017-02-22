# Google Test unit tests

# gather all **/gtest*.cpp files with unit tests into single gtest executable. Not CMake-way
execute_process(COMMAND find ${ClickHouse_SOURCE_DIR}/dbms -path "*/tests/gtest*.cpp" -printf "%p;" OUTPUT_VARIABLE sources_with_gtest)

if (ENABLE_TESTS AND sources_with_gtest)
	add_executable(unit_tests ${sources_with_gtest})
	target_link_libraries(unit_tests gtest_main dbms)
	add_test(unit_tests unit_tests)
endif()
