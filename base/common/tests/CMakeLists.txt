include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

add_executable (date_lut_init date_lut_init.cpp)
add_executable (date_lut2 date_lut2.cpp)
add_executable (date_lut3 date_lut3.cpp)
add_executable (date_lut4 date_lut4.cpp)
add_executable (date_lut_default_timezone date_lut_default_timezone.cpp)
add_executable (local_date_time_comparison local_date_time_comparison.cpp)
add_executable (realloc-perf allocator.cpp)

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

target_link_libraries (date_lut_init PRIVATE common ${PLATFORM_LIBS})
target_link_libraries (date_lut2 PRIVATE common ${PLATFORM_LIBS})
target_link_libraries (date_lut3 PRIVATE common ${PLATFORM_LIBS})
target_link_libraries (date_lut4 PRIVATE common ${PLATFORM_LIBS})
target_link_libraries (date_lut_default_timezone PRIVATE common ${PLATFORM_LIBS})
target_link_libraries (local_date_time_comparison PRIVATE common)
target_link_libraries (realloc-perf PRIVATE common)
add_check(local_date_time_comparison)

if(USE_GTEST)
    add_executable(unit_tests_libcommon gtest_json_test.cpp gtest_strong_typedef.cpp gtest_find_symbols.cpp)
    target_link_libraries(unit_tests_libcommon PRIVATE common ${GTEST_MAIN_LIBRARIES} ${GTEST_LIBRARIES})
    add_check(unit_tests_libcommon)
endif()

add_executable (dump_variable dump_variable.cpp)
target_link_libraries (dump_variable PRIVATE clickhouse_common_io)
