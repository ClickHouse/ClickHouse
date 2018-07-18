include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

add_executable (date_lut_init date_lut_init.cpp)
add_executable (date_lut2 date_lut2.cpp)
add_executable (date_lut3 date_lut3.cpp)
add_executable (date_lut4 date_lut4.cpp)
add_executable (date_lut_default_timezone date_lut_default_timezone.cpp)
add_executable (multi_version multi_version.cpp)
add_executable (local_date_time_comparison local_date_time_comparison.cpp)

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

target_link_libraries (date_lut_init common ${PLATFORM_LIBS})
target_link_libraries (date_lut2 common ${PLATFORM_LIBS})
target_link_libraries (date_lut3 common ${PLATFORM_LIBS})
target_link_libraries (date_lut4 common ${PLATFORM_LIBS})
target_link_libraries (date_lut_default_timezone common ${PLATFORM_LIBS})
target_link_libraries (multi_version common)
target_link_libraries (local_date_time_comparison common)
add_check(multi_version)
add_check(local_date_time_comparison)

add_executable (unit_tests_libcommon gtest_json_test.cpp gtest_strong_typedef.cpp)
target_link_libraries (unit_tests_libcommon gtest_main common)
add_check(unit_tests_libcommon)

add_executable (dump_variable dump_variable.cpp)
target_link_libraries (dump_variable clickhouse_common_io)
