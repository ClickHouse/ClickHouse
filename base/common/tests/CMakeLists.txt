include (${ClickHouse_SOURCE_DIR}/cmake/add_check.cmake)

add_executable (local_date_time_comparison local_date_time_comparison.cpp)
add_executable (realloc-perf allocator.cpp)

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

target_link_libraries (local_date_time_comparison PRIVATE common)
target_link_libraries (realloc-perf PRIVATE common)
add_check(local_date_time_comparison)

add_executable (dump_variable dump_variable.cpp)
target_link_libraries (dump_variable PRIVATE clickhouse_common_io)
