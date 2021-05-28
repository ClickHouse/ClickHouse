include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions_array .)
add_library(clickhouse_functions_array ${clickhouse_functions_array_sources} ${clickhouse_functions_array_headers})
target_link_libraries(clickhouse_functions_array PRIVATE dbms clickhouse_functions_gatherutils)

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions_array PRIVATE "-g0")
endif ()
