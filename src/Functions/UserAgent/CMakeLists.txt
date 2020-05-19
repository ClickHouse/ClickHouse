include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_functions_useragent .)
add_library(clickhouse_functions_useragent ${clickhouse_functions_useragent_sources}  extractBrowserFromUserAgent.cpp)
target_link_libraries(clickhouse_functions_useragent PRIVATE dbms)

if (CMAKE_BUILD_TYPE_UC STREQUAL "RELEASE" OR CMAKE_BUILD_TYPE_UC STREQUAL "RELWITHDEBINFO" OR CMAKE_BUILD_TYPE_UC STREQUAL "MINSIZEREL")
    # Won't generate debug info for files with heavy template instantiation to achieve faster linking and lower size.
    target_compile_options(clickhouse_functions_useragent PRIVATE "-g0")
endif ()
