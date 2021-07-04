include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_jsonpath_generator .)
add_library(clickhouse_functions_jsonpath_generator ${clickhouse_functions_jsonpath_generator_sources} ${clickhouse_functions_jsonpath_generator_headers})
target_link_libraries(clickhouse_functions_jsonpath_generator PRIVATE dbms)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_jsonpath_generator PRIVATE "-g0")
endif()
