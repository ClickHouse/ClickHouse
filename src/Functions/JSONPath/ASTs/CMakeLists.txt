include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_jsonpath_asts .)
add_library(clickhouse_functions_jsonpath_asts ${clickhouse_functions_jsonpath_asts_sources} ${clickhouse_functions_jsonpath_asts_headers})
target_link_libraries(clickhouse_functions_jsonpath_asts PRIVATE dbms)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_jsonpath_asts PRIVATE "-g0")
endif()
