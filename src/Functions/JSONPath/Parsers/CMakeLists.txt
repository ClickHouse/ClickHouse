include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_jsonpath_parsers .)
add_library(clickhouse_functions_jsonpath_parsers ${clickhouse_functions_jsonpath_parsers_sources} ${clickhouse_functions_jsonpath_parsers_headers})
target_link_libraries(clickhouse_functions_jsonpath_parsers PRIVATE dbms)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_jsonpath_parsers PRIVATE "-g0")
endif()
