include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_jsonpath_generators .)
add_library(clickhouse_functions_jsonpath_generators ${clickhouse_functions_jsonpath_generators_sources} ${clickhouse_functions_jsonpath_generators_headers})
target_link_libraries(clickhouse_functions_jsonpath_generators PRIVATE dbms)

if (STRIP_DEBUG_SYMBOLS_FUNCTIONS)
    target_compile_options(clickhouse_functions_jsonpath_generators PRIVATE "-g0")
endif()
