include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_xpath ASTs)
add_headers_and_sources(clickhouse_functions_xpath Instructions)
add_headers_and_sources(clickhouse_functions_xpath Parsers)

add_library(clickhouse_functions_xpath ${clickhouse_functions_xpath_sources} ${clickhouse_functions_xpath_headers})

target_link_libraries(clickhouse_functions_xpath PRIVATE dbms)
target_link_libraries(clickhouse_functions_xpath PRIVATE clickhouse_parsers)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options(clickhouse_functions_xpath PRIVATE "-g0")
endif()
