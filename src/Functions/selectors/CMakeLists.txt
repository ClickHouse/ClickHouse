include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_functions_selectors .)
add_library(clickhouse_functions_selectors ${clickhouse_functions_selectors_sources} ${clickhouse_functions_selectors_headers})

target_link_libraries(clickhouse_functions_selectors PRIVATE dbms)
