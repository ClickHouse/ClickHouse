include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_functions_user_defined .)
add_library(clickhouse_functions_user_defined ${clickhouse_functions_user_defined_sources} ${clickhouse_functions_user_defined_headers})

target_link_libraries(clickhouse_functions_user_defined PRIVATE dbms)
