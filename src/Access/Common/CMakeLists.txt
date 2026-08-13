include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_access .)
add_library(clickhouse_common_access ${clickhouse_common_access_headers} ${clickhouse_common_access_sources})
target_link_libraries(clickhouse_common_access PUBLIC clickhouse_common_io)
