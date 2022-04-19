include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_backups .)
add_library(clickhouse_common_backups ${clickhouse_common_backups_headers} ${clickhouse_common_backups_sources})
target_link_libraries(clickhouse_common_backups PUBLIC clickhouse_common_io)
