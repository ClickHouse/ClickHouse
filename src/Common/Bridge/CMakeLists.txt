include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_bridge .)

add_library(clickhouse_common_bridge ${clickhouse_common_bridge_headers} ${clickhouse_common_bridge_sources})

target_link_libraries (clickhouse_common_bridge PUBLIC clickhouse_common_io common PRIVATE daemon Poco::Util)

