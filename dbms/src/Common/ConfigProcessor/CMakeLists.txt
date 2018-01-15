
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_configprocessor .)

add_library(clickhouse_common_configprocessor ${SPLIT_SHARED} ${clickhouse_common_configprocessor_headers} ${clickhouse_common_configprocessor_sources})

target_link_libraries (clickhouse_common_configprocessor clickhouse_common_zookeeper string_utils)
target_include_directories (clickhouse_common_configprocessor PRIVATE ${DBMS_INCLUDE_DIR})
