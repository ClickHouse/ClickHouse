include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_zookeeper .)

add_library(clickhouse_common_zookeeper ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})

target_link_libraries (clickhouse_common_zookeeper PUBLIC clickhouse_common_io common PRIVATE string_utils)

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
