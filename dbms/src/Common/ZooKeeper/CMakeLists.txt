
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)

add_headers_and_sources(clickhouse_common_zookeeper .)

add_library(clickhouse_common_zookeeper ${SPLIT_SHARED} ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})

if (NOT USE_INTERNAL_ZOOKEEPER_LIBRARY)
    target_include_directories (clickhouse_common_zookeeper BEFORE PUBLIC ${ZOOKEEPER_INCLUDE_DIR})
endif ()

target_link_libraries (clickhouse_common_zookeeper clickhouse_common_io ${ZOOKEEPER_LIBRARY})

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
