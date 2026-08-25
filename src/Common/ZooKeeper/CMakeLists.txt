include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_zookeeper .)

# Needs to be built differently depending on ZOOKEEPER_LOG
list(REMOVE_ITEM clickhouse_common_zookeeper_sources "ZooKeeperImpl.cpp")


add_library(clickhouse_common_zookeeper_base ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})
target_link_libraries (clickhouse_common_zookeeper_base
    PUBLIC
        clickhouse_common_io
        clickhouse_compression
        common
)

# for clickhouse server
add_library(clickhouse_common_zookeeper ZooKeeperImpl.cpp)
target_compile_definitions (clickhouse_common_zookeeper PRIVATE -DZOOKEEPER_LOG)
target_link_libraries (clickhouse_common_zookeeper
    PUBLIC
        clickhouse_common_io
        clickhouse_compression
        common
)

# for examples -- no logging (to avoid extra dependencies)
add_library(clickhouse_common_zookeeper_no_log ZooKeeperImpl.cpp)
target_link_libraries (clickhouse_common_zookeeper_no_log
    PUBLIC
        clickhouse_common_io
        clickhouse_compression
        common
)
if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
