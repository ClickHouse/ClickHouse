include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_zookeeper .)

# for clickhouse server
#
# NOTE: this library depends from Interpreters (DB::SystemLog<DB::ZooKeeperLogElement>::add),
# and so it should be STATIC because otherwise:
# - it will either fail to compile with -Wl,--unresolved-symbols=report-all
# - or it will report errors at runtime
add_library(clickhouse_common_zookeeper STATIC ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})
target_compile_definitions (clickhouse_common_zookeeper PRIVATE -DZOOKEEPER_LOG)
target_link_libraries (clickhouse_common_zookeeper
    PUBLIC
        clickhouse_common_io
        common
    PRIVATE
        string_utils
)

# for examples -- no logging (to avoid extra dependencies)
add_library(clickhouse_common_zookeeper_no_log ${clickhouse_common_zookeeper_headers} ${clickhouse_common_zookeeper_sources})
target_link_libraries (clickhouse_common_zookeeper_no_log
    PUBLIC
        clickhouse_common_io
        common
    PRIVATE
        string_utils
)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()
