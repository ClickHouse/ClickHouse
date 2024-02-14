include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_headers_and_sources(clickhouse_common_foundationdb .)
add_headers_and_sources(clickhouse_common_foundationdb ./internal)
add_library(clickhouse_common_foundationdb
    ${clickhouse_common_foundationdb_headers}
    ${clickhouse_common_foundationdb_sources}
    ${ClickHouse_SOURCE_DIR}/src/Core/BackgroundSchedulePool.cpp
)
target_compile_definitions (clickhouse_common_foundationdb PRIVATE -DZOOKEEPER_LOG)
target_link_libraries(clickhouse_common_foundationdb
    PUBLIC
        clickhouse_common_io
        common
    PRIVATE
        ch_contrib::fdb_c
        ch_contrib::fmt
)
if(SANITIZE STREQUAL "undefined")
    # See https://github.com/apple/foundationdb/pull/2393 and https://github.com/apple/foundationdb/issues/1434
    target_compile_options(clickhouse_common_foundationdb PRIVATE -fno-sanitize=alignment)
endif()
