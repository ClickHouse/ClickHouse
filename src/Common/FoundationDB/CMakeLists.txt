include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")

add_subdirectory("protos/")

add_headers_and_sources(clickhouse_common_foundationdb .)
add_library(clickhouse_common_foundationdb ${clickhouse_common_foundationdb_headers} ${clickhouse_common_foundationdb_sources})
target_link_libraries(clickhouse_common_foundationdb
    PUBLIC
        clickhouse_common_io
        common
        clickhouse_common_foundationdb_protos
    PRIVATE
        fdb_c
)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error -Wno-unused-parameter")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error -Wno-unused-parameter")