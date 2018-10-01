include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_parsers .)
add_library(clickhouse_parsers ${LINK_MODE} ${clickhouse_parsers_headers} ${clickhouse_parsers_sources})
target_link_libraries (clickhouse_parsers clickhouse_common_io)
target_include_directories (clickhouse_parsers PUBLIC ${DBMS_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
