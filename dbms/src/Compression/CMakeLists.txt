include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_compression .)
add_library(clickhouse_compression ${LINK_MODE} ${clickhouse_compression_headers} ${clickhouse_compression_sources})
target_link_libraries(clickhouse_compression PRIVATE clickhouse_parsers clickhouse_common_io ${ZSTD_LIBRARY})
target_include_directories(clickhouse_compression PUBLIC ${DBMS_INCLUDE_DIR})

#if(ENABLE_TESTS)
#    add_subdirectory(tests)
#endif()
