include(${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake)
add_headers_and_sources(clickhouse_compression .)

add_library(clickhouse_compression ${clickhouse_compression_sources})
target_link_libraries(clickhouse_compression PRIVATE clickhouse_parsers ${LZ4_LIBRARY})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    target_include_directories(clickhouse_compression SYSTEM BEFORE PRIVATE ${LZ4_INCLUDE_DIR})
endif ()

if (ZSTD_LIBRARY)
    target_link_libraries(clickhouse_compression PRIVATE ${ZSTD_LIBRARY})
endif()

if (NOT USE_INTERNAL_ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    target_include_directories(clickhouse_compression SYSTEM BEFORE PRIVATE ${ZSTD_INCLUDE_DIR})
endif ()

if(ENABLE_TESTS)
    add_subdirectory(tests)
endif()
