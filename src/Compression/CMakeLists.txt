include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(clickhouse_compression .)

# Remove this file, because it has dependencies on DataTypes
list(REMOVE_ITEM ${clickhouse_compression_sources} CompressionFactoryAdditions.cpp)

add_library(clickhouse_compression ${clickhouse_compression_headers} ${clickhouse_compression_sources})
target_link_libraries(clickhouse_compression PUBLIC clickhouse_parsers clickhouse_common_io common lz4)

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
