if (ENABLE_FUZZING)
    include("${ClickHouse_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
    add_headers_and_sources(fuzz_compression .)

    # Remove this file, because it has dependencies on DataTypes
    list(REMOVE_ITEM ${fuzz_compression_sources} CompressionFactoryAdditions.cpp)

    add_library(fuzz_compression ${fuzz_compression_headers} ${fuzz_compression_sources})
    target_link_libraries(fuzz_compression PUBLIC clickhouse_parsers clickhouse_common_io common ch_contrib::lz4)
endif()

if (ENABLE_EXAMPLES)
    add_subdirectory(examples)
endif()

if (ENABLE_FUZZING)
    add_subdirectory(fuzzers)
endif()
