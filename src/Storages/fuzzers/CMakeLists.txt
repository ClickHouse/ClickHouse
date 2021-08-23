
add_executable (mergetree_checksum_fuzzer
    mergetree_checksum_fuzzer.cpp
    "${ClickHouse_SOURCE_DIR}/src/Storages/MergeTree/MergeTreeDataPartChecksum.cpp"
    "${ClickHouse_SOURCE_DIR}/src/Compression/CompressedReadBuffer.cpp"
    "${ClickHouse_SOURCE_DIR}/src/Compression/CompressedWriteBuffer.cpp"
)
target_link_libraries (mergetree_checksum_fuzzer PRIVATE clickhouse_common_io fuzz_compression ${LIB_FUZZING_ENGINE})

add_executable (columns_description_fuzzer columns_description_fuzzer.cpp)
target_link_libraries (columns_description_fuzzer PRIVATE dbms ${LIB_FUZZING_ENGINE})
