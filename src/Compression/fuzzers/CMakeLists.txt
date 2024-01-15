# Our code has strong cohesion and target associated with `Compression` also depends on `DataTypes`.
# But we can exclude some files which have dependencies in case of
# fuzzer related build (we are interested in fuzzing only particular part of our code).
# So, some symbols will be declared, but not defined. Unfortunately, this trick doesn't work with UBSan.
# If you want really small size of the resulted binary, just link with fuzz_compression and clickhouse_common_io

clickhouse_add_executable (compressed_buffer_fuzzer compressed_buffer_fuzzer.cpp)
target_link_libraries (compressed_buffer_fuzzer PRIVATE dbms)

clickhouse_add_executable (lz4_decompress_fuzzer lz4_decompress_fuzzer.cpp)
target_link_libraries (lz4_decompress_fuzzer PUBLIC dbms ch_contrib::lz4)

clickhouse_add_executable (delta_decompress_fuzzer delta_decompress_fuzzer.cpp)
target_link_libraries (delta_decompress_fuzzer PRIVATE dbms)

clickhouse_add_executable (double_delta_decompress_fuzzer double_delta_decompress_fuzzer.cpp)
target_link_libraries (double_delta_decompress_fuzzer PRIVATE dbms)

clickhouse_add_executable (encrypted_decompress_fuzzer encrypted_decompress_fuzzer.cpp)
target_link_libraries (encrypted_decompress_fuzzer PRIVATE dbms)

clickhouse_add_executable (gcd_decompress_fuzzer gcd_decompress_fuzzer.cpp)
target_link_libraries (gcd_decompress_fuzzer PRIVATE dbms)
