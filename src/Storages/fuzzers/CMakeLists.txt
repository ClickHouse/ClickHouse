
clickhouse_add_executable (mergetree_checksum_fuzzer mergetree_checksum_fuzzer.cpp)
target_link_libraries (mergetree_checksum_fuzzer PRIVATE dbms)

clickhouse_add_executable (columns_description_fuzzer columns_description_fuzzer.cpp)
target_link_libraries (columns_description_fuzzer PRIVATE dbms)

clickhouse_add_executable (skip_index_granule_fuzzer skip_index_granule_fuzzer.cpp)
target_link_libraries (skip_index_granule_fuzzer PRIVATE clickhouse_functions dbms)

clickhouse_add_executable (replication_log_fuzzer replication_log_fuzzer.cpp)
target_link_libraries (replication_log_fuzzer PRIVATE dbms)

# `src/Storages` is added before the target `clickhouse_pulsar_proto` is defined.
if (USE_PULSAR)
    clickhouse_add_executable (pulsar_frame_fuzzer pulsar_frame_fuzzer.cpp)
    target_link_libraries (pulsar_frame_fuzzer PRIVATE dbms clickhouse_pulsar_proto)
endif()
