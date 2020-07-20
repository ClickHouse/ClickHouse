add_executable (part_name part_name.cpp)
target_link_libraries (part_name PRIVATE dbms)

add_executable (remove_symlink_directory remove_symlink_directory.cpp)
target_link_libraries (remove_symlink_directory PRIVATE dbms)

add_executable (merge_selector merge_selector.cpp)
target_link_libraries (merge_selector PRIVATE dbms)

add_executable (merge_selector2 merge_selector2.cpp)
target_link_libraries (merge_selector2 PRIVATE dbms)

add_executable (get_current_inserts_in_replicated get_current_inserts_in_replicated.cpp)
target_link_libraries (get_current_inserts_in_replicated PRIVATE dbms clickhouse_common_config clickhouse_common_zookeeper string_utils)

add_executable (get_abandonable_lock_in_all_partitions get_abandonable_lock_in_all_partitions.cpp)
target_link_libraries (get_abandonable_lock_in_all_partitions PRIVATE dbms clickhouse_common_config clickhouse_common_zookeeper)

add_executable (transform_part_zk_nodes transform_part_zk_nodes.cpp)
target_link_libraries (transform_part_zk_nodes
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_zookeeper
        dbms
        string_utils
)

if (ENABLE_FUZZING)
    add_executable (mergetree_checksum_fuzzer mergetree_checksum_fuzzer.cpp)
    target_link_libraries (mergetree_checksum_fuzzer PRIVATE dbms ${LIB_FUZZING_ENGINE})
endif ()
