if (USE_ROCKSDB)
    add_executable (merge_tree_metadata_cache merge_tree_metadata_cache.cpp)
    target_link_libraries (merge_tree_metadata_cache PRIVATE dbms)
endif()