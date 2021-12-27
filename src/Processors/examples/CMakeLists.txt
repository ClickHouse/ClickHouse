if (USE_ROCKSDB)
    add_executable (merge_tree_meta_cache merge_tree_meta_cache.cpp)
    target_link_libraries (merge_tree_meta_cache PRIVATE dbms)
endif()
