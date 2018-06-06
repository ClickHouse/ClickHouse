option (USE_INTERNAL_SPARCEHASH_LIBRARY "Set to FALSE to use system sparsehash library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_SPARCEHASH_LIBRARY)
    find_path (SPARCEHASH_INCLUDE_DIR NAMES sparsehash/sparse_hash_map PATHS ${SPARCEHASH_INCLUDE_PATHS})
endif ()

if (SPARCEHASH_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_SPARCEHASH_LIBRARY 1)
    set (SPARCEHASH_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libsparsehash")
endif ()

message (STATUS "Using sparsehash: ${SPARCEHASH_INCLUDE_DIR}")
