option (USE_INTERNAL_SPARSEHASH_LIBRARY "Set to FALSE to use system sparsehash library instead of bundled"
        ON) # ON by default as we are not aware of any system providing package for sparsehash-c11

if (NOT USE_INTERNAL_SPARSEHASH_LIBRARY)
    find_path (SPARSEHASH_INCLUDE_DIR NAMES sparsehash/sparse_hash_map PATHS ${SPARSEHASH_INCLUDE_PATHS})
    if (NOT SPARSEHASH_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system sparsehash library")
    endif ()
endif ()

if (SPARSEHASH_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_SPARSEHASH_LIBRARY 1)
    set (SPARSEHASH_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/sparsehash-c11")
endif ()

message (STATUS "Using sparsehash: ${SPARSEHASH_INCLUDE_DIR}")
