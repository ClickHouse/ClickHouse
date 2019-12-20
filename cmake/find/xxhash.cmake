option (USE_INTERNAL_XXHASH_LIBRARY "Set to FALSE to use system xxHash library instead of bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_XXHASH_LIBRARY AND NOT USE_INTERNAL_LZ4_LIBRARY)
    message (WARNING "can not use internal xxhash without internal lz4")
    set (USE_INTERNAL_XXHASH_LIBRARY 0)
endif ()

if (USE_INTERNAL_XXHASH_LIBRARY)
    set (XXHASH_LIBRARY lz4)
    set (XXHASH_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/lz4/lib)
else ()
    find_library (XXHASH_LIBRARY xxhash)
    find_path (XXHASH_INCLUDE_DIR NAMES xxhash.h PATHS ${XXHASH_INCLUDE_PATHS})
endif ()

if (XXHASH_LIBRARY AND XXHASH_INCLUDE_DIR)
    set (USE_XXHASH 1)
else ()
    set (USE_XXHASH 0)
endif ()

message (STATUS "Using xxhash=${USE_XXHASH}: ${XXHASH_INCLUDE_DIR} : ${XXHASH_LIBRARY}")
