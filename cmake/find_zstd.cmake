option (USE_INTERNAL_ZSTD_LIBRARY "Set to FALSE to use system zstd library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_ZSTD_LIBRARY)
    find_library (ZSTD_LIBRARY zstd)
    find_path (ZSTD_INCLUDE_DIR NAMES zstd.h PATHS ${ZSTD_INCLUDE_PATHS})
endif ()

if (ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
    include_directories (${ZSTD_INCLUDE_DIR})
else ()
    set (USE_INTERNAL_ZSTD_LIBRARY 1)
    set (ZSTD_LIBRARY zstd)
endif ()

message (STATUS "Using zstd: ${ZSTD_INCLUDE_DIR} : ${ZSTD_LIBRARY}")
