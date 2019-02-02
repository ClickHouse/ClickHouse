option (USE_INTERNAL_BROTLI_LIBRARY "Set to FALSE to use system libbrotli library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/brotli/c/include/brotli/decode.h")
    if (USE_INTERNAL_BROTLI_LIBRARY)
        message (WARNING "submodule contrib/brotli is missing. to fix try run: \n git submodule update --init --recursive")
        set (USE_INTERNAL_BROTLI_LIBRARY 0)
    endif ()
    set (MISSING_INTERNAL_BROTLI_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_BROTLI_LIBRARY)
    find_library (BROTLI_LIBRARY brotli)
    find_path (BROTLI_INCLUDE_DIR NAMES decode.h encode.h port.h types.h PATHS ${BROTLI_INCLUDE_PATHS})
endif ()

if (BROTLI_LIBRARY AND BROTLI_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_BROTLI_LIBRARY)
    set (BROTLI_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/brotli/c/include)
    set (USE_INTERNAL_BROTLI_LIBRARY 1)
    set (BROTLI_LIBRARY brotli)
    set (USE_BROTLI 1)
endif ()

message (STATUS "Using brotli: ${BROTLI_INCLUDE_DIR} : ${BROTLI_LIBRARY}")
