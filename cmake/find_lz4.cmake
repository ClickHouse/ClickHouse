option (USE_INTERNAL_LZ4_LIBRARY "Set to FALSE to use system lz4 library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/lz4/lib/lz4.h")
    if (USE_INTERNAL_LZ4_LIBRARY)
       message (WARNING "submodule contrib/lz4 is missing. to fix try run: \n git submodule update --init --recursive")
       set (USE_INTERNAL_LZ4_LIBRARY 0)
    endif ()
    set (MISSING_INTERNAL_LZ4_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    find_library (LZ4_LIBRARY lz4)
    find_path (LZ4_INCLUDE_DIR NAMES lz4.h PATHS ${LZ4_INCLUDE_PATHS})
endif ()

if (LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
elseif (NOT MISSING_INTERNAL_LZ4_LIBRARY)
    set (LZ4_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/lz4/lib)
    set (USE_INTERNAL_LZ4_LIBRARY 1)
    set (LZ4_LIBRARY lz4)
endif ()

message (STATUS "Using lz4: ${LZ4_INCLUDE_DIR} : ${LZ4_LIBRARY}")
