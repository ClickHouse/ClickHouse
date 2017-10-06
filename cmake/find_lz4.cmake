option (USE_INTERNAL_LZ4_LIBRARY "Set to FALSE to use system lz4 library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_LZ4_LIBRARY)
    find_library (LZ4_LIBRARY lz4)
    find_path (LZ4_INCLUDE_DIR NAMES lz4.h PATHS ${LZ4_INCLUDE_PATHS})
endif ()

if (LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
    include_directories (${LZ4_INCLUDE_DIR})
else ()
    set (LZ4_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/liblz4/include/lz4)
    set (USE_INTERNAL_LZ4_LIBRARY 1)
    set (LZ4_LIBRARY lz4)
endif ()

message (STATUS "Using lz4: ${LZ4_INCLUDE_DIR} : ${LZ4_LIBRARY}")
