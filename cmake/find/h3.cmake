option (USE_INTERNAL_H3_LIBRARY "Set to FALSE to use system h3 library instead of bundled" ${NOT_UNBUNDLED})

set (H3_INCLUDE_PATHS /usr/local/include/h3)

if (USE_INTERNAL_H3_LIBRARY)
    set (H3_LIBRARY h3)
    set (H3_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/h3/src/h3lib/include)
else ()
    find_library (H3_LIBRARY h3)
    find_path (H3_INCLUDE_DIR NAMES h3api.h PATHS ${H3_INCLUDE_PATHS})
endif ()

if (H3_LIBRARY AND H3_INCLUDE_DIR)
    set (USE_H3 1)
else ()
    set (USE_H3 0)
endif ()

message (STATUS "Using h3=${USE_H3}: ${H3_INCLUDE_DIR} : ${H3_LIBRARY}")
