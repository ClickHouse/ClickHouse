option (ENABLE_H3 "Enable H3" ${ENABLE_LIBRARIES})
if (ENABLE_H3)

option (USE_INTERNAL_H3_LIBRARY "Set to FALSE to use system h3 library instead of bundled" ${NOT_UNBUNDLED})

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/h3/src/h3lib/include/h3Index.h")
    if(USE_INTERNAL_H3_LIBRARY)
       message(WARNING "submodule contrib/h3 is missing. to fix try run: \n git submodule update --init --recursive")
    endif()
    set(MISSING_INTERNAL_H3_LIBRARY 1)
    set(USE_INTERNAL_H3_LIBRARY 0)
endif()

if (USE_INTERNAL_H3_LIBRARY)
    set (H3_LIBRARY h3)
    set (H3_INCLUDE_DIR ${ClickHouse_SOURCE_DIR}/contrib/h3/src/h3lib/include)
elseif (NOT MISSING_INTERNAL_H3_LIBRARY)
    find_library (H3_LIBRARY h3)
    find_path (H3_INCLUDE_DIR NAMES h3/h3api.h PATHS ${H3_INCLUDE_PATHS})
endif ()

if (H3_LIBRARY AND H3_INCLUDE_DIR)
    set (USE_H3 1)
endif ()

endif ()

message (STATUS "Using h3=${USE_H3}: ${H3_INCLUDE_DIR} : ${H3_LIBRARY}")
