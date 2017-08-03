option (USE_INTERNAL_CCTZ_LIBRARY "Set to FALSE to use system cctz library instead of bundled" ${NOT_UNBUNDLED})

if (NOT USE_INTERNAL_CCTZ_LIBRARY)
    find_library (CCTZ_LIBRARY cctz)
    find_path (CCTZ_INCLUDE_DIR NAMES civil_time.h PATHS ${CCTZ_INCLUDE_PATHS})
endif ()

if (CCTZ_LIBRARY AND CCTZ_INCLUDE_DIR)
else ()
    set (USE_INTERNAL_CCTZ_LIBRARY 1)
    set (CCTZ_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libcctz/include")
    set (CCTZ_LIBRARY cctz)
endif ()

message (STATUS "Using cctz: ${CCTZ_INCLUDE_DIR} : ${CCTZ_LIBRARY}")
