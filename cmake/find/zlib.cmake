option (USE_INTERNAL_ZLIB_LIBRARY "Set to FALSE to use system zlib library instead of bundled" ${NOT_UNBUNDLED})

set (INTERNAL_ZLIB_NAME "zlib" CACHE INTERNAL "")

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng/zlib.h")
    if(USE_INTERNAL_ZLIB_LIBRARY)
        message(WARNING "submodule contrib/zlib-ng is missing. to fix try run: \n git submodule update --init --recursive")
    endif()
    set(USE_INTERNAL_ZLIB_LIBRARY 0)
    set(MISSING_INTERNAL_ZLIB_LIBRARY 1)
endif()

if (NOT USE_INTERNAL_ZLIB_LIBRARY)
    find_package (ZLIB)
endif ()

if (NOT ZLIB_FOUND AND NOT MISSING_INTERNAL_ZLIB_LIBRARY)
    set (USE_INTERNAL_ZLIB_LIBRARY 1)
    set (ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng" "${ClickHouse_SOURCE_DIR}/contrib/zlib-ng-cmake" CACHE INTERNAL "")
    set (ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR}) # for poco
    set (ZLIB_INCLUDE_DIRECTORIES ${ZLIB_INCLUDE_DIR}) # for protobuf
    set (ZLIB_FOUND 1) # for poco
    set (ZLIB_LIBRARIES zlib CACHE INTERNAL "")
endif ()

message (STATUS "Using zlib: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
