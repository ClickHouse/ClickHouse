option (USE_INTERNAL_ZLIB_LIBRARY "Set to FALSE to use system zlib library instead of bundled" ${NOT_UNBUNDLED})

if (NOT MSVC)
    set (INTERNAL_ZLIB_NAME "zlib-ng" CACHE INTERNAL "")
else ()
    set (INTERNAL_ZLIB_NAME "zlib" CACHE INTERNAL "")
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
        message (WARNING "Will use standard zlib, please clone manually:\n git clone https://github.com/madler/zlib.git ${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal zlib library")
    endif ()
endif ()

if(NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}/zlib.h")
    if(USE_INTERNAL_ZLIB_LIBRARY)
        message(WARNING "submodule contrib/${INTERNAL_ZLIB_NAME} is missing. to fix try run: \n git submodule update --init")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal zlib library")
    endif()
    set(USE_INTERNAL_ZLIB_LIBRARY 0)
    set(MISSING_INTERNAL_ZLIB_LIBRARY 1)
endif()

if (NOT USE_INTERNAL_ZLIB_LIBRARY)
    find_package (ZLIB)
    if (NOT ZLIB_FOUND)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system zlib library")
    else()
        set (ZLIB_NAME "libz")
    endif()
endif ()

if (NOT ZLIB_FOUND AND NOT MISSING_INTERNAL_ZLIB_LIBRARY)
    # https://github.com/zlib-ng/zlib-ng/pull/733
    # This is disabed by default
    add_compile_definitions(Z_TLS=__thread)
    set (USE_INTERNAL_ZLIB_LIBRARY 1)
    set (ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}" "${ClickHouse_BINARY_DIR}/contrib/${INTERNAL_ZLIB_NAME}" CACHE INTERNAL "") # generated zconf.h
    set (ZLIB_INCLUDE_DIRS ${ZLIB_INCLUDE_DIR}) # for poco
    set (ZLIB_INCLUDE_DIRECTORIES ${ZLIB_INCLUDE_DIR}) # for protobuf
    set (ZLIB_FOUND 1) # for poco
    set (ZLIB_LIBRARIES zlib CACHE INTERNAL "")
    set (ZLIB_LIBRARY_NAME ${ZLIB_LIBRARIES}) # for cassandra
    set (ZLIB_NAME "${INTERNAL_ZLIB_NAME}")
endif ()

message (STATUS "Using ${ZLIB_NAME}: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
