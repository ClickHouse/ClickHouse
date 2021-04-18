
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libuv")
    message (WARNING "submodule contrib/libuv is missing. to fix try run: \n git submodule update --init --recursive")
    SET(MISSING_INTERNAL_LIBUV_LIBRARY 1)
    return()
endif()

if (MAKE_STATIC_LIBRARIES)
    set (LIBUV_LIBRARY uv_a)
else()
    set (LIBUV_LIBRARY uv)
endif()

set (LIBUV_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/libuv")
set (LIBUV_INCLUDE_DIR "${LIBUV_ROOT_DIR}/include")

message (STATUS "Using libuv: ${LIBUV_ROOT_DIR} : ${LIBUV_LIBRARY}")
