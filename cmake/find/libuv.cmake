if (OS_DARWIN AND COMPILER_GCC)
    message (WARNING "libuv cannot be built with GCC in macOS due to a bug: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=93082")
    SET(MISSING_INTERNAL_LIBUV_LIBRARY 1)
    return()
endif()

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
