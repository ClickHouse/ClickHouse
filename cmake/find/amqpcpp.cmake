if (OS_DARWIN AND COMPILER_GCC)
    # AMQP-CPP requires libuv which cannot be built with GCC in macOS due to a bug: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=93082
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
endif()

option(ENABLE_AMQPCPP "Enalbe AMQP-CPP" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AMQPCPP)
    return()
endif()


if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libuv")
    message (ERROR "submodule contrib/libuv is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libuv needed for AMQP-CPP library")
    set (USE_AMQPCPP 0)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/CMakeLists.txt")
    message (WARNING "submodule contrib/AMQP-CPP is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal AMQP-CPP library")
    set (USE_AMQPCPP 0)
    return()
endif ()

set (USE_AMQPCPP 1)
set (AMQPCPP_LIBRARY amqp-cpp)

if (MAKE_STATIC_LIBRARIES)
    set (LIBUV_LIBRARY uv_a)
else()
    set (LIBUV_LIBRARY uv)
endif()

set (LIBUV_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/libuv")
set (LIBUV_INCLUDE_DIR "${LIBUV_ROOT_DIR}/include")

set (AMQPCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include")
list (APPEND AMQPCPP_INCLUDE_DIR
        "${LIBUV_INCLUDE_DIR}"
        "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP")

list (APPEND AMQPCPP_LIBRARY  "${LIBUV_LIBRARY}")

# Assign libuv include and libraries
#set(CASS_LIBS ${CASS_LIBS} ${LIBUV_LIBRARIES})

message (STATUS "Using AMQP-CPP=${USE_AMQPCPP}: ${AMQPCPP_INCLUDE_DIR} : ${AMQPCPP_LIBRARY}")
message (STATUS "Using libuv: ${LIBUV_ROOT_DIR} : ${LIBUV_LIBRARY}")
