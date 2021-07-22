if (OS_DARWIN AND COMPILER_GCC)
    # AMQP-CPP requires libuv which cannot be built with GCC in macOS due to a bug: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=93082
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
endif()

option(ENABLE_AMQPCPP "Enalbe AMQP-CPP" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AMQPCPP)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/CMakeLists.txt")
    message (WARNING "submodule contrib/AMQP-CPP is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal AMQP-CPP library")
    set (USE_AMQPCPP 0)
    return()
endif ()

set (USE_AMQPCPP 1)
set (AMQPCPP_LIBRARY AMQP-CPP)

set (AMQPCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include")
list (APPEND AMQPCPP_INCLUDE_DIR
        "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include"
        "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP")

message (STATUS "Using AMQP-CPP=${USE_AMQPCPP}: ${AMQPCPP_INCLUDE_DIR} : ${AMQPCPP_LIBRARY}")
