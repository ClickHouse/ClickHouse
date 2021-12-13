if (MISSING_INTERNAL_LIBUV_LIBRARY)
    message (WARNING "Can't find internal libuv needed for AMQP-CPP library")
    set (ENABLE_AMQPCPP OFF CACHE INTERNAL "")
endif()

option(ENABLE_AMQPCPP "Enalbe AMQP-CPP" ${ENABLE_LIBRARIES})

if (NOT ENABLE_AMQPCPP)
    return()
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/CMakeLists.txt")
    message (WARNING "submodule contrib/AMQP-CPP is missing. to fix try run: \n git submodule update --init")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal AMQP-CPP library")
    set (USE_AMQPCPP 0)
    return()
endif ()

set (USE_AMQPCPP 1)
set (AMQPCPP_LIBRARY amqp-cpp ${OPENSSL_LIBRARIES})

set (AMQPCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include")
list (APPEND AMQPCPP_INCLUDE_DIR
        "${LIBUV_INCLUDE_DIR}"
        "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP")

list (APPEND AMQPCPP_LIBRARY  "${LIBUV_LIBRARY}")

message (STATUS "Using AMQP-CPP=${USE_AMQPCPP}: ${AMQPCPP_INCLUDE_DIR} : ${AMQPCPP_LIBRARY}")
