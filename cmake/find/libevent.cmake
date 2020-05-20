SET(ENABLE_LIBEVENT 1)
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libevent/CMakeLists.txt")
    message (WARNING "submodule contrib/libevent is missing. to fix try run:
    \n git submodule update --init --recursive")

    set (ENABLE_LIBEVENT 0)
endif ()

if (ENABLE_LIBEVENT)

    set (USE_LIBEVENT 1)
    set (LIBEVENT_LIBRARY LIBEVENT)

    set (LIBEVENT_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libevent")

    list (APPEND LIBEVENT_INCLUDE_DIR
            "${ClickHouse_SOURCE_DIR}/contrib/libevent/include/event2"
            "${ClickHouse_SOURCE_DIR}/contrib/libevent/include")

endif()

message (STATUS "Using libevent=${USE_LIBEVENT}: ${LIBEVENT_INCLUDE_DIR} : ${LIBEVENT_LIBRARY}")
