option(ENABLE_CASSANDRA "Enable Cassandra" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CASSANDRA)
    return()
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libuv")
    message (ERROR "submodule contrib/libuv is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal libuv needed for Cassandra")
elseif (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cassandra")
    message (ERROR "submodule contrib/cassandra is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal Cassandra")
else()
    set (LIBUV_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/libuv")
    set (CASSANDRA_INCLUDE_DIR
            "${ClickHouse_SOURCE_DIR}/contrib/cassandra/include/")
    if (MAKE_STATIC_LIBRARIES)
        set (LIBUV_LIBRARY uv_a)
        set (CASSANDRA_LIBRARY cassandra_static)
    else()
        set (LIBUV_LIBRARY uv)
        set (CASSANDRA_LIBRARY cassandra)
    endif()

    set (USE_CASSANDRA 1)
    set (CASS_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/cassandra")
endif()

message (STATUS "Using cassandra=${USE_CASSANDRA}: ${CASSANDRA_INCLUDE_DIR} : ${CASSANDRA_LIBRARY}")
message (STATUS "Using libuv: ${LIBUV_ROOT_DIR} : ${LIBUV_LIBRARY}")
