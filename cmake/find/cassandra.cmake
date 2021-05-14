if (MISSING_INTERNAL_LIBUV_LIBRARY)
    message (WARNING "Disabling cassandra due to missing libuv")
    set (ENABLE_CASSANDRA OFF CACHE INTERNAL "")
endif()

option(ENABLE_CASSANDRA "Enable Cassandra" ${ENABLE_LIBRARIES})

if (NOT ENABLE_CASSANDRA)
    return()
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
endif()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cassandra")
    message (ERROR "submodule contrib/cassandra is missing. to fix try run: \n git submodule update --init --recursive")
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal Cassandra")
    set (USE_CASSANDRA 0)
    return()
endif()

set (USE_CASSANDRA 1)
set (CASSANDRA_INCLUDE_DIR
        "${ClickHouse_SOURCE_DIR}/contrib/cassandra/include/")
if (MAKE_STATIC_LIBRARIES)
    set (CASSANDRA_LIBRARY cassandra_static)
else()
    set (CASSANDRA_LIBRARY cassandra)
endif()

set (CASS_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/cassandra")

message (STATUS "Using cassandra=${USE_CASSANDRA}: ${CASSANDRA_INCLUDE_DIR} : ${CASSANDRA_LIBRARY}")
