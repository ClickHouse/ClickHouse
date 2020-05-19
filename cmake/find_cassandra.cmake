if (NOT DEFINED ENABLE_CASSANDRA OR ENABLE_CASSANDRA)
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cassandra")
        message (WARNING "submodule contrib/cassandra is missing. to fix try run: \n git submodule update --init --recursive")
    else()
        set (CASSANDRA_INCLUDE_DIR
                "${ClickHouse_SOURCE_DIR}/contrib/cassandra/include/")
        set (CASSANDRA_LIBRARY cassandra)
        set (USE_CASSANDRA 1)
        set(CASS_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/cassandra")

        message(STATUS "Using cassandra: ${CASSANDRA_LIBRARY}")
    endif()
endif()
