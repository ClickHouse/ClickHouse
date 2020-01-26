SET(ENABLE_AMQPCPP 1)
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/amqpcpp/CMakeLists.txt")
    message (WARNING "submodule contrib/amqpcpp is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_AMQPCPP 0)
endif ()

if (ENABLE_AMQPCPP)

    set (USE_AMQPCPP 1)
    set (AMQPCPP_LIBRARY "amqpcpp")

    set (AMQPCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/amqpcpp/include")

    list (APPEND AMQPCPP_INCLUDE_DIR
            "${ClickHouse_SOURCE_DIR}/contrib/amqpcpp/include/src"
            "${ClickHouse_SOURCE_DIR}/contrib/amqpcpp/include"
            "${ClickHouse_SOURCE_DIR}/contrib/amqpcpp"
            )


endif()