option(ENABLE_AMQPCPP "Enalbe AMQP-CPP" ${ENABLE_LIBRARIES})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/CMakeLists.txt")
    message (WARNING "submodule contrib/AMQP-CPP is missing. to fix try run: \n git submodule update --init --recursive")
    set (ENABLE_AMQPCPP 0)
endif ()

if (ENABLE_AMQPCPP)

    set (USE_AMQPCPP 1)
    set (AMQPCPP_LIBRARY AMQP-CPP)

    set (AMQPCPP_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include")

    list (APPEND AMQPCPP_INCLUDE_DIR
            "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP/include"
            "${ClickHouse_SOURCE_DIR}/contrib/AMQP-CPP")

endif()

message (STATUS "Using AMQP-CPP=${USE_AMQPCPP}: ${AMQPCPP_INCLUDE_DIR} : ${AMQPCPP_LIBRARY}")
