option (ENABLE_PROTOBUF "Enable Protobuf" ON)

if (ENABLE_PROTOBUF)

INCLUDE(FindProtobuf)
FIND_PACKAGE(Protobuf REQUIRED)
INCLUDE_DIRECTORIES(${PROTOBUF_INCLUDE_DIR})

set (USE_PROTOBUF 1)

#[[TODO:
option (USE_INTERNAL_RDKAFKA_LIBRARY "Set to FALSE to use system libprotobuf instead of the bundled" ${NOT_UNBUNDLED})

endif ()

if (USE_PROTOBUF)
    message (STATUS "Using protobuf=${USE_PROTOBUF}: ${PROTOBUF_INCLUDE_DIR} : ${PROTOBUF_LIBS}")
else ()
    message (STATUS "Build without capnp (support for Cap'n Proto format will be disabled)")]]

endif ()
