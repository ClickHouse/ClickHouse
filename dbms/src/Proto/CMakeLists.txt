set (CAPNP_PATH ${CMAKE_BINARY_DIR}/contrib/capnproto/c++/src/capnp)
set (CAPNP_BIN ${CAPNP_PATH}/capnp)

add_custom_command (OUTPUT ServerMessage.capnp.c++ ServerMessage.capnp.h
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/ServerMessage.capnp ${CMAKE_CURRENT_BINARY_DIR}/ServerMessage.capnp
    COMMAND ${CMAKE_COMMAND} -E env PATH=${CAPNP_PATH} ${CAPNP_BIN} compile -I ${CAPNP_INCLUDE_DIR} -oc++ ServerMessage.capnp
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR} DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ServerMessage.capnp)

add_library (clickhouse_proto ServerMessage.capnp.c++ protoHelpers.cpp)
target_link_libraries (clickhouse_proto clickhouse_common_io ${CAPNP_LIBRARY})
target_include_directories (clickhouse_proto PUBLIC ${CAPNP_INCLUDE_DIR} ${DBMS_INCLUDE_DIR})
target_include_directories (clickhouse_proto PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR} ${COMMON_INCLUDE_DIR} ${DBMS_INCLUDE_DIR} ${CITYHASH_CONTRIB_INCLUDE_DIR})
