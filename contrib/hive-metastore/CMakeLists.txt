set (SRCS
    ${CMAKE_CURRENT_LIST_DIR}/hive_metastore_constants.cpp
    ${CMAKE_CURRENT_LIST_DIR}/hive_metastore_types.cpp
    ${CMAKE_CURRENT_LIST_DIR}/ThriftHiveMetastore.cpp
)

# set(THRIFT_COMPILER_PATH ${ClickHouse_BINARY_DIR}/contrib/thrift-cmake/thrift-compiler)
# add_custom_command(
#   OUTPUT ${SRCS}
#   COMMAND ${THRIFT_COMPILER_PATH}
#   ARGS -r --gen cpp -out ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_LIST_DIR}/hive_metastore.thrift
#   COMMENT "Running cpp thrift compiler for hive_metastore.thrift"
#   DEPENDS ${THRIFT_COMPILER}
#   VERBATIM)

add_library(hivemetastore ${SRCS})
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM PUBLIC ${CMAKE_CURRENT_LIST_DIR})
