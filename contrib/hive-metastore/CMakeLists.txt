set (SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/hive_metastore_constants.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/hive_metastore_types.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/ThriftHiveMetastore.cpp
)

set(THRIFT_COMPILER ${ClickHouse_BINARY_DIR}/contrib/arrow-cmake/thrift-compiler)
add_custom_command(
  OUTPUT ${SRCS}
  COMMAND thrift-compiler
  ARGS -r --gen cpp -out ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_LIST_DIR}/hive_metastore.thrift
  COMMENT "Running cpp thrift compiler for hive_metastore.thrift"
  VERBATIM)

add_library(hivemetastore ${SRCS})
add_dependencies(hivemetastore ${THRIFT_LIBRARY} thrift-compiler)
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

