set (SRCS
    ${CMAKE_CURRENT_LIST_DIR}/hive_metastore_constants.cpp
    ${CMAKE_CURRENT_LIST_DIR}/hive_metastore_types.cpp
    ${CMAKE_CURRENT_LIST_DIR}/ThriftHiveMetastore.cpp
)

add_library(hivemetastore ${SRCS})
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM PUBLIC ${CMAKE_CURRENT_LIST_DIR})
