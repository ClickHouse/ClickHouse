set (SRCS
    hive_metastore_constants.cpp
    hive_metastore_types.cpp
    ThriftHiveMetastore.cpp
)

add_library(hivemetastore ${SRCS})
add_dependencies(hivemetastore ${THRIFT_LIBRARY})
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore)