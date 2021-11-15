set (SRCS
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_constants.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_types.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/ThriftHiveMetastore.cpp
)

add_library(hivemetastore ${SRCS})
target_link_libraries(hivemetastore PUBLIC ${THRIFT_LIBRARY})
target_include_directories(hivemetastore SYSTEM PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore)
