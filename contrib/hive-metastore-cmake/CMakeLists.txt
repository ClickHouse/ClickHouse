set (SRCS
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_constants.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_types.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/ThriftHiveMetastore.cpp
)

add_library(${HIVE_METASTORE_LIBRARY} ${SRCS})
target_link_libraries(${HIVE_METASTORE_LIBRARY} PUBLIC ${THRIFT_LIBRARY})
target_include_directories(${HIVE_METASTORE_LIBRARY} SYSTEM PUBLIC ${HIVE_METASTORE_INCLUDE_DIR})
