if (TARGET ch_contrib::hdfs)
    option(ENABLE_HIVE "Enable Hive" ${ENABLE_LIBRARIES})
elseif(ENABLE_HIVE)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use Hive without HDFS")
endif()

if (NOT ENABLE_HIVE)
    message(STATUS "Not using hive")
    return()
endif()

set (SRCS
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_constants.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/hive_metastore_types.cpp
    ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore/ThriftHiveMetastore.cpp
)

add_library(_hivemetastore ${SRCS})
add_library(ch_contrib::hivemetastore ALIAS _hivemetastore)
target_link_libraries(_hivemetastore PUBLIC ch_contrib::thrift)
target_include_directories(_hivemetastore SYSTEM BEFORE PUBLIC ${ClickHouse_SOURCE_DIR}/contrib/hive-metastore)
