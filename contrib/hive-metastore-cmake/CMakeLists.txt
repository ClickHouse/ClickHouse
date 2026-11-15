if (NOT TARGET ch_contrib::hdfs)
    if (ENABLE_HIVE)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use Hive without HDFS")
    endif()
# `Storages/Hive/HiveFile.h` uses the ORC reader and `parquet/arrow/reader.h` outright,
# so Hive needs both readers, not just the metastore client. The ORC reader needs only snappy.
elseif (NOT TARGET ch_contrib::snappy OR NOT TARGET _parquet)
    if (ENABLE_HIVE)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Cannot use Hive without ORC and Parquet")
    endif()
else()
    option(ENABLE_HIVE "Enable Hive" ${ENABLE_LIBRARIES})
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
