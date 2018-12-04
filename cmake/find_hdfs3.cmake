option (USE_INTERNAL_HDFS3_LIBRARY "Set to FALSE to use system HDFS3 instead of bundled" ON)

if (NOT USE_INTERNAL_HDFS3_LIBRARY)
    find_package(hdfs3)
endif ()

if (HDFS3_LIBRARY AND HDFS3_INCLUDE_DIR)
else ()
    set(HDFS3_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3/include")
    set(HDFS3_LIBRARY hdfs3)
endif()

message (STATUS "Using hdfs3: ${HDFS3_INCLUDE_DIR} : ${HDFS3_LIBRARY}")
