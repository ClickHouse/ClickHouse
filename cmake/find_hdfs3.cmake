if (NOT ARCH_ARM)
    option (ENABLE_HDFS "Enable HDFS" ${NOT_UNBUNDLED})
endif ()

if (ENABLE_HDFS AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3/include/hdfs/hdfs.h")
   message (WARNING "submodule contrib/libhdfs3 is missing. to fix try run: \n git submodule update --init --recursive")
   set (ENABLE_HDFS  0)
endif ()

if (ENABLE_HDFS)
option (USE_INTERNAL_HDFS3_LIBRARY "Set to FALSE to use system HDFS3 instead of bundled" ON)

if (NOT USE_INTERNAL_HDFS3_LIBRARY)
    find_package(hdfs3)
endif ()

if (HDFS3_LIBRARY AND HDFS3_INCLUDE_DIR)
else ()
    set(HDFS3_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libhdfs3/include")
    set(HDFS3_LIBRARY hdfs3)
endif()
set (USE_HDFS 1)

endif()

message (STATUS "Using hdfs3: ${HDFS3_INCLUDE_DIR} : ${HDFS3_LIBRARY}")
