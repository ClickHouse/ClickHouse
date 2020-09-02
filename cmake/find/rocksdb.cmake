option(ENABLE_ROCKSDB "Enable ROCKSDB" ${ENABLE_LIBRARIES})
option(USE_INTERNAL_ROCKSDB_LIBRARY "Set to FALSE to use system ROCKSDB library instead of bundled" ${NOT_UNBUNDLED})

if(ENABLE_ROCKSDB)
    if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/rocksdb")
        message (WARNING "submodule contrib is missing. to fix try run: \n git submodule update --init --recursive")
        set (MISSING_ROCKSDB 1)
    endif ()

    if (USE_INTERNAL_ROCKSDB_LIBRARY AND NOT MISSING_ROCKSDB)
        set (ROCKSDB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb/include")
        set (ROCKSDB_CORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb/include")
        set (ROCKSDB_LIBRARY "rocksdb")
        set (USE_INTERNAL_ROCKSDB_LIBRARY 1)
        set (USE_ROCKSDB 1)
    else()
        find_package(ROCKSDB)
    endif ()

endif()

message (STATUS "Using ROCKSDB=${USE_ROCKSDB}: ${ROCKSDB_INCLUDE_DIR} : ${ROCKSDB_LIBRARY}")