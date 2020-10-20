option(ENABLE_ROCKSDB "Enable ROCKSDB" ${ENABLE_LIBRARIES})

if (NOT ENABLE_ROCKSDB)
    if (USE_INTERNAL_ROCKSDB_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal rocksdb library with ENABLE_ROCKSDB=OFF")
    endif()
    return()
endif()

option(USE_INTERNAL_ROCKSDB_LIBRARY "Set to FALSE to use system ROCKSDB library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/rocksdb/CMakeLists.txt")
    if (USE_INTERNAL_ROCKSDB_LIBRARY)
        message (WARNING "submodule contrib is missing. to fix try run: \n git submodule update --init --recursive")
        message(${RECONFIGURE_MESSAGE_LEVEL} "cannot find internal rocksdb")
    endif()
    set (MISSING_INTERNAL_ROCKSDB 1)
endif ()

if (NOT USE_INTERNAL_ROCKSDB_LIBRARY)
    find_library (ROCKSDB_LIBRARY rocksdb)
    find_path (ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h PATHS ${ROCKSDB_INCLUDE_PATHS})
    if (NOT ROCKSDB_LIBRARY OR NOT ROCKSDB_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system rocksdb library")
    endif()
endif ()

if(ROCKSDB_LIBRARY AND ROCKSDB_INCLUDE_DIR)
    set(USE_ROCKSDB 1)
elseif (NOT MISSING_INTERNAL_ROCKSDB)
    set (USE_INTERNAL_ROCKSDB_LIBRARY 1)

    set (ROCKSDB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb/include")
    set (ROCKSDB_CORE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/rocksdb/include")
    set (ROCKSDB_LIBRARY "rocksdb")
    set (USE_ROCKSDB 1)
endif ()

message (STATUS "Using ROCKSDB=${USE_ROCKSDB}: ${ROCKSDB_INCLUDE_DIR} : ${ROCKSDB_LIBRARY}")