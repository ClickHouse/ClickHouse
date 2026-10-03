option (ENABLE_MAXMINDDB "Enable the MaxMindDB table engine" ${ENABLE_LIBRARIES})

if (NOT ENABLE_MAXMINDDB)
    message (STATUS "Not using libmaxminddb")
    return ()
endif ()

set (MAXMINDDB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libmaxminddb")

# All supported Clang targets provide unsigned __int128.
set (MMDB_UINT128_USING_MODE 0)
set (MMDB_UINT128_IS_BYTE_ARRAY 0)
configure_file (
    "${MAXMINDDB_SOURCE_DIR}/include/maxminddb_config.h.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/maxminddb_config.h")

add_library (_maxminddb
    "${MAXMINDDB_SOURCE_DIR}/src/maxminddb.c"
    "${MAXMINDDB_SOURCE_DIR}/src/data-pool.c")
add_library (ch_contrib::maxminddb ALIAS _maxminddb)

target_include_directories (_maxminddb SYSTEM BEFORE PUBLIC
    "${CMAKE_CURRENT_BINARY_DIR}/include"
    "${MAXMINDDB_SOURCE_DIR}/include")
target_compile_definitions (_maxminddb PRIVATE PACKAGE_VERSION="1.14.1")
