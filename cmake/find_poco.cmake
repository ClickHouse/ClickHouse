option (USE_INTERNAL_POCO_LIBRARY "Set to FALSE to use system poco library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/poco/CMakeLists.txt")
   if (USE_INTERNAL_POCO_LIBRARY)
      message (WARNING "submodule contrib/poco is missing. to fix try run: \n git submodule update --init --recursive")
   endif ()
   set (USE_INTERNAL_POCO_LIBRARY 0)
   set (MISSING_INTERNAL_POCO_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_POCO_LIBRARY)
    find_package (Poco COMPONENTS Net NetSSL XML Data Crypto DataODBC MongoDB)
endif ()

if (Poco_INCLUDE_DIRS AND Poco_Foundation_LIBRARY)
elseif (NOT MISSING_INTERNAL_POCO_LIBRARY)

    set (USE_INTERNAL_POCO_LIBRARY 1)

    set (ENABLE_ZIP 0 CACHE BOOL "")
    set (ENABLE_PAGECOMPILER 0 CACHE BOOL "")
    set (ENABLE_PAGECOMPILER_FILE2PAGE 0 CACHE BOOL "")
    set (ENABLE_REDIS 0 CACHE BOOL "")
    set (ENABLE_DATA_SQLITE 0 CACHE BOOL "")
    set (ENABLE_DATA_MYSQL 0 CACHE BOOL "")
    set (ENABLE_DATA_POSTGRESQL 0 CACHE BOOL "")
    set (POCO_UNBUNDLED 1 CACHE BOOL "")
    set (POCO_UNBUNDLED_PCRE 0 CACHE BOOL "")
    set (POCO_UNBUNDLED_EXPAT 0 CACHE BOOL "")
    set (POCO_STATIC ${MAKE_STATIC_LIBRARIES} CACHE BOOL "")
    set (POCO_VERBOSE_MESSAGES 1 CACHE BOOL "")


    # used in internal compiler
    list (APPEND Poco_INCLUDE_DIRS
        "${ClickHouse_SOURCE_DIR}/contrib/poco/Foundation/include/"
        "${ClickHouse_SOURCE_DIR}/contrib/poco/Util/include/"
    )

    if (NOT DEFINED POCO_ENABLE_MONGODB OR POCO_ENABLE_MONGODB)
        set (Poco_MongoDB_FOUND 1)
        set (Poco_MongoDB_LIBRARY PocoMongoDB)
        set (Poco_MongoDB_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/poco/MongoDB/include/")
    endif ()

    if (ODBC_FOUND)
        set (Poco_DataODBC_FOUND 1)
        set (Poco_DataODBC_LIBRARY PocoDataODBC ${ODBC_LIBRARIES} ${LTDL_LIBRARY})
        set (Poco_DataODBC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/poco/Data/ODBC/include/")
    endif ()

    # TODO! fix internal ssl
    if (OPENSSL_FOUND AND NOT USE_INTERNAL_SSL_LIBRARY)
        set (Poco_NetSSL_FOUND 1)
        set (Poco_NetSSL_LIBRARY PocoNetSSL)
        set (Poco_Crypto_LIBRARY PocoCrypto)
    endif ()

    if (USE_STATIC_LIBRARIES AND USE_INTERNAL_ZLIB_LIBRARY)
        list (APPEND Poco_INCLUDE_DIRS
            "${ClickHouse_SOURCE_DIR}/contrib/${INTERNAL_ZLIB_NAME}/"
            "${ClickHouse_BINARY_DIR}/contrib/${INTERNAL_ZLIB_NAME}/"
        )
    endif ()

    set (Poco_Foundation_LIBRARY PocoFoundation)
    set (Poco_Util_LIBRARY PocoUtil)
    set (Poco_Net_LIBRARY PocoNet)
    set (Poco_Data_LIBRARY PocoData)
    set (Poco_XML_LIBRARY PocoXML)
endif ()

message(STATUS "Using Poco: ${Poco_INCLUDE_DIRS} : ${Poco_Foundation_LIBRARY},${Poco_Util_LIBRARY},${Poco_Net_LIBRARY},${Poco_NetSSL_LIBRARY},${Poco_XML_LIBRARY},${Poco_Data_LIBRARY},${Poco_DataODBC_LIBRARY},${Poco_MongoDB_LIBRARY}; MongoDB=${Poco_MongoDB_FOUND}, DataODBC=${Poco_DataODBC_FOUND}, NetSSL=${Poco_NetSSL_FOUND}")

# How to make sutable poco:
# use branch:
#  develop  OR  poco-1.7.9-release + 6a49c94d18c654d7a20b8c8ea47071b1fdd4813b
# and merge:
# ClickHouse-Extras/clickhouse_unbundled
# ClickHouse-Extras/clickhouse_unbundled_zlib
# ClickHouse-Extras/clickhouse_task
# ClickHouse-Extras/clickhouse_misc
# ClickHouse-Extras/clickhouse_anl
# ClickHouse-Extras/clickhouse_http_header https://github.com/pocoproject/poco/pull/1574
# ClickHouse-Extras/clickhouse_socket
# ClickHouse-Extras/clickhouse_warning
# ClickHouse-Extras/clickhouse-purge-logs-on-no-space
# ClickHouse-Extras/clickhouse_freebsd
