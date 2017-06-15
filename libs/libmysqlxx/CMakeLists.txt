include_directories (include)
include_directories (${ClickHouse_SOURCE_DIR}/libs/libcommon/include)
include(${ClickHouse_SOURCE_DIR}/cmake/dbms_include.cmake)

add_library (mysqlxx
    src/Connection.cpp
    src/Exception.cpp
    src/Query.cpp
    src/ResultBase.cpp
    src/StoreQueryResult.cpp
    src/UseQueryResult.cpp
    src/Row.cpp
    src/Value.cpp
    src/Pool.cpp
    src/PoolWithFailover.cpp

    include/mysqlxx/Connection.h
    include/mysqlxx/Exception.h
    include/mysqlxx/Manip.h
    include/mysqlxx/mysqlxx.h
    include/mysqlxx/Null.h
    include/mysqlxx/Pool.h
    include/mysqlxx/PoolWithFailover.h
    include/mysqlxx/Query.h
    include/mysqlxx/ResultBase.h
    include/mysqlxx/Row.h
    include/mysqlxx/StoreQueryResult.h
    include/mysqlxx/Transaction.h
    include/mysqlxx/Types.h
    include/mysqlxx/UseQueryResult.h
    include/mysqlxx/Value.h
)

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

if (USE_STATIC_LIBRARIES AND STATIC_MYSQLCLIENT_LIB)
    set (MYSQLCLIENT_LIB mariadbclient)
endif ()

target_link_libraries (mysqlxx common ${MYSQLCLIENT_LIB} ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${PLATFORM_LIBS})

if (ENABLE_TESTS)
    add_subdirectory (src/tests)
endif ()
