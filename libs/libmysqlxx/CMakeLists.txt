add_library (mysqlxx ${SPLIT_SHARED}
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

target_include_directories (mysqlxx PUBLIC include)

set(PLATFORM_LIBRARIES ${CMAKE_DL_LIBS})

if (USE_MYSQL)
    target_include_directories (mysqlxx SYSTEM PRIVATE ${MYSQL_INCLUDE_DIR})
endif ()

if (USE_STATIC_LIBRARIES AND STATIC_MYSQLCLIENT_LIB)
    set (MYSQLCLIENT_LIB ${CMAKE_CURRENT_BINARY_DIR}/libmysqlclient.a)
    add_custom_command (
        OUTPUT ${MYSQLCLIENT_LIB}
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/patch.sh ${STATIC_MYSQLCLIENT_LIB} ${MYSQLCLIENT_LIB}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Patching mysqlclient library.")
    add_custom_target (our_mysql_client DEPENDS ${MYSQLCLIENT_LIB})
    add_dependencies (mysqlxx our_mysql_client)
    set (MYSQLCLIENT_LIBRARIES ${MYSQLCLIENT_LIB})
endif ()

target_link_libraries (mysqlxx common ${MYSQLCLIENT_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${PLATFORM_LIBRARIES})
target_include_directories (mysqlxx SYSTEM PRIVATE ${OPENSSL_INCLUDE_DIR})

if (ENABLE_TESTS)
    add_subdirectory (src/tests)
endif ()
