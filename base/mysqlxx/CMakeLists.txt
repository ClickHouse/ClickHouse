add_library (mysqlxx
    Connection.cpp
    Exception.cpp
    Query.cpp
    ResultBase.cpp
    UseQueryResult.cpp
    Row.cpp
    Value.cpp
    Pool.cpp
    PoolFactory.cpp
    PoolWithFailover.cpp
)

target_include_directories (mysqlxx PUBLIC ..)

if (USE_INTERNAL_MYSQL_LIBRARY)
    target_include_directories (mysqlxx PUBLIC "${ClickHouse_SOURCE_DIR}/contrib/mariadb-connector-c/include")
    target_include_directories (mysqlxx PUBLIC "${ClickHouse_BINARY_DIR}/contrib/mariadb-connector-c/include")
else ()
    set(PLATFORM_LIBRARIES ${CMAKE_DL_LIBS})

    if (USE_MYSQL)
        target_include_directories (mysqlxx SYSTEM PRIVATE ${MYSQL_INCLUDE_DIR})
    endif ()

    if (APPLE)
        find_library (ICONV_LIBRARY iconv)
        set (MYSQLCLIENT_LIBRARIES ${MYSQLCLIENT_LIBRARIES} ${STATIC_MYSQLCLIENT_LIB} ${ICONV_LIBRARY})
    elseif (USE_STATIC_LIBRARIES AND STATIC_MYSQLCLIENT_LIB)
        set (MYSQLCLIENT_LIBRARIES ${STATIC_MYSQLCLIENT_LIB})
    endif ()
endif ()

target_link_libraries (mysqlxx
    PUBLIC
        common
    PRIVATE
        ${MYSQLCLIENT_LIBRARIES}
        ${ZLIB_LIBRARIES}
)

if(OPENSSL_LIBRARIES)
    target_link_libraries(mysqlxx PRIVATE ${OPENSSL_LIBRARIES})
endif()

target_link_libraries(mysqlxx PRIVATE ${PLATFORM_LIBRARIES})

if (NOT USE_INTERNAL_MYSQL_LIBRARY AND OPENSSL_INCLUDE_DIR)
    target_include_directories (mysqlxx SYSTEM PRIVATE ${OPENSSL_INCLUDE_DIR})
endif ()

target_no_warning(mysqlxx reserved-macro-identifier)

if (NOT USE_INTERNAL_MYSQL_LIBRARY AND USE_STATIC_LIBRARIES)
   message(WARNING "Statically linking with system mysql/mariadb only works "
           "if mysql client libraries are built with same openssl version as "
           "we are going to use now. It wouldn't work if GnuTLS is used. "
           "Try -D\"USE_INTERNAL_MYSQL_LIBRARY\"=ON or -D\"ENABLE_MYSQL\"=OFF or "
           "-D\"USE_STATIC_LIBRARIES\"=OFF")
endif ()

if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()
