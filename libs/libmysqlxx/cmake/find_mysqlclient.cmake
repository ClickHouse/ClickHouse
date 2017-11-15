option (ENABLE_MYSQL "Enable MySQL" ON)

if (ENABLE_MYSQL)
    set (MYSQL_LIB_PATHS
        "/usr/local/opt/mysql/lib"
        "/usr/local/lib/mysql/"
        "/usr/local/lib/mysql"
        "/usr/local/lib64/mysql"
        "/usr/mysql/lib/mysql"
        "/usr/mysql/lib64/mysql"
        "/usr/lib/mysql"
        "/usr/lib64/mysql"
        "/lib/mysql"
        "/lib64/mysql")

    set (MYSQL_INCLUDE_PATHS
        "/usr/local/opt/mysql/include"
        "/usr/mysql/include/mysql"
        "/usr/local/include/mysql"
        "/usr/include/mysql")

    find_path (MYSQL_INCLUDE_DIR NAMES mysql/mysql.h PATHS ${MYSQL_INCLUDE_PATHS})

    if (USE_STATIC_LIBRARIES)
        find_library (STATIC_MYSQLCLIENT_LIB mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS})
    else ()
        find_library (MYSQLCLIENT_LIBRARIES mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS})
    endif ()

    if (MYSQL_INCLUDE_DIR AND (STATIC_MYSQLCLIENT_LIB OR MYSQLCLIENT_LIBRARIES))
        set (USE_MYSQL 1)
        set (MYSQLXX_LIBRARY mysqlxx)
    endif ()
endif ()

if (USE_MYSQL)
    message (STATUS "Using mysqlclient=${USE_MYSQL}: ${MYSQL_INCLUDE_DIR} : ${MYSQLCLIENT_LIBRARIES}; staticlib=${STATIC_MYSQLCLIENT_LIB}")
else ()
    message (STATUS "Build without mysqlclient (support for MYSQL dictionary source will be disabled)")
endif ()
