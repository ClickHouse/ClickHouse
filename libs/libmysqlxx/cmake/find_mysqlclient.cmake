option (ENABLE_MYSQL "Enable MySQL" ON)

if (ENABLE_MYSQL)
    set (MYSQL_LIB_PATHS
        "/usr/local/opt/mysql/lib"
        "/usr/local/lib"
        "/usr/local/lib64"
        "/usr/mysql/lib"
        "/usr/mysql/lib64"
        "/usr/lib"
        "/usr/lib64"
        "/lib"
        "/lib64")

    set (MYSQL_INCLUDE_PATHS
        "/usr/local/opt/mysql/include"
        "/usr/mysql/include"
        "/usr/local/include"
        "/usr/include")

    find_path (MYSQL_INCLUDE_DIR NAMES mysql/mysql.h PATHS ${MYSQL_INCLUDE_PATHS} PATH_SUFFIXES mysql)

    if (USE_STATIC_LIBRARIES)
        find_library (STATIC_MYSQLCLIENT_LIB NAMES mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS} PATH_SUFFIXES mysql)
    else ()
        find_library (MYSQLCLIENT_LIBRARIES NAMES mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS} PATH_SUFFIXES mysql)
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
