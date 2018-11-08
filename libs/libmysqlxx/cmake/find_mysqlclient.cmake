option (ENABLE_MYSQL "Enable MySQL" ${OS_LINUX})
if (OS_LINUX)
    option (USE_INTERNAL_MYSQL_LIBRARY "Set to FALSE to use system mysqlclient library instead of bundled" ${NOT_UNBUNDLED})
else ()
    option (USE_INTERNAL_MYSQL_LIBRARY "Set to FALSE to use system mysqlclient library instead of bundled" OFF)
endif ()

if (USE_INTERNAL_MYSQL_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/mariadb-connector-c/README")
    message (WARNING "submodule contrib/mariadb-connector-c is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_MYSQL_LIBRARY 0)
endif ()


if (ENABLE_MYSQL)
    if (USE_INTERNAL_MYSQL_LIBRARY)
        set (MYSQLCLIENT_LIBRARIES mysqlclient)
        set (USE_MYSQL 1)
        set (MYSQLXX_LIBRARY mysqlxx)
    else ()
        set (MYSQL_LIB_PATHS
            "/usr/local/opt/mysql/lib"
            "/usr/local/lib"
            "/usr/local/lib64"
            "/usr/local/lib/mariadb" # macos brew mariadb-connector-c
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

        find_path (MYSQL_INCLUDE_DIR NAMES mysql/mysql.h mariadb/mysql.h PATHS ${MYSQL_INCLUDE_PATHS} PATH_SUFFIXES mysql)

        if (USE_STATIC_LIBRARIES)
            find_library (STATIC_MYSQLCLIENT_LIB NAMES mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS} PATH_SUFFIXES mysql)
        else ()
            find_library (MYSQLCLIENT_LIBRARIES NAMES mariadb mariadbclient mysqlclient PATHS ${MYSQL_LIB_PATHS} PATH_SUFFIXES mysql)
        endif ()

        if (MYSQL_INCLUDE_DIR AND (STATIC_MYSQLCLIENT_LIB OR MYSQLCLIENT_LIBRARIES))
            set (USE_MYSQL 1)
            set (MYSQLXX_LIBRARY mysqlxx)
            if (APPLE)
                # /usr/local/include/mysql/mysql_com.h:1011:10: fatal error: mysql/udf_registration_types.h: No such file or directory
                set(MYSQL_INCLUDE_DIR ${MYSQL_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR}/mysql)
            endif ()
        endif ()
    endif ()
endif ()

if (USE_MYSQL)
    message (STATUS "Using mysqlclient=${USE_MYSQL}: ${MYSQL_INCLUDE_DIR} : ${MYSQLCLIENT_LIBRARIES}; staticlib=${STATIC_MYSQLCLIENT_LIB}")
else ()
    message (STATUS "Build without mysqlclient (support for MYSQL dictionary source will be disabled)")
endif ()
