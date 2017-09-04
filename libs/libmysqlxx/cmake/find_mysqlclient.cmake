option (ENABLE_MYSQL "Enable MySQL" ON)

option (USE_INTERNAL_MYSQL_LIBRARY "Set to FALSE to use system mysql library instead of bundled" ${NOT_UNBUNDLED})

if (ENABLE_MYSQL)

   if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/mariadb-connector-c/CMakeLists.txt")
      message (WARNING "submodule contrib/mariadb-connector-c is missing. to fix try run: git submodule update --init --recursive")
      set (USE_INTERNAL_MYSQL_LIBRARY 0)
   endif ()

   if (NOT USE_INTERNAL_MYSQL_LIBRARY)
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
   endif ()

    if (MYSQL_INCLUDE_DIR AND (STATIC_MYSQLCLIENT_LIB OR MYSQLCLIENT_LIBRARIES))
        include_directories (MYSQL_INCLUDE_DIR)
    else ()                                   
        set (USE_INTERNAL_MYSQL_LIBRARY 1)
        set (MYSQLCLIENT_LIBRARIES mariadbclient)
        set (MYSQL_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/mariadb-connector-c/include")
    endif ()

    set(USE_MYSQL 1)
    set(MYSQLXX_LIBRARY mysqlxx)
endif ()

if (USE_MYSQL)
    message (STATUS "Using mysqlclient=${USE_MYSQL}: ${MYSQL_INCLUDE_DIR} : ${MYSQLCLIENT_LIBRARIES}; staticlib=${STATIC_MYSQLCLIENT_LIB} internal=${USE_INTERNAL_MYSQL_LIBRARY}")
else ()
    message (STATUS "Build without mysqlclient (support for MYSQL dictionary source will be disabled)")
endif ()
