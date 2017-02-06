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
	"/lib64/mysql"
)

set (MYSQL_INCLUDE_PATHS
	"/usr/local/opt/mysql/include"
	"/usr/mysql/include/mysql"
	"/usr/local/include/mysql"
	"/usr/include/mysql"
)

find_path (MYSQL_INCLUDE_DIR NAMES mysql/mysql.h PATHS ${MYSQL_INCLUDE_PATHS})

if (USE_STATIC_LIBRARIES)
	find_library (STATIC_MYSQLCLIENT_LIB libmysqlclient.a PATHS ${MYSQL_LIB_PATHS})
else ()
	find_library (MYSQLCLIENT_LIB mysqlclient PATHS ${MYSQL_LIB_PATHS})
endif ()

include_directories (${MYSQL_INCLUDE_DIR})

message (STATUS "Using mysqlclient: ${MYSQL_INCLUDE_DIR} : ${MYSQLCLIENT_LIB}; static=${STATIC_MYSQLCLIENT_LIB}")
