find_path(MYSQL_INCLUDE_DIR mysql.h
		/usr/include/mysql
		/usr/local/include/mysql
		/opt/mysql/mysql/include
		/opt/mysql/mysql/include/mysql
		/usr/local/mysql/include
		/usr/local/mysql/include/mysql
		$ENV{MYSQL_INCLUDE_DIR}
		$ENV{MYSQL_DIR}/include
		$ENV{ProgramFiles}/MySQL/*/include
		$ENV{SystemDrive}/MySQL/*/include)

if (WIN32)
	if (CMAKE_BUILD_TYPE STREQUAL Debug)
		set(libsuffixDist debug)
		set(libsuffixBuild Debug)
	else (CMAKE_BUILD_TYPE STREQUAL Debug)
		set(libsuffixDist opt)
		set(libsuffixBuild Release)
		add_definitions(-DDBUG_OFF)
	endif (CMAKE_BUILD_TYPE STREQUAL Debug)

	find_library(MYSQL_LIB NAMES mysqlclient
				 PATHS
				 $ENV{MYSQL_DIR}/lib/${libsuffixDist}
				 $ENV{MYSQL_DIR}/libmysql/${libsuffixBuild}
				 $ENV{MYSQL_DIR}/client/${libsuffixBuild}
				 $ENV{ProgramFiles}/MySQL/*/lib/${libsuffixDist}
				 $ENV{SystemDrive}/MySQL/*/lib/${libsuffixDist})
else (WIN32)
	find_library(MYSQL_LIB NAMES mysqlclient_r
				 PATHS
				 /usr/lib/mysql
				 /usr/local/lib/mysql
				 /usr/local/mysql/lib
				 /usr/local/mysql/lib/mysql
				 /opt/mysql/mysql/lib
				 /opt/mysql/mysql/lib/mysql
				 $ENV{MYSQL_DIR}/libmysql_r/.libs
				 $ENV{MYSQL_DIR}/lib
				 $ENV{MYSQL_DIR}/lib/mysql)
endif (WIN32)

if(MYSQL_LIB)
	get_filename_component(MYSQL_LIB_DIR ${MYSQL_LIB} PATH)
endif(MYSQL_LIB)

if (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)
	set(MYSQL_FOUND TRUE)
	message(STATUS "MySQL Include directory: ${MYSQL_INCLUDE_DIR}  library directory: ${MYSQL_LIB_DIR}")
	include_directories(${MYSQL_INCLUDE_DIR})
	link_directories(${MYSQL_LIB_DIR})
else (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)
	message(STATUS "Couldn't find MySQL")
endif (MYSQL_INCLUDE_DIR AND MYSQL_LIB_DIR)
