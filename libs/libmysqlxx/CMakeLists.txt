add_library (mysqlxx
	src/Connection.cpp
	src/Query.cpp
	src/ResultBase.cpp
	src/StoreQueryResult.cpp
	src/UseQueryResult.cpp
	src/Value.cpp
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

add_dependencies (mysqlxx common)

set (MYSQL_HINTS "/usr/local/opt/mysql/lib")
find_library (MYSQLCLIENT_LIB libmysqlclient.a HINTS ${MYSQL_HINTS})
set (OUR_MYSQLCLIENT_LIB ${CMAKE_CURRENT_BINARY_DIR}/libmysqlclient.a)

target_link_libraries (mysqlxx common ${OUR_MYSQLCLIENT_LIB} ${OPENSSL_LIBS} libz.a dl)

add_custom_command(
	OUTPUT ${OUR_MYSQLCLIENT_LIB}
	COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/patch.sh ${MYSQLCLIENT_LIB} ${OUR_MYSQLCLIENT_LIB}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Patching mysqlclient library.")
add_custom_target(our_mysql_client DEPENDS ${OUR_MYSQLCLIENT_LIB})
add_dependencies(mysqlxx our_mysql_client)

if (TESTS)
	add_subdirectory (src/tests)
endif (TESTS)
