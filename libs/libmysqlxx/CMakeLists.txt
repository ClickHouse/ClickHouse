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

set(PLATFORM_LIBS ${CMAKE_DL_LIBS})

if (USE_STATIC_LIBRARIES)
	set (MYSQLCLIENT_LIB ${CMAKE_CURRENT_BINARY_DIR}/libmysqlclient.a)
	add_custom_command (
		OUTPUT ${MYSQLCLIENT_LIB}
		COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/patch.sh ${STATIC_MYSQLCLIENT_LIB} ${MYSQLCLIENT_LIB}
		WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
		COMMENT "Patching mysqlclient library.")
	add_custom_target (our_mysql_client DEPENDS ${MYSQLCLIENT_LIB})
	add_dependencies (mysqlxx our_mysql_client)
	set (Z_LIB zlibstatic)
else ()
	set (Z_PATHS "/usr/local/opt/zlib/lib" ${MYSQL_LIB_PATHS})
	find_library (Z_LIB z PATHS ${Z_PATHS})
endif ()

target_link_libraries (mysqlxx common ${MYSQLCLIENT_LIB} ${OPENSSL_LIBRARIES} ${Z_LIB} ${PLATFORM_LIBS})

if (ENABLE_TESTS)
	add_subdirectory (src/tests)
endif (ENABLE_TESTS)
