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

target_link_libraries (mysqlxx common libmysqlclient.a ${OPENSSL_LIBS} libz.a dl)


ADD_CUSTOM_COMMAND(
    TARGET mysqlxx
    POST_BUILD
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/patch.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Patching mysqlclient library.")

IF(TESTS)
	add_subdirectory (src/tests)
ENDIF(TESTS)
