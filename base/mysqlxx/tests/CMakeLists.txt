add_executable (mysqlxx_test mysqlxx_test.cpp)
target_link_libraries (mysqlxx_test PRIVATE mysqlxx)

add_executable (mysqlxx_pool_test mysqlxx_pool_test.cpp)
target_link_libraries (mysqlxx_pool_test PRIVATE mysqlxx)
