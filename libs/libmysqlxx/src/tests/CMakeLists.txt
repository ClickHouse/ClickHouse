include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (mysqlxx_test mysqlxx_test.cpp)
add_executable (failover failover.cpp)

target_link_libraries (mysqlxx_test mysqlxx dbms rt)
target_link_libraries (failover mysqlxx PocoUtil PocoFoundation)
