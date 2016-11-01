include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (mysqlxx_test mysqlxx_test.cpp)
add_executable (failover failover.cpp)

if (APPLE)
        SET(RT_LIBRARIES "")
else()
        SET(RT_LIBRARIES "rt")
endif()
target_link_libraries (mysqlxx_test mysqlxx dbms ${RT_LIBRARIES})
target_link_libraries (failover mysqlxx PocoUtil PocoFoundation)
