add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect dbms)

add_executable(test_multiplexing test_multiplexing.cpp)
target_link_libraries (test_multiplexing dbms)
