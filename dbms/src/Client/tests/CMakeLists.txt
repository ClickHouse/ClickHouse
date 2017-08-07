add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect dbms)

add_executable(test_multiplexing test_multiplexing.cpp)
target_include_directories (test_multiplexing PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)
target_link_libraries (test_multiplexing dbms daemon)
