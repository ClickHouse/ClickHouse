add_executable(test-connect test_connect.cpp)
target_link_libraries (test-connect dbms)

add_definitions(-Wno-non-virtual-dtor)

add_executable(test_multiplexing test_multiplexing.cpp)
target_include_directories (test_multiplexing PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include ${ClickHouse_SOURCE_DIR}/contrib/libboost/boost_1_62_0/)
target_link_libraries (test_multiplexing dbms daemon  boost_coroutine_internal boost_context_internal boost_thread_internal)
