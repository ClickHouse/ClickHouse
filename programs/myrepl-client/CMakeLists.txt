include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_executable(myrepl-client myrepl_client.cpp)
target_link_libraries(myrepl-client)