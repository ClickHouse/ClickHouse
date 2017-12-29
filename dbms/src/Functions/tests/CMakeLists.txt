include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (number_traits number_traits.cpp)
target_link_libraries (number_traits dbms)
