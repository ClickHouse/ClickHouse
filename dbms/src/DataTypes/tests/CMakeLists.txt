include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (data_types_number_fixed data_types_number_fixed.cpp ${SRCS})
target_link_libraries (data_types_number_fixed dbms)

add_executable (data_type_string data_type_string.cpp ${SRCS})
target_link_libraries (data_type_string dbms)
