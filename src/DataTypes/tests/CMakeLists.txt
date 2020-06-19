add_executable (data_types_number_fixed data_types_number_fixed.cpp)
target_link_libraries (data_types_number_fixed PRIVATE dbms)

add_executable (data_type_string data_type_string.cpp)
target_link_libraries (data_type_string PRIVATE dbms)
