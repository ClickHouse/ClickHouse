
add_executable (data_types_number_fixed data_types_number_fixed.cpp)
target_link_libraries (data_types_number_fixed PRIVATE dbms)

add_executable (data_type_string data_type_string.cpp)
target_link_libraries (data_type_string PRIVATE dbms)

if(USE_GTEST)
    add_executable(data_type_get_common_type data_type_get_common_type.cpp)
    target_link_libraries(data_type_get_common_type PRIVATE dbms ${GTEST_BOTH_LIBRARIES})
endif()
