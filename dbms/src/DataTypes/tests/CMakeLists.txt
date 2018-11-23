set(SRCS )

add_executable (data_types_number_fixed data_types_number_fixed.cpp ${SRCS})
target_link_libraries (data_types_number_fixed PRIVATE dbms)

add_executable (data_type_string data_type_string.cpp ${SRCS})
target_link_libraries (data_type_string PRIVATE dbms)

add_executable (data_type_get_common_type data_type_get_common_type.cpp ${SRCS})
target_link_libraries (data_type_get_common_type PRIVATE dbms gtest_main)
