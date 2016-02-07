include_directories (${CMAKE_CURRENT_BINARY_DIR})

set(SRCS )

add_executable (system_numbers system_numbers.cpp ${SRCS})
target_link_libraries (system_numbers dbms)

add_executable (storage_log storage_log.cpp ${SRCS})
target_link_libraries (storage_log dbms)

add_executable (hit_log hit_log.cpp ${SRCS})
target_link_libraries (hit_log dbms)

add_executable (merge_tree merge_tree.cpp ${SRCS})
target_link_libraries (merge_tree dbms)

add_executable (seek_speed_test seek_speed_test.cpp ${SRCS})
target_link_libraries (seek_speed_test dbms)

add_executable (pk_condition pk_condition.cpp ${SRCS})
target_link_libraries (pk_condition dbms)
add_check(pk_condition)

add_executable (part_checker part_checker.cpp ${SRCS})
target_link_libraries (part_checker dbms)

add_executable (part_name part_name.cpp ${SRCS})
target_link_libraries (part_name dbms)

