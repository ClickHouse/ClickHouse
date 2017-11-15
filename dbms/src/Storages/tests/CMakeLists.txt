include_directories (${CMAKE_CURRENT_BINARY_DIR})

add_executable (system_numbers system_numbers.cpp)
target_link_libraries (system_numbers dbms clickhouse_storages_system)

add_executable (storage_log storage_log.cpp)
target_link_libraries (storage_log dbms)

add_executable (hit_log hit_log.cpp)
target_link_libraries (hit_log dbms)

add_executable (seek_speed_test seek_speed_test.cpp)
target_link_libraries (seek_speed_test dbms)

add_executable (part_checker part_checker.cpp)
target_link_libraries (part_checker dbms)

add_executable (part_name part_name.cpp)
target_link_libraries (part_name dbms)

add_executable (remove_symlink_directory remove_symlink_directory.cpp)
target_link_libraries (remove_symlink_directory dbms)

add_executable (merge_selector merge_selector.cpp)
target_link_libraries (merge_selector dbms)

add_executable (merge_selector2 merge_selector2.cpp)
target_link_libraries (merge_selector2 dbms)
