add_executable(collect_aliases collect_aliases.cpp)
target_link_libraries(collect_aliases dbms)

add_executable(collect_tables collect_tables.cpp)
target_link_libraries(collect_tables dbms)

add_executable(analyze_columns analyze_columns.cpp)
target_link_libraries(analyze_columns dbms)
