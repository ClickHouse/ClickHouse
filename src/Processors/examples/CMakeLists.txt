add_executable (comma_separated_streams comma_separated_streams.cpp)
target_link_libraries (comma_separated_streams PRIVATE dbms)
#target_include_directories(comma_separated_streams SYSTEM BEFORE PRIVATE 
#    ${ClickHouse_SOURCE_DIR}/contrib/arrow/cpp/src
#    ${ClickHouse_SOURCE_DIR}/contrib/arrow-cmake/cpp/src)
# dbms_target_link_libraries(PRIVATE ${ARROW_LIBRARY})
