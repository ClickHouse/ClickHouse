add_executable (processors_test processors_test.cpp)
add_executable (processors_test_chain processors_test_chain.cpp)
add_executable (processors_test_merge processors_test_merge.cpp)

target_link_libraries (processors_test dbms)
target_link_libraries (processors_test_chain dbms)
target_link_libraries (processors_test_merge dbms)
