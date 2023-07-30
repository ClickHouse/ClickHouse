add_executable (query_analyzer query_analyzer.cpp)
target_include_directories (query_analyzer SYSTEM BEFORE PRIVATE ${SPARSEHASH_INCLUDE_DIR})
target_link_libraries (query_analyzer PRIVATE dbms)
