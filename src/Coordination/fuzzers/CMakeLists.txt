add_executable (request_fuzzer request_fuzzer.cpp)
target_link_libraries (request_fuzzer PRIVATE dbms ${LIB_FUZZING_ENGINE})
