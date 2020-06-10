add_executable (compressed_buffer compressed_buffer.cpp)
target_link_libraries (compressed_buffer PRIVATE dbms)

add_executable (cached_compressed_read_buffer cached_compressed_read_buffer.cpp)
target_link_libraries (cached_compressed_read_buffer PRIVATE dbms)

if (ENABLE_FUZZING)
    add_executable (compressed_buffer_fuzzer compressed_buffer_fuzzer.cpp)
    target_link_libraries (compressed_buffer_fuzzer PRIVATE dbms ${LIB_FUZZING_ENGINE})
endif ()
