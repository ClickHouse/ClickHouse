add_executable (compressed_buffer compressed_buffer.cpp)
target_link_libraries (compressed_buffer PRIVATE dbms)

add_executable (cached_compressed_read_buffer cached_compressed_read_buffer.cpp)
target_link_libraries (cached_compressed_read_buffer PRIVATE dbms)

if (ENABLE_FUZZING)
    add_executable (compressed_buffer_fuzz compressed_buffer_fuzz.cpp)
    target_link_libraries (compressed_buffer_fuzz PRIVATE dbms)
    set_target_properties(compressed_buffer_fuzz PROPERTIES LINK_FLAGS ${LIBFUZZER_CMAKE_CXX_FLAGS})
endif ()
