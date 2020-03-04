
add_subdirectory(StringGenerator)

cuda_add_executable(cuda_strings_aggregator_with_file 
        cuda_strings_aggregator_with_file.cpp  OPTIONS -DUSE_TABLE_METHOD=1 -DUSE_CUB=1)
target_link_libraries (cuda_strings_aggregator_with_file dbms_cuda)