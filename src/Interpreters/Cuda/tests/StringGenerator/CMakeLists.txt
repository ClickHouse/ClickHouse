
file(COPY test_initialize.info DESTINATION . FILE_PERMISSIONS OWNER_READ OWNER_WRITE)
file(COPY test_read_file.info DESTINATION . FILE_PERMISSIONS OWNER_READ OWNER_WRITE)

cuda_add_executable(generator ideone_LxhX15.cpp)
#target_compile_features(generator PUBLIC cxx_std_11)
