set(LLVM_LIBC_DIR "${CMAKE_BINARY_DIR}/contrib/libllvmlibc-cmake")
link_directories("${LLVM_LIBC_DIR}")

target_link_libraries(global-libs INTERFACE libllvmlibc)
