if (OS_LINUX)
    add_library(glibc-rseq STATIC rseq.c)
    # __rseq_offset/__rseq_size **requires** GOT indirection, otherwise it will be replaced with NULL by the linker
    target_compile_options(glibc-rseq PRIVATE -fno-direct-access-external-data)
    target_no_warning(glibc-rseq declaration-after-statement)
    target_link_libraries(global-libs INTERFACE glibc-rseq)
endif()
