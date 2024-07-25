if (GLIBC_COMPATIBILITY)
    add_subdirectory(memcpy)
    if(TARGET memcpy)
        set(MEMCPY_LIBRARY memcpy)
    endif()

    enable_language(ASM)

    add_headers_and_sources(glibc_compatibility .)
    add_headers_and_sources(glibc_compatibility musl)

    if (SANITIZE STREQUAL "memory" OR SANITIZE STREQUAL "thread")
        # Sanitizers are not compatible with high ASLR entropy, which is the default on modern Linux distributions, and
        # to workaround this limitation, TSAN and MSAN (couldn't see other sanitizers doing the same), re-exec the binary
        # without ASLR (see https://github.com/llvm/llvm-project/commit/0784b1eefa36d4acbb0dacd2d18796e26313b6c5)
        #
        # The problem we face is that, in order to re-exec, the sanitizer wants to use the original pathname in the call
        # and to get its value it uses getauxval (https://github.com/llvm/llvm-project/blob/20eff684203287828d6722fc860b9d3621429542/compiler-rt/lib/sanitizer_common/sanitizer_linux_libcdep.cpp#L985-L988).
        # Since we provide getauxval ourselves (to minimize the version dependency on runtime glibc), we are the ones
        # being called and we fail horribly:
        #
        #    ==301455==ERROR: MemorySanitizer: SEGV on unknown address 0x2ffc6d721550 (pc 0x5622c1cc0073 bp 0x000000000003 sp 0x7ffc6d721530 T301455)
        #    ==301455==The signal is caused by a WRITE memory access.
        # #0 0x5622c1cc0073 in __auxv_init_procfs ./ClickHouse/base/glibc-compatibility/musl/getauxval.c:129:5
        # #1 0x5622c1cbffe9 in getauxval ./ClickHouse/base/glibc-compatibility/musl/getauxval.c:240:12
        # #2 0x5622c0d7bfb4 in __sanitizer::ReExec() crtstuff.c
        # #3 0x5622c0df7bfc in __msan::InitShadowWithReExec(bool) crtstuff.c
        # #4 0x5622c0d95356 in __msan_init (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x256356) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)
        # #5 0x5622c0dfe878 in msan.module_ctor main.cc
        # #6 0x5622c1cc156c in __libc_csu_init (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x118256c) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)
        # #7 0x73dc05dd7ea3 in __libc_start_main /usr/src/debug/glibc/glibc/csu/../csu/libc-start.c:343:6
        # #8 0x5622c0d6b7cd in _start (./ClickHouse/build_msan/contrib/google-protobuf-cmake/protoc+0x22c7cd) (BuildId: 6411d3c88b898ba3f7d49760555977d3e61f0741)
        #
        # The source of the issue above is that, at this point in time during __msan_init, we can't really do much as
        # most global variables aren't initialized or available yet, so we we can't initiate the auxiliary vector.
        # Normal glibc / musl getauxval doesn't have this problem since they initiate their auxval vector at the very
        # start of __libc_start_main (just keeping track of argv+argc+1), but we don't have such option (otherwise
        # this complexity of reading "/proc/self/auxv" or using __environ would not be necessary).
        #
        # To avoid this crashes on the re-exec call (see above how it would fail when creating `aux`, and it we used
        # __auxv_init_environ then it would SIGSEV on READing `__environ`) we don't include getauxval for the sanitizers
        # This means that those builds will depend on GLIBC 2.16 instead of 2.4 in x86_64
        list(REMOVE_ITEM glibc_compatibility musl/getauxval.c)
    endif()

    if (ARCH_AARCH64)
        list (APPEND glibc_compatibility_sources musl/aarch64/syscall.s musl/aarch64/longjmp.s)
        set (musl_arch_include_dir musl/aarch64)
    elseif (ARCH_AMD64)
        list (APPEND glibc_compatibility_sources musl/x86_64/syscall.s musl/x86_64/longjmp.s)
        set (musl_arch_include_dir musl/x86_64)
    else ()
        message (FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
    endif ()

    # Need to omit frame pointers to match the performance of glibc
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")

    add_library(glibc-compatibility STATIC ${glibc_compatibility_sources})

    target_no_warning(glibc-compatibility unused-command-line-argument)
    target_no_warning(glibc-compatibility unused-but-set-variable)
    target_no_warning(glibc-compatibility builtin-requires-header)

    target_include_directories(glibc-compatibility PRIVATE libcxxabi ${musl_arch_include_dir})

    if (ENABLE_OPENSSL_DYNAMIC)
        target_compile_options(glibc-compatibility PRIVATE -fPIC)
    endif ()

    target_link_libraries(global-libs INTERFACE glibc-compatibility ${MEMCPY_LIBRARY})

    message (STATUS "Some symbols from glibc will be replaced for compatibility")

elseif (CLICKHOUSE_OFFICIAL_BUILD)
    message (WARNING "Option GLIBC_COMPATIBILITY must be turned on for production builds.")
endif ()
