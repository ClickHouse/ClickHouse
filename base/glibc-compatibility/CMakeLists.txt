if (GLIBC_COMPATIBILITY)
    add_subdirectory(memcpy)
    if(TARGET memcpy)
        set(MEMCPY_LIBRARY memcpy)
    endif()

    enable_language(ASM)

    add_headers_and_sources(glibc_compatibility .)
    add_headers_and_sources(glibc_compatibility musl)
    if (ARCH_AARCH64)
        list (APPEND glibc_compatibility_sources musl/aarch64/syscall.s musl/aarch64/longjmp.s)
        set (musl_arch_include_dir musl/aarch64)
        # Disable getauxval in aarch64. ARM glibc minimum requirement for the project is 2.18 and getauxval is present
        # in 2.16. Having a custom one introduces issues with sanitizers
        list (REMOVE_ITEM glibc_compatibility_sources musl/getauxval.c)
    elseif (ARCH_AMD64)
        list (APPEND glibc_compatibility_sources musl/x86_64/syscall.s musl/x86_64/longjmp.s)
        set (musl_arch_include_dir musl/x86_64)
    else ()
        message (FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
    endif ()

    if (SANITIZE STREQUAL thread AND ARCH_AMD64)
        # Disable TSAN instrumentation that conflicts with re-exec due to high ASLR entropy using getauxval
        # See longer comment in __auxv_init_procfs
        # In the case of tsan we need to make sure getauxval is not instrumented as that would introduce tsan
        # internal calls to functions that depend on a state that isn't initialized yet
        set_source_files_properties(
                musl/getauxval.c
                PROPERTIES COMPILE_FLAGS "-mllvm -tsan-instrument-func-entry-exit=false")
    endif()

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
