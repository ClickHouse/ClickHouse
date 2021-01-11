if (GLIBC_COMPATIBILITY)
    set (ENABLE_FASTMEMCPY ON)

    enable_language(ASM)
    include(CheckIncludeFile)

    check_include_file("sys/random.h" HAVE_SYS_RANDOM_H)

    if(COMPILER_CLANG)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
    endif()

    add_headers_and_sources(glibc_compatibility .)
    add_headers_and_sources(glibc_compatibility musl)
    if (ARCH_ARM)
        list (APPEND glibc_compatibility_sources musl/aarch64/syscall.s musl/aarch64/longjmp.s)
        set (musl_arch_include_dir musl/aarch64)
    elseif (ARCH_AMD64)
        list (APPEND glibc_compatibility_sources musl/x86_64/syscall.s musl/x86_64/longjmp.s)
        set (musl_arch_include_dir musl/x86_64)
    else ()
        message (FATAL_ERROR "glibc_compatibility can only be used on x86_64 or aarch64.")
    endif ()

    list(REMOVE_ITEM glibc_compatibility_sources musl/getentropy.c)
    if(HAVE_SYS_RANDOM_H)
        list(APPEND glibc_compatibility_sources musl/getentropy.c)
    endif()

    add_library (clickhouse_memcpy OBJECT
        ${ClickHouse_SOURCE_DIR}/contrib/FastMemcpy/memcpy_wrapper.c
    )

    # Need to omit frame pointers to match the performance of glibc
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fomit-frame-pointer")

    add_library(glibc-compatibility STATIC ${glibc_compatibility_sources})

    if (COMPILER_CLANG)
        target_compile_options(glibc-compatibility PRIVATE -Wno-unused-command-line-argument)
    elseif (COMPILER_GCC)
        target_compile_options(glibc-compatibility PRIVATE -Wno-unused-but-set-variable)
    endif ()

    target_include_directories(glibc-compatibility PRIVATE libcxxabi ${musl_arch_include_dir})

    if (NOT USE_STATIC_LIBRARIES AND NOT MAKE_STATIC_LIBRARIES)
        target_compile_options(glibc-compatibility PRIVATE -fPIC)
    endif ()

    target_link_libraries(global-libs INTERFACE glibc-compatibility)

    install(
        TARGETS glibc-compatibility
        EXPORT global
        ARCHIVE DESTINATION lib
    )

    message (STATUS "Some symbols from glibc will be replaced for compatibility")
elseif (YANDEX_OFFICIAL_BUILD)
    message (WARNING "Option GLIBC_COMPATIBILITY must be turned on for production builds.")
endif ()
