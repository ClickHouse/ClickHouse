if (GLIBC_COMPATIBILITY)
    set (USE_INTERNAL_MEMCPY ON)

    enable_language(ASM)
    include(CheckIncludeFile)

    check_include_file("sys/random.h" HAVE_SYS_RANDOM_H)

    if(COMPILER_CLANG)
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
    endif()

    add_headers_and_sources(glibc_compatibility .)
    add_headers_and_sources(glibc_compatibility musl)
    list(APPEND glibc_compatibility_sources musl/syscall.s musl/longjmp.s)

    list(REMOVE_ITEM glibc_compatibility_sources musl/getentropy.c)
    if(HAVE_SYS_RANDOM_H)
        list(APPEND glibc_compatibility_sources musl/getentropy.c)
    endif()

    if(MAKE_STATIC_LIBRARIES)
        list(APPEND glibc_compatibility_sources libcxxabi/cxa_thread_atexit.cpp)
    endif()

    add_library(glibc-compatibility STATIC ${glibc_compatibility_sources})

    target_include_directories(glibc-compatibility PRIVATE libcxxabi)

    if (USE_STATIC_LIBRARIES=0 AND MAKE_STATIC_LIBRARIES=OFF)
        target_compile_options(PRIVATE -fPIC)
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
