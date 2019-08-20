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
