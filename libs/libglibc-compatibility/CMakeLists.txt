enable_language(ASM)
include(CheckIncludeFile)

check_include_file("sys/random.h" HAVE_SYS_RANDOM_H)

if(COMPILER_CLANG)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
endif()


set(GLIBC_COMPATIBILITY_SOURCES
glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/longjmp.s
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall.s
musl/syscall_ret.c
musl/sched_cpucount.c
musl/glob.c
musl/exp2f.c
musl/pwritev.c
musl/getrandom.c
musl/fcntl.c
musl/timespec_get.c
musl/sched_getcpu.c
)

if(HAVE_SYS_RANDOM_H)
    list(APPEND GLIBC_COMPATIBILITY_SOURCES musl/getentropy.c)
endif()

if(MAKE_STATIC_LIBRARIES)
    set(GLIBC_COMPATIBILITY_SOURCES ${GLIBC_COMPATIBILITY_SOURCES}
        libcxxabi/cxa_thread_atexit.cpp)
endif()

add_library(glibc-compatibility ${GLIBC_COMPATIBILITY_SOURCES})

target_include_directories(glibc-compatibility PRIVATE libcxxabi)
