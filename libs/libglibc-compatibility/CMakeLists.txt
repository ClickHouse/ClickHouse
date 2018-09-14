enable_language (ASM)

if (COMPILER_CLANG)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-requires-header")
endif ()

add_library (glibc-compatibility

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

libcxxabi/cxa_thread_atexit.cpp
libcxxabi/abort_message.cpp
libcxxabi/cxa_thread_atexit_impl.c
)

target_include_directories(glibc-compatibility PRIVATE libcxxabi)

add_subdirectory (tests)
