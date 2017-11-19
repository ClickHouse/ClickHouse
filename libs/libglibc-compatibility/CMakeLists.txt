enable_language (ASM)

add_library (glibc-compatibility glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/longjmp.s
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall.s
musl/syscall_ret.c
musl/sched_cpucount.c)

add_subdirectory (tests)
