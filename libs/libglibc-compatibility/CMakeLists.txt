# N.B. This library works on x86_64 only.
enable_language (ASM)
add_library (glibc-compatibility glibc-compatibility.c musl/pipe2.c musl/fallocate.c musl/longjmp.s musl/setjmp.s musl/vasprintf.c musl/lgamma.c)

add_subdirectory (tests)
