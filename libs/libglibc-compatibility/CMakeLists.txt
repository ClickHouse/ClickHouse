enable_language (ASM)
add_library (glibc-compatibility glibc-compatibility.c musl/pipe2.c musl/fallocate.c musl/longjmp.s musl/vasprintf.c musl/lgamma.c)
