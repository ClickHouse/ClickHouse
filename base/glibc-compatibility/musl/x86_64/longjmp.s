/* Copyright 2011-2012 Nicholas J. Kain, licensed under standard MIT license */
.global musl_glibc_longjmp
.type musl_glibc_longjmp,@function
musl_glibc_longjmp:
    mov    0x30(%rdi),%r8
    mov    0x8(%rdi),%r9
    mov    0x38(%rdi),%rdx
    ror    $0x11,%r8
    xor    %fs:0x30,%r8     /* this ends up being the stack pointer */
    ror    $0x11,%r9
    xor    %fs:0x30,%r9
    ror    $0x11,%rdx
    xor    %fs:0x30,%rdx    /* this is the instruction pointer */
    mov    (%rdi),%rbx      /* rdi is the jmp_buf, restore regs from it */
    mov    0x10(%rdi),%r12
    mov    0x18(%rdi),%r13
    mov    0x20(%rdi),%r14
    mov    0x28(%rdi),%r15
    mov    %esi,%eax
    mov    %r8,%rsp
    mov    %r9,%rbp
    jmpq   *%rdx            /* goto saved address without altering rsp */
