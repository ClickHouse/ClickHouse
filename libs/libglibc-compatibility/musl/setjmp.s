/* Copyright 2011-2012 Nicholas J. Kain, licensed under standard MIT license */
.global musl_setjmp
.type musl_setjmp,@function
musl_setjmp:
	mov %rbx,(%rdi)         /* rdi is jmp_buf, move registers onto it */
	mov %rbp,8(%rdi)
	mov %r12,16(%rdi)
	mov %r13,24(%rdi)
	mov %r14,32(%rdi)
	mov %r15,40(%rdi)
	lea 8(%rsp),%rdx        /* this is our rsp WITHOUT current ret addr */
	mov %rdx,48(%rdi)
	mov (%rsp),%rdx         /* save return addr ptr for new rip */
	mov %rdx,56(%rdi)
	xor %rax,%rax           /* always return 0 */
	ret
