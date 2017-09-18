/* Copyright 2011-2012 Nicholas J. Kain, licensed under standard MIT license */
.global _longjmp
.global longjmp
.type _longjmp,@function
.type longjmp,@function
_longjmp:
longjmp:
	mov %rsi,%rax           /* val will be longjmp return */
	test %rax,%rax
	jnz 1f
	inc %rax                /* if val==0, val=1 per longjmp semantics */
1:
	mov (%rdi),%rbx         /* rdi is the jmp_buf, restore regs from it */
	mov 8(%rdi),%rbp
	mov 16(%rdi),%r12
	mov 24(%rdi),%r13
	mov 32(%rdi),%r14
	mov 40(%rdi),%r15
	mov 48(%rdi),%rdx       /* this ends up being the stack pointer */
	mov %rdx,%rsp
	mov 56(%rdi),%rdx       /* this is the instruction pointer */
	jmp *%rdx               /* goto saved address without altering rsp */
