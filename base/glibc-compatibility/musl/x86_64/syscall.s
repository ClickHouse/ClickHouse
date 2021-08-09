.global __syscall
.hidden __syscall
.type __syscall,@function
__syscall:
.cfi_startproc
	movq %rdi,%rax
	movq %rsi,%rdi
	movq %rdx,%rsi
	movq %rcx,%rdx
	movq %r8,%r10
	movq %r9,%r8
	movq 8(%rsp),%r9
	syscall
	ret
.cfi_endproc
