/* Replaces `arch/x86_64/trampoline.c`, for the same reason as `trampoline-aarch64.S`: the byte before
 * the entry is a `nop` inside the trampoline's own FDE, which marks the outermost frame of a context's
 * stack by saying the return address is undefined. */

	.text
	.p2align 4
	.global libucontext_trampoline
	.hidden libucontext_trampoline
	.type libucontext_trampoline, @function
	.cfi_startproc
	.cfi_undefined rip
	nop
libucontext_trampoline:
	/* `libucontext_makecontext` points rbx to the stack slot holding `uc_link`. On entry the stack
	 * is 16-byte aligned, as `libucontext_makecontext` arranges, so the calls below are aligned. */
	movq (%rbx), %rdi
	testq %rdi, %rdi
	jz 1f
	call libucontext_setcontext
	call abort@PLT
1:
	xorl %edi, %edi
	call exit@PLT
	.cfi_endproc
	.size libucontext_trampoline, . - libucontext_trampoline

	.section .note.GNU-stack, "", @progbits
