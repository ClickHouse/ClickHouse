/* Replaces `arch/aarch64/trampoline.c`: `libucontext_makecontext` sets the return address of a new
 * context to `libucontext_trampoline`, so this is the bottom frame of every stack a context runs on.
 *
 * An unwinder looks up the frame of a return address at the address minus 1. For the C trampoline
 * that is the last byte of whatever function the linker placed before it, typically
 * `libucontext_makecontext`, whose unwind rule applied to the registers of a fresh context reads
 * memory at a garbage address, so a stack trace taken on such a stack (a fiber of `boost::context`)
 * faults. Here the byte before the entry is a `nop` inside the trampoline's own FDE, which says the
 * return address is undefined, the DWARF way to mark the outermost frame, like glibc's `__startcontext`. */

	.text
	.p2align 2
	.global libucontext_trampoline
	.hidden libucontext_trampoline
	.type libucontext_trampoline, %function
	.cfi_startproc
	.cfi_undefined x30
	nop
libucontext_trampoline:
	/* `libucontext_makecontext` puts `uc_link` into x19. */
	cbz x19, 1f
	mov x0, x19
	bl libucontext_setcontext
	bl abort
1:
	mov x0, #0
	bl exit
	.cfi_endproc
	.size libucontext_trampoline, . - libucontext_trampoline

	.section .note.GNU-stack, "", %progbits
