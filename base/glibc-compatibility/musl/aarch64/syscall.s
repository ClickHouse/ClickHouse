.global __syscall
.hidden __syscall
.type __syscall,%function
__syscall:
.cfi_startproc
	hint #34 // bti c
	uxtw x8,w0
	mov x0,x1
	mov x1,x2
	mov x2,x3
	mov x3,x4
	mov x4,x5
	mov x5,x6
	mov x6,x7
	svc 0
	ret
.cfi_endproc

// BTI landing pad above, and the BTI and PAC properties (GNU_PROPERTY_AARCH64_FEATURE_1_AND), without which
// the linker disables BTI for the whole executable. Both are no-ops for builds and CPUs without branch protection.
.section .note.gnu.property,"a"
.p2align 3
.word 4
.word 16
.word 5
.asciz "GNU"
.word 0xc0000000
.word 4
.word 3
.word 0
