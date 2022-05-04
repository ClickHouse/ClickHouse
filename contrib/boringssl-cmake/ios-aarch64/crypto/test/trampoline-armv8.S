// This file is generated from a similarly-named Perl script in the BoringSSL
// source tree. Do not edit by hand.

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif
#if __has_feature(memory_sanitizer) && !defined(OPENSSL_NO_ASM)
#define OPENSSL_NO_ASM
#endif

#if !defined(OPENSSL_NO_ASM)
#if defined(BORINGSSL_PREFIX)
#include <boringssl_prefix_symbols_asm.h>
#endif
.text

// abi_test_trampoline loads callee-saved registers from |state|, calls |func|
// with |argv|, then saves the callee-saved registers into |state|. It returns
// the result of |func|. The |unwind| argument is unused.
// uint64_t abi_test_trampoline(void (*func)(...), CallerState *state,
//                              const uint64_t *argv, size_t argc,
//                              uint64_t unwind);

.globl	_abi_test_trampoline
.private_extern	_abi_test_trampoline
.align	4
_abi_test_trampoline:
Labi_test_trampoline_begin:
	// Stack layout (low to high addresses)
	//   x29,x30 (16 bytes)
	//    d8-d15 (64 bytes)
	//   x19-x28 (80 bytes)
	//    x1 (8 bytes)
	//   padding (8 bytes)
	stp	x29, x30, [sp, #-176]!
	mov	x29, sp

	// Saved callee-saved registers and |state|.
	stp	d8, d9, [sp, #16]
	stp	d10, d11, [sp, #32]
	stp	d12, d13, [sp, #48]
	stp	d14, d15, [sp, #64]
	stp	x19, x20, [sp, #80]
	stp	x21, x22, [sp, #96]
	stp	x23, x24, [sp, #112]
	stp	x25, x26, [sp, #128]
	stp	x27, x28, [sp, #144]
	str	x1, [sp, #160]

	// Load registers from |state|, with the exception of x29. x29 is the
	// frame pointer and also callee-saved, but AAPCS64 allows platforms to
	// mandate that x29 always point to a frame. iOS64 does so, which means
	// we cannot fill x29 with entropy without violating ABI rules
	// ourselves. x29 is tested separately below.
	ldp	d8, d9, [x1], #16
	ldp	d10, d11, [x1], #16
	ldp	d12, d13, [x1], #16
	ldp	d14, d15, [x1], #16
	ldp	x19, x20, [x1], #16
	ldp	x21, x22, [x1], #16
	ldp	x23, x24, [x1], #16
	ldp	x25, x26, [x1], #16
	ldp	x27, x28, [x1], #16

	// Move parameters into temporary registers.
	mov	x9, x0
	mov	x10, x2
	mov	x11, x3

	// Load parameters into registers.
	cbz	x11, Largs_done
	ldr	x0, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x1, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x2, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x3, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x4, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x5, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x6, [x10], #8
	subs	x11, x11, #1
	b.eq	Largs_done
	ldr	x7, [x10], #8

Largs_done:
	blr	x9

	// Reload |state| and store registers.
	ldr	x1, [sp, #160]
	stp	d8, d9, [x1], #16
	stp	d10, d11, [x1], #16
	stp	d12, d13, [x1], #16
	stp	d14, d15, [x1], #16
	stp	x19, x20, [x1], #16
	stp	x21, x22, [x1], #16
	stp	x23, x24, [x1], #16
	stp	x25, x26, [x1], #16
	stp	x27, x28, [x1], #16

	// |func| is required to preserve x29, the frame pointer. We cannot load
	// random values into x29 (see comment above), so compare it against the
	// expected value and zero the field of |state| if corrupted.
	mov	x9, sp
	cmp	x29, x9
	b.eq	Lx29_ok
	str	xzr, [x1]

Lx29_ok:
	// Restore callee-saved registers.
	ldp	d8, d9, [sp, #16]
	ldp	d10, d11, [sp, #32]
	ldp	d12, d13, [sp, #48]
	ldp	d14, d15, [sp, #64]
	ldp	x19, x20, [sp, #80]
	ldp	x21, x22, [sp, #96]
	ldp	x23, x24, [sp, #112]
	ldp	x25, x26, [sp, #128]
	ldp	x27, x28, [sp, #144]

	ldp	x29, x30, [sp], #176
	ret


.globl	_abi_test_clobber_x0
.private_extern	_abi_test_clobber_x0
.align	4
_abi_test_clobber_x0:
	mov	x0, xzr
	ret


.globl	_abi_test_clobber_x1
.private_extern	_abi_test_clobber_x1
.align	4
_abi_test_clobber_x1:
	mov	x1, xzr
	ret


.globl	_abi_test_clobber_x2
.private_extern	_abi_test_clobber_x2
.align	4
_abi_test_clobber_x2:
	mov	x2, xzr
	ret


.globl	_abi_test_clobber_x3
.private_extern	_abi_test_clobber_x3
.align	4
_abi_test_clobber_x3:
	mov	x3, xzr
	ret


.globl	_abi_test_clobber_x4
.private_extern	_abi_test_clobber_x4
.align	4
_abi_test_clobber_x4:
	mov	x4, xzr
	ret


.globl	_abi_test_clobber_x5
.private_extern	_abi_test_clobber_x5
.align	4
_abi_test_clobber_x5:
	mov	x5, xzr
	ret


.globl	_abi_test_clobber_x6
.private_extern	_abi_test_clobber_x6
.align	4
_abi_test_clobber_x6:
	mov	x6, xzr
	ret


.globl	_abi_test_clobber_x7
.private_extern	_abi_test_clobber_x7
.align	4
_abi_test_clobber_x7:
	mov	x7, xzr
	ret


.globl	_abi_test_clobber_x8
.private_extern	_abi_test_clobber_x8
.align	4
_abi_test_clobber_x8:
	mov	x8, xzr
	ret


.globl	_abi_test_clobber_x9
.private_extern	_abi_test_clobber_x9
.align	4
_abi_test_clobber_x9:
	mov	x9, xzr
	ret


.globl	_abi_test_clobber_x10
.private_extern	_abi_test_clobber_x10
.align	4
_abi_test_clobber_x10:
	mov	x10, xzr
	ret


.globl	_abi_test_clobber_x11
.private_extern	_abi_test_clobber_x11
.align	4
_abi_test_clobber_x11:
	mov	x11, xzr
	ret


.globl	_abi_test_clobber_x12
.private_extern	_abi_test_clobber_x12
.align	4
_abi_test_clobber_x12:
	mov	x12, xzr
	ret


.globl	_abi_test_clobber_x13
.private_extern	_abi_test_clobber_x13
.align	4
_abi_test_clobber_x13:
	mov	x13, xzr
	ret


.globl	_abi_test_clobber_x14
.private_extern	_abi_test_clobber_x14
.align	4
_abi_test_clobber_x14:
	mov	x14, xzr
	ret


.globl	_abi_test_clobber_x15
.private_extern	_abi_test_clobber_x15
.align	4
_abi_test_clobber_x15:
	mov	x15, xzr
	ret


.globl	_abi_test_clobber_x16
.private_extern	_abi_test_clobber_x16
.align	4
_abi_test_clobber_x16:
	mov	x16, xzr
	ret


.globl	_abi_test_clobber_x17
.private_extern	_abi_test_clobber_x17
.align	4
_abi_test_clobber_x17:
	mov	x17, xzr
	ret


.globl	_abi_test_clobber_x19
.private_extern	_abi_test_clobber_x19
.align	4
_abi_test_clobber_x19:
	mov	x19, xzr
	ret


.globl	_abi_test_clobber_x20
.private_extern	_abi_test_clobber_x20
.align	4
_abi_test_clobber_x20:
	mov	x20, xzr
	ret


.globl	_abi_test_clobber_x21
.private_extern	_abi_test_clobber_x21
.align	4
_abi_test_clobber_x21:
	mov	x21, xzr
	ret


.globl	_abi_test_clobber_x22
.private_extern	_abi_test_clobber_x22
.align	4
_abi_test_clobber_x22:
	mov	x22, xzr
	ret


.globl	_abi_test_clobber_x23
.private_extern	_abi_test_clobber_x23
.align	4
_abi_test_clobber_x23:
	mov	x23, xzr
	ret


.globl	_abi_test_clobber_x24
.private_extern	_abi_test_clobber_x24
.align	4
_abi_test_clobber_x24:
	mov	x24, xzr
	ret


.globl	_abi_test_clobber_x25
.private_extern	_abi_test_clobber_x25
.align	4
_abi_test_clobber_x25:
	mov	x25, xzr
	ret


.globl	_abi_test_clobber_x26
.private_extern	_abi_test_clobber_x26
.align	4
_abi_test_clobber_x26:
	mov	x26, xzr
	ret


.globl	_abi_test_clobber_x27
.private_extern	_abi_test_clobber_x27
.align	4
_abi_test_clobber_x27:
	mov	x27, xzr
	ret


.globl	_abi_test_clobber_x28
.private_extern	_abi_test_clobber_x28
.align	4
_abi_test_clobber_x28:
	mov	x28, xzr
	ret


.globl	_abi_test_clobber_x29
.private_extern	_abi_test_clobber_x29
.align	4
_abi_test_clobber_x29:
	mov	x29, xzr
	ret


.globl	_abi_test_clobber_d0
.private_extern	_abi_test_clobber_d0
.align	4
_abi_test_clobber_d0:
	fmov	d0, xzr
	ret


.globl	_abi_test_clobber_d1
.private_extern	_abi_test_clobber_d1
.align	4
_abi_test_clobber_d1:
	fmov	d1, xzr
	ret


.globl	_abi_test_clobber_d2
.private_extern	_abi_test_clobber_d2
.align	4
_abi_test_clobber_d2:
	fmov	d2, xzr
	ret


.globl	_abi_test_clobber_d3
.private_extern	_abi_test_clobber_d3
.align	4
_abi_test_clobber_d3:
	fmov	d3, xzr
	ret


.globl	_abi_test_clobber_d4
.private_extern	_abi_test_clobber_d4
.align	4
_abi_test_clobber_d4:
	fmov	d4, xzr
	ret


.globl	_abi_test_clobber_d5
.private_extern	_abi_test_clobber_d5
.align	4
_abi_test_clobber_d5:
	fmov	d5, xzr
	ret


.globl	_abi_test_clobber_d6
.private_extern	_abi_test_clobber_d6
.align	4
_abi_test_clobber_d6:
	fmov	d6, xzr
	ret


.globl	_abi_test_clobber_d7
.private_extern	_abi_test_clobber_d7
.align	4
_abi_test_clobber_d7:
	fmov	d7, xzr
	ret


.globl	_abi_test_clobber_d8
.private_extern	_abi_test_clobber_d8
.align	4
_abi_test_clobber_d8:
	fmov	d8, xzr
	ret


.globl	_abi_test_clobber_d9
.private_extern	_abi_test_clobber_d9
.align	4
_abi_test_clobber_d9:
	fmov	d9, xzr
	ret


.globl	_abi_test_clobber_d10
.private_extern	_abi_test_clobber_d10
.align	4
_abi_test_clobber_d10:
	fmov	d10, xzr
	ret


.globl	_abi_test_clobber_d11
.private_extern	_abi_test_clobber_d11
.align	4
_abi_test_clobber_d11:
	fmov	d11, xzr
	ret


.globl	_abi_test_clobber_d12
.private_extern	_abi_test_clobber_d12
.align	4
_abi_test_clobber_d12:
	fmov	d12, xzr
	ret


.globl	_abi_test_clobber_d13
.private_extern	_abi_test_clobber_d13
.align	4
_abi_test_clobber_d13:
	fmov	d13, xzr
	ret


.globl	_abi_test_clobber_d14
.private_extern	_abi_test_clobber_d14
.align	4
_abi_test_clobber_d14:
	fmov	d14, xzr
	ret


.globl	_abi_test_clobber_d15
.private_extern	_abi_test_clobber_d15
.align	4
_abi_test_clobber_d15:
	fmov	d15, xzr
	ret


.globl	_abi_test_clobber_d16
.private_extern	_abi_test_clobber_d16
.align	4
_abi_test_clobber_d16:
	fmov	d16, xzr
	ret


.globl	_abi_test_clobber_d17
.private_extern	_abi_test_clobber_d17
.align	4
_abi_test_clobber_d17:
	fmov	d17, xzr
	ret


.globl	_abi_test_clobber_d18
.private_extern	_abi_test_clobber_d18
.align	4
_abi_test_clobber_d18:
	fmov	d18, xzr
	ret


.globl	_abi_test_clobber_d19
.private_extern	_abi_test_clobber_d19
.align	4
_abi_test_clobber_d19:
	fmov	d19, xzr
	ret


.globl	_abi_test_clobber_d20
.private_extern	_abi_test_clobber_d20
.align	4
_abi_test_clobber_d20:
	fmov	d20, xzr
	ret


.globl	_abi_test_clobber_d21
.private_extern	_abi_test_clobber_d21
.align	4
_abi_test_clobber_d21:
	fmov	d21, xzr
	ret


.globl	_abi_test_clobber_d22
.private_extern	_abi_test_clobber_d22
.align	4
_abi_test_clobber_d22:
	fmov	d22, xzr
	ret


.globl	_abi_test_clobber_d23
.private_extern	_abi_test_clobber_d23
.align	4
_abi_test_clobber_d23:
	fmov	d23, xzr
	ret


.globl	_abi_test_clobber_d24
.private_extern	_abi_test_clobber_d24
.align	4
_abi_test_clobber_d24:
	fmov	d24, xzr
	ret


.globl	_abi_test_clobber_d25
.private_extern	_abi_test_clobber_d25
.align	4
_abi_test_clobber_d25:
	fmov	d25, xzr
	ret


.globl	_abi_test_clobber_d26
.private_extern	_abi_test_clobber_d26
.align	4
_abi_test_clobber_d26:
	fmov	d26, xzr
	ret


.globl	_abi_test_clobber_d27
.private_extern	_abi_test_clobber_d27
.align	4
_abi_test_clobber_d27:
	fmov	d27, xzr
	ret


.globl	_abi_test_clobber_d28
.private_extern	_abi_test_clobber_d28
.align	4
_abi_test_clobber_d28:
	fmov	d28, xzr
	ret


.globl	_abi_test_clobber_d29
.private_extern	_abi_test_clobber_d29
.align	4
_abi_test_clobber_d29:
	fmov	d29, xzr
	ret


.globl	_abi_test_clobber_d30
.private_extern	_abi_test_clobber_d30
.align	4
_abi_test_clobber_d30:
	fmov	d30, xzr
	ret


.globl	_abi_test_clobber_d31
.private_extern	_abi_test_clobber_d31
.align	4
_abi_test_clobber_d31:
	fmov	d31, xzr
	ret


.globl	_abi_test_clobber_v8_upper
.private_extern	_abi_test_clobber_v8_upper
.align	4
_abi_test_clobber_v8_upper:
	fmov	v8.d[1], xzr
	ret


.globl	_abi_test_clobber_v9_upper
.private_extern	_abi_test_clobber_v9_upper
.align	4
_abi_test_clobber_v9_upper:
	fmov	v9.d[1], xzr
	ret


.globl	_abi_test_clobber_v10_upper
.private_extern	_abi_test_clobber_v10_upper
.align	4
_abi_test_clobber_v10_upper:
	fmov	v10.d[1], xzr
	ret


.globl	_abi_test_clobber_v11_upper
.private_extern	_abi_test_clobber_v11_upper
.align	4
_abi_test_clobber_v11_upper:
	fmov	v11.d[1], xzr
	ret


.globl	_abi_test_clobber_v12_upper
.private_extern	_abi_test_clobber_v12_upper
.align	4
_abi_test_clobber_v12_upper:
	fmov	v12.d[1], xzr
	ret


.globl	_abi_test_clobber_v13_upper
.private_extern	_abi_test_clobber_v13_upper
.align	4
_abi_test_clobber_v13_upper:
	fmov	v13.d[1], xzr
	ret


.globl	_abi_test_clobber_v14_upper
.private_extern	_abi_test_clobber_v14_upper
.align	4
_abi_test_clobber_v14_upper:
	fmov	v14.d[1], xzr
	ret


.globl	_abi_test_clobber_v15_upper
.private_extern	_abi_test_clobber_v15_upper
.align	4
_abi_test_clobber_v15_upper:
	fmov	v15.d[1], xzr
	ret

#endif  // !OPENSSL_NO_ASM
