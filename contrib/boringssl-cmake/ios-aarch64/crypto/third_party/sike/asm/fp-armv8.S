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
.section	__TEXT,__const

# p434 x 2
Lp434x2:
.quad	0xFFFFFFFFFFFFFFFE, 0xFFFFFFFFFFFFFFFF
.quad	0xFB82ECF5C5FFFFFF, 0xF78CB8F062B15D47
.quad	0xD9F8BFAD038A40AC, 0x0004683E4E2EE688

# p434 + 1
Lp434p1:
.quad	0xFDC1767AE3000000, 0x7BC65C783158AEA3
.quad	0x6CFC5FD681C52056, 0x0002341F27177344

.text
.globl	_sike_mpmul
.private_extern	_sike_mpmul
.align	4
_sike_mpmul:
	stp	x29, x30, [sp,#-96]!
	add	x29, sp, #0
	stp	x19, x20, [sp,#16]
	stp	x21, x22, [sp,#32]
	stp	x23, x24, [sp,#48]
	stp	x25, x26, [sp,#64]
	stp	x27, x28, [sp,#80]

	ldp	x3,  x4, [x0]
	ldp	x5,  x6, [x0,#16]
	ldp	x7,  x8, [x0,#32]
	ldr	x9,      [x0,#48]
	ldp	x10, x11, [x1,#0]
	ldp	x12, x13, [x1,#16]
	ldp	x14, x15, [x1,#32]
	ldr	x16,      [x1,#48]

        // x3-x7 <- AH + AL, x7 <- carry
	adds	x3, x3, x7
	adcs	x4, x4, x8
	adcs	x5, x5, x9
	adcs	x6, x6, xzr
	adc	x7, xzr, xzr

        // x10-x13 <- BH + BL, x8 <- carry
	adds	x10, x10, x14
	adcs	x11, x11, x15
	adcs	x12, x12, x16
	adcs	x13, x13, xzr
	adc	x8, xzr, xzr

        // x9 <- combined carry
	and	x9, x7, x8
        // x7-x8 <- mask
	sub	x7, xzr, x7
	sub	x8, xzr, x8

        // x15-x19 <- masked (BH + BL)
	and	x14, x10, x7
	and	x15, x11, x7
	and	x16, x12, x7
	and	x17, x13, x7

        // x20-x23 <- masked (AH + AL)
	and	x20, x3, x8
	and	x21, x4, x8
	and	x22, x5, x8
	and	x23, x6, x8

        // x15-x19, x7 <- masked (AH+AL) + masked (BH+BL), step 1
	adds	x14, x14, x20
	adcs	x15, x15, x21
	adcs	x16, x16, x22
	adcs	x17, x17, x23
	adc	x7, x9, xzr

        // x8-x9,x19,x20-x24 <- (AH+AL) x (BH+BL), low part
	stp	x3, x4, [x2,#0]
                // A0-A1 <- AH + AL, T0 <- mask
	adds	x3, x3, x5
	adcs	x4, x4, x6
	adc	x25, xzr, xzr

        // C6, T1 <- BH + BL, C7 <- mask
	adds	x23, x10, x12
	adcs	x26, x11, x13
	adc	x24, xzr, xzr

        // C0-C1 <- masked (BH + BL)
	sub	x19, xzr, x25
	sub	x20, xzr, x24
	and	x8, x23, x19
	and	x9, x26, x19

        // C4-C5 <- masked (AH + AL), T0 <- combined carry
	and	x21, x3, x20
	and	x22, x4, x20
	mul	x19, x3, x23
	mul	x20, x3, x26
	and	x25, x25, x24

        // C0-C1, T0 <- (AH+AL) x (BH+BL), part 1
	adds	x8, x21, x8
	umulh	x21, x3, x26
	adcs	x9, x22, x9
	umulh	x22, x3, x23
	adc	x25, x25, xzr

        // C2-C5 <- (AH+AL) x (BH+BL), low part
	mul	x3, x4, x23
	umulh	x23, x4, x23
	adds	x20, x20, x22
	adc	x21, x21, xzr

	mul	x24, x4, x26
	umulh	x26, x4, x26
	adds	x20, x20, x3
	adcs	x21, x21, x23
	adc	x22, xzr, xzr

	adds	x21, x21, x24
	adc	x22, x22, x26

	ldp	x3, x4, [x2,#0]

        // C2-C5, T0 <- (AH+AL) x (BH+BL), final part
	adds	x21, x8, x21
	umulh	x24, x3, x10
	umulh	x26, x3, x11
	adcs	x22, x9, x22
	mul	x8, x3, x10
	mul	x9, x3, x11
	adc	x25, x25, xzr

        // C0-C1, T1, C7 <- AL x BL
	mul	x3, x4, x10
	umulh	x10, x4, x10
	adds	x9, x9, x24
	adc	x26, x26, xzr

	mul	x23, x4, x11
	umulh	x11, x4, x11
	adds	x9, x9, x3
	adcs	x26, x26, x10
	adc	x24, xzr, xzr

	adds	x26, x26, x23
	adc	x24, x24, x11


        // C2-C5, T0 <- (AH+AL) x (BH+BL) - ALxBL
	mul	x3, x5, x12
	umulh	x10, x5, x12
	subs	x19, x19, x8
	sbcs	x20, x20, x9
	sbcs	x21, x21, x26
	mul	x4, x5, x13
	umulh	x23, x5, x13
	sbcs	x22, x22, x24
	sbc	x25, x25, xzr

        // A0, A1, C6, B0 <- AH x BH
	mul	x5, x6, x12
	umulh	x12, x6, x12
	adds	x4, x4, x10
	adc	x23, x23, xzr

	mul	x11, x6, x13
	umulh	x13, x6, x13
	adds	x4, x4, x5
	adcs	x23, x23, x12
	adc	x10, xzr, xzr

	adds	x23, x23, x11
	adc	x10, x10, x13


        // C2-C5, T0 <- (AH+AL) x (BH+BL) - ALxBL - AHxBH
	subs	x19, x19, x3
	sbcs	x20, x20, x4
	sbcs	x21, x21, x23
	sbcs	x22, x22, x10
	sbc	x25, x25, xzr

	adds	x19, x19, x26
	adcs	x20, x20, x24
	adcs	x21, x21, x3
	adcs	x22, x22, x4
	adcs	x23, x25, x23
	adc	x24, x10, xzr


        // x15-x19, x7 <- (AH+AL) x (BH+BL), final step
	adds	x14, x14, x21
	adcs	x15, x15, x22
	adcs	x16, x16, x23
	adcs	x17, x17, x24
	adc	x7, x7, xzr

        // Load AL
	ldp	x3, x4, [x0]
	ldp	x5, x6, [x0,#16]
        // Load BL
	ldp	x10, x11, [x1,#0]
	ldp	x12, x13, [x1,#16]

        // Temporarily store x8 in x2
	stp	x8, x9, [x2,#0]
        // x21-x28 <- AL x BL
                // A0-A1 <- AH + AL, T0 <- mask
	adds	x3, x3, x5
	adcs	x4, x4, x6
	adc	x8, xzr, xzr

        // C6, T1 <- BH + BL, C7 <- mask
	adds	x27, x10, x12
	adcs	x9, x11, x13
	adc	x28, xzr, xzr

        // C0-C1 <- masked (BH + BL)
	sub	x23, xzr, x8
	sub	x24, xzr, x28
	and	x21, x27, x23
	and	x22, x9, x23

        // C4-C5 <- masked (AH + AL), T0 <- combined carry
	and	x25, x3, x24
	and	x26, x4, x24
	mul	x23, x3, x27
	mul	x24, x3, x9
	and	x8, x8, x28

        // C0-C1, T0 <- (AH+AL) x (BH+BL), part 1
	adds	x21, x25, x21
	umulh	x25, x3, x9
	adcs	x22, x26, x22
	umulh	x26, x3, x27
	adc	x8, x8, xzr

        // C2-C5 <- (AH+AL) x (BH+BL), low part
	mul	x3, x4, x27
	umulh	x27, x4, x27
	adds	x24, x24, x26
	adc	x25, x25, xzr

	mul	x28, x4, x9
	umulh	x9, x4, x9
	adds	x24, x24, x3
	adcs	x25, x25, x27
	adc	x26, xzr, xzr

	adds	x25, x25, x28
	adc	x26, x26, x9

	ldp	x3, x4, [x0,#0]

        // C2-C5, T0 <- (AH+AL) x (BH+BL), final part
	adds	x25, x21, x25
	umulh	x28, x3, x10
	umulh	x9, x3, x11
	adcs	x26, x22, x26
	mul	x21, x3, x10
	mul	x22, x3, x11
	adc	x8, x8, xzr

        // C0-C1, T1, C7 <- AL x BL
	mul	x3, x4, x10
	umulh	x10, x4, x10
	adds	x22, x22, x28
	adc	x9, x9, xzr

	mul	x27, x4, x11
	umulh	x11, x4, x11
	adds	x22, x22, x3
	adcs	x9, x9, x10
	adc	x28, xzr, xzr

	adds	x9, x9, x27
	adc	x28, x28, x11


        // C2-C5, T0 <- (AH+AL) x (BH+BL) - ALxBL
	mul	x3, x5, x12
	umulh	x10, x5, x12
	subs	x23, x23, x21
	sbcs	x24, x24, x22
	sbcs	x25, x25, x9
	mul	x4, x5, x13
	umulh	x27, x5, x13
	sbcs	x26, x26, x28
	sbc	x8, x8, xzr

        // A0, A1, C6, B0 <- AH x BH
	mul	x5, x6, x12
	umulh	x12, x6, x12
	adds	x4, x4, x10
	adc	x27, x27, xzr

	mul	x11, x6, x13
	umulh	x13, x6, x13
	adds	x4, x4, x5
	adcs	x27, x27, x12
	adc	x10, xzr, xzr

	adds	x27, x27, x11
	adc	x10, x10, x13


        // C2-C5, T0 <- (AH+AL) x (BH+BL) - ALxBL - AHxBH
	subs	x23, x23, x3
	sbcs	x24, x24, x4
	sbcs	x25, x25, x27
	sbcs	x26, x26, x10
	sbc	x8, x8, xzr

	adds	x23, x23, x9
	adcs	x24, x24, x28
	adcs	x25, x25, x3
	adcs	x26, x26, x4
	adcs	x27, x8, x27
	adc	x28, x10, xzr

        // Restore x8
	ldp	x8, x9, [x2,#0]

        // x8-x10,x20,x15-x17,x19 <- maskd (AH+AL) x (BH+BL) - ALxBL
	subs	x8, x8, x21
	sbcs	x9, x9, x22
	sbcs	x19, x19, x23
	sbcs	x20, x20, x24
	sbcs	x14, x14, x25
	sbcs	x15, x15, x26
	sbcs	x16, x16, x27
	sbcs	x17, x17, x28
	sbc	x7, x7, xzr

        // Store ALxBL, low
	stp	x21, x22, [x2]
	stp	x23, x24, [x2,#16]

        // Load AH
	ldp	x3, x4, [x0,#32]
	ldr	x5,     [x0,#48]
        // Load BH
	ldp	x10, x11, [x1,#32]
	ldr	x12,      [x1,#48]

	adds	x8,  x8, x25
	adcs	x9,  x9, x26
	adcs	x19, x19, x27
	adcs	x20, x20, x28
	adc	x1, xzr, xzr

	add	x0, x0, #32
        // Temporarily store x8,x9 in x2
	stp	x8,x9, [x2,#32]
        // x21-x28 <- AH x BH

        // A0 * B0
	mul	x21, x3, x10  // C0
	umulh	x24, x3, x10

        // A0 * B1
	mul	x22, x3, x11
	umulh	x23, x3, x11

        // A1 * B0
	mul	x8, x4, x10
	umulh	x9, x4, x10
	adds	x22, x22, x24
	adc	x23, x23, xzr

        // A0 * B2
	mul	x27, x3, x12
	umulh	x28, x3, x12
	adds	x22, x22, x8  // C1
	adcs	x23, x23, x9
	adc	x24, xzr, xzr

        // A2 * B0
	mul	x8, x5, x10
	umulh	x25, x5, x10
	adds	x23, x23, x27
	adcs	x24, x24, x25
	adc	x25, xzr, xzr

        // A1 * B1
	mul	x27, x4, x11
	umulh	x9, x4, x11
	adds	x23, x23, x8
	adcs	x24, x24, x28
	adc	x25, x25, xzr

        // A1 * B2
	mul	x8, x4, x12
	umulh	x28, x4, x12
	adds	x23, x23, x27 // C2
	adcs	x24, x24, x9
	adc	x25, x25, xzr

        // A2 * B1
	mul	x27, x5, x11
	umulh	x9, x5, x11
	adds	x24, x24, x8
	adcs	x25, x25, x28
	adc	x26, xzr, xzr

        // A2 * B2
	mul	x8, x5, x12
	umulh	x28, x5, x12
	adds	x24, x24, x27 // C3
	adcs	x25, x25, x9
	adc	x26, x26, xzr

	adds	x25, x25, x8 // C4
	adc	x26, x26, x28 // C5

        // Restore x8,x9
	ldp	x8,x9, [x2,#32]

	neg	x1, x1

        // x8-x9,x19,x20,x14-x17 <- (AH+AL) x (BH+BL) - ALxBL - AHxBH
	subs	x8, x8, x21
	sbcs	x9, x9, x22
	sbcs	x19, x19, x23
	sbcs	x20, x20, x24
	sbcs	x14, x14, x25
	sbcs	x15, x15, x26
	sbcs	x16, x16, xzr
	sbcs	x17, x17, xzr
	sbc	x7, x7, xzr

        // Store (AH+AL) x (BH+BL) - ALxBL - AHxBH, low
	stp	x8,  x9, [x2,#32]
	stp	x19, x20, [x2,#48]

	adds	x1,  x1, #1
	adcs	x14, x14, x21
	adcs	x15, x15, x22
	adcs	x16, x16, x23
	adcs	x17, x17, x24
	adcs	x25,  x7, x25
	adc	x26, x26, xzr

	stp	x14, x15, [x2,#64]
	stp	x16, x17, [x2,#80]
	stp	x25, x26, [x2,#96]

	ldp	x19, x20, [x29,#16]
	ldp	x21, x22, [x29,#32]
	ldp	x23, x24, [x29,#48]
	ldp	x25, x26, [x29,#64]
	ldp	x27, x28, [x29,#80]
	ldp	x29, x30, [sp],#96
	ret
.globl	_sike_fprdc
.private_extern	_sike_fprdc
.align	4
_sike_fprdc:
	stp	x29, x30, [sp, #-96]!
	add	x29, sp, xzr
	stp	x19, x20, [sp,#16]
	stp	x21, x22, [sp,#32]
	stp	x23, x24, [sp,#48]
	stp	x25, x26, [sp,#64]
	stp	x27, x28, [sp,#80]

	ldp	x2, x3, [x0,#0]       // a[0-1]

        // Load the prime constant
	adrp	x26, Lp434p1@PAGE
	add	x26, x26, Lp434p1@PAGEOFF
	ldp	x23, x24, [x26, #0x0]
	ldp	x25, x26, [x26,#0x10]

        // a[0-1] * p434+1
	mul	x4, x2, x23  // C0
	umulh	x7, x2, x23

	mul	x5, x2, x24
	umulh	x6, x2, x24

	mul	x10, x3, x23
	umulh	x11, x3, x23
	adds	x5, x5, x7
	adc	x6, x6, xzr

	mul	x27, x2, x25
	umulh	x28, x2, x25
	adds	x5, x5, x10  // C1
	adcs	x6, x6, x11
	adc	x7, xzr, xzr

	mul	x10, x3, x24
	umulh	x11, x3, x24
	adds	x6, x6, x27
	adcs	x7, x7, x28
	adc	x8, xzr, xzr

	mul	x27, x2, x26
	umulh	x28, x2, x26
	adds	x6, x6, x10  // C2
	adcs	x7, x7, x11
	adc	x8, x8, xzr

	mul	x10, x3, x25
	umulh	x11, x3, x25
	adds	x7, x7, x27
	adcs	x8, x8, x28
	adc	x9, xzr, xzr

	mul	x27, x3, x26
	umulh	x28, x3, x26
	adds	x7, x7, x10  // C3
	adcs	x8, x8, x11
	adc	x9, x9, xzr
	adds	x8, x8, x27  // C4
	adc	x9, x9, x28  // C5



	ldp	x10, x11, [x0, #0x18]
	ldp	x12, x13, [x0, #0x28]
	ldp	x14, x15, [x0, #0x38]
	ldp	x16, x17, [x0, #0x48]
	ldp	x19, x20, [x0, #0x58]
	ldr	x21,      [x0, #0x68]

	adds	x10, x10, x4
	adcs	x11, x11, x5
	adcs	x12, x12, x6
	adcs	x13, x13, x7
	adcs	x14, x14, x8
	adcs	x15, x15, x9
	adcs	x22, x16, xzr
	adcs	x17, x17, xzr
	adcs	x19, x19, xzr
	adcs	x20, x20, xzr
	adc	x21, x21, xzr

	ldr	x2,  [x0,#0x10]       // a[2]
        // a[2-3] * p434+1
	mul	x4, x2, x23  // C0
	umulh	x7, x2, x23

	mul	x5, x2, x24
	umulh	x6, x2, x24

	mul	x0, x10, x23
	umulh	x3, x10, x23
	adds	x5, x5, x7
	adc	x6, x6, xzr

	mul	x27, x2, x25
	umulh	x28, x2, x25
	adds	x5, x5, x0  // C1
	adcs	x6, x6, x3
	adc	x7, xzr, xzr

	mul	x0, x10, x24
	umulh	x3, x10, x24
	adds	x6, x6, x27
	adcs	x7, x7, x28
	adc	x8, xzr, xzr

	mul	x27, x2, x26
	umulh	x28, x2, x26
	adds	x6, x6, x0  // C2
	adcs	x7, x7, x3
	adc	x8, x8, xzr

	mul	x0, x10, x25
	umulh	x3, x10, x25
	adds	x7, x7, x27
	adcs	x8, x8, x28
	adc	x9, xzr, xzr

	mul	x27, x10, x26
	umulh	x28, x10, x26
	adds	x7, x7, x0  // C3
	adcs	x8, x8, x3
	adc	x9, x9, xzr
	adds	x8, x8, x27  // C4
	adc	x9, x9, x28  // C5



	adds	x12, x12, x4
	adcs	x13, x13, x5
	adcs	x14, x14, x6
	adcs	x15, x15, x7
	adcs	x16, x22, x8
	adcs	x17, x17, x9
	adcs	x22, x19, xzr
	adcs	x20, x20, xzr
	adc	x21, x21, xzr

	mul	x4, x11, x23  // C0
	umulh	x7, x11, x23

	mul	x5, x11, x24
	umulh	x6, x11, x24

	mul	x10, x12, x23
	umulh	x3, x12, x23
	adds	x5, x5, x7
	adc	x6, x6, xzr

	mul	x27, x11, x25
	umulh	x28, x11, x25
	adds	x5, x5, x10  // C1
	adcs	x6, x6, x3
	adc	x7, xzr, xzr

	mul	x10, x12, x24
	umulh	x3, x12, x24
	adds	x6, x6, x27
	adcs	x7, x7, x28
	adc	x8, xzr, xzr

	mul	x27, x11, x26
	umulh	x28, x11, x26
	adds	x6, x6, x10  // C2
	adcs	x7, x7, x3
	adc	x8, x8, xzr

	mul	x10, x12, x25
	umulh	x3, x12, x25
	adds	x7, x7, x27
	adcs	x8, x8, x28
	adc	x9, xzr, xzr

	mul	x27, x12, x26
	umulh	x28, x12, x26
	adds	x7, x7, x10  // C3
	adcs	x8, x8, x3
	adc	x9, x9, xzr
	adds	x8, x8, x27  // C4
	adc	x9, x9, x28  // C5


	adds	x14, x14, x4
	adcs	x15, x15, x5
	adcs	x16, x16, x6
	adcs	x17, x17, x7
	adcs	x19, x22, x8
	adcs	x20, x20, x9
	adc	x22, x21, xzr

	stp	x14, x15, [x1, #0x0]     // C0, C1

	mul	x4, x13, x23    // C0
	umulh	x10, x13, x23

	mul	x5, x13, x24
	umulh	x27, x13, x24
	adds	x5, x5, x10    // C1
	adc	x10, xzr, xzr

	mul	x6, x13, x25
	umulh	x28, x13, x25
	adds	x27, x10, x27
	adcs	x6, x6, x27    // C2
	adc	x10, xzr, xzr

	mul	x7, x13, x26
	umulh	x8, x13, x26
	adds	x28, x10, x28
	adcs	x7, x7, x28    // C3
	adc	x8, x8, xzr    // C4

	adds	x16, x16, x4
	adcs	x17, x17, x5
	adcs	x19, x19, x6
	adcs	x20, x20, x7
	adc	x21, x22, x8

	str	x16,       [x1, #0x10]
	stp	x17, x19,  [x1, #0x18]
	stp	x20, x21,  [x1, #0x28]

	ldp	x19, x20, [x29,#16]
	ldp	x21, x22, [x29,#32]
	ldp	x23, x24, [x29,#48]
	ldp	x25, x26, [x29,#64]
	ldp	x27, x28, [x29,#80]
	ldp	x29, x30, [sp],#96
	ret
.globl	_sike_fpadd
.private_extern	_sike_fpadd
.align	4
_sike_fpadd:
	stp	x29,x30, [sp,#-16]!
	add	x29, sp, #0

	ldp	x3, x4,   [x0,#0]
	ldp	x5, x6,   [x0,#16]
	ldp	x7, x8,   [x0,#32]
	ldr	x9,       [x0,#48]
	ldp	x11, x12, [x1,#0]
	ldp	x13, x14, [x1,#16]
	ldp	x15, x16, [x1,#32]
	ldr	x17,      [x1,#48]

        // Add a + b
	adds	x3, x3, x11
	adcs	x4, x4, x12
	adcs	x5, x5, x13
	adcs	x6, x6, x14
	adcs	x7, x7, x15
	adcs	x8, x8, x16
	adc	x9, x9, x17

        //  Subtract 2xp434
	adrp	x17, Lp434x2@PAGE
	add	x17, x17, Lp434x2@PAGEOFF
	ldp	x11, x12, [x17, #0]
	ldp	x13, x14, [x17, #16]
	ldp	x15, x16, [x17, #32]
	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x12
	sbcs	x6, x6, x13
	sbcs	x7, x7, x14
	sbcs	x8, x8, x15
	sbcs	x9, x9, x16
	sbc	x0, xzr, xzr    // x0 can be reused now

        // Add 2xp434 anded with the mask in x0
	and	x11, x11, x0
	and	x12, x12, x0
	and	x13, x13, x0
	and	x14, x14, x0
	and	x15, x15, x0
	and	x16, x16, x0

	adds	x3, x3, x11
	adcs	x4, x4, x12
	adcs	x5, x5, x12
	adcs	x6, x6, x13
	adcs	x7, x7, x14
	adcs	x8, x8, x15
	adc	x9, x9, x16

	stp	x3, x4,  [x2,#0]
	stp	x5, x6,  [x2,#16]
	stp	x7, x8,  [x2,#32]
	str	x9,      [x2,#48]

	ldp	x29, x30, [sp],#16
	ret
.globl	_sike_fpsub
.private_extern	_sike_fpsub
.align	4
_sike_fpsub:
	stp	x29, x30, [sp,#-16]!
	add	x29, sp, #0

	ldp	x3, x4,   [x0,#0]
	ldp	x5, x6,   [x0,#16]
	ldp	x7, x8,   [x0,#32]
	ldr	x9,       [x0,#48]
	ldp	x11, x12, [x1,#0]
	ldp	x13, x14, [x1,#16]
	ldp	x15, x16, [x1,#32]
	ldr	x17,      [x1,#48]

        // Subtract a - b
	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	sbcs	x7, x7, x15
	sbcs	x8, x8, x16
	sbcs	x9, x9, x17
	sbc	x0, xzr, xzr

        // Add 2xp434 anded with the mask in x0
	adrp	x17, Lp434x2@PAGE
	add	x17, x17, Lp434x2@PAGEOFF

        // First half
	ldp	x11, x12, [x17, #0]
	ldp	x13, x14, [x17, #16]
	ldp	x15, x16, [x17, #32]

        // Add 2xp434 anded with the mask in x0
	and	x11, x11, x0
	and	x12, x12, x0
	and	x13, x13, x0
	and	x14, x14, x0
	and	x15, x15, x0
	and	x16, x16, x0

	adds	x3, x3, x11
	adcs	x4, x4, x12
	adcs	x5, x5, x12
	adcs	x6, x6, x13
	adcs	x7, x7, x14
	adcs	x8, x8, x15
	adc	x9, x9, x16

	stp	x3, x4,  [x2,#0]
	stp	x5, x6,  [x2,#16]
	stp	x7, x8,  [x2,#32]
	str	x9,      [x2,#48]

	ldp	x29, x30, [sp],#16
	ret
.globl	_sike_mpadd_asm
.private_extern	_sike_mpadd_asm
.align	4
_sike_mpadd_asm:
	stp	x29, x30, [sp,#-16]!
	add	x29, sp, #0

	ldp	x3, x4,   [x0,#0]
	ldp	x5, x6,   [x0,#16]
	ldp	x7, x8,   [x0,#32]
	ldr	x9,       [x0,#48]
	ldp	x11, x12, [x1,#0]
	ldp	x13, x14, [x1,#16]
	ldp	x15, x16, [x1,#32]
	ldr	x17,      [x1,#48]

	adds	x3, x3, x11
	adcs	x4, x4, x12
	adcs	x5, x5, x13
	adcs	x6, x6, x14
	adcs	x7, x7, x15
	adcs	x8, x8, x16
	adc	x9, x9, x17

	stp	x3, x4,   [x2,#0]
	stp	x5, x6,   [x2,#16]
	stp	x7, x8,   [x2,#32]
	str	x9,       [x2,#48]

	ldp	x29, x30, [sp],#16
	ret
.globl	_sike_mpsubx2_asm
.private_extern	_sike_mpsubx2_asm
.align	4
_sike_mpsubx2_asm:
	stp	x29, x30, [sp,#-16]!
	add	x29, sp, #0

	ldp	x3, x4,   [x0,#0]
	ldp	x5, x6,   [x0,#16]
	ldp	x11, x12, [x1,#0]
	ldp	x13, x14, [x1,#16]
	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	ldp	x7, x8,   [x0,#32]
	ldp	x9, x10,  [x0,#48]
	ldp	x11, x12, [x1,#32]
	ldp	x13, x14, [x1,#48]
	sbcs	x7, x7, x11
	sbcs	x8, x8, x12
	sbcs	x9, x9, x13
	sbcs	x10, x10, x14

	stp	x3, x4,   [x2,#0]
	stp	x5, x6,   [x2,#16]
	stp	x7, x8,   [x2,#32]
	stp	x9, x10,  [x2,#48]

	ldp	x3, x4,   [x0,#64]
	ldp	x5, x6,   [x0,#80]
	ldp	x11, x12, [x1,#64]
	ldp	x13, x14, [x1,#80]
	sbcs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	ldp	x7, x8,   [x0,#96]
	ldp	x11, x12, [x1,#96]
	sbcs	x7, x7, x11
	sbcs	x8, x8, x12
	sbc	x0, xzr, xzr

	stp	x3, x4,   [x2,#64]
	stp	x5, x6,   [x2,#80]
	stp	x7, x8,   [x2,#96]

	ldp	x29, x30, [sp],#16
	ret
.globl	_sike_mpdblsubx2_asm
.private_extern	_sike_mpdblsubx2_asm
.align	4
_sike_mpdblsubx2_asm:
	stp	x29, x30, [sp, #-16]!
	add	x29, sp, #0

	ldp	x3, x4,   [x2, #0]
	ldp	x5, x6,   [x2,#16]
	ldp	x7, x8,   [x2,#32]

	ldp	x11, x12, [x0, #0]
	ldp	x13, x14, [x0,#16]
	ldp	x15, x16, [x0,#32]

	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	sbcs	x7, x7, x15
	sbcs	x8, x8, x16

        // x9 stores carry
	adc	x9, xzr, xzr

	ldp	x11, x12, [x1, #0]
	ldp	x13, x14, [x1,#16]
	ldp	x15, x16, [x1,#32]
	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	sbcs	x7, x7, x15
	sbcs	x8, x8, x16
	adc	x9, x9, xzr

	stp	x3, x4,   [x2, #0]
	stp	x5, x6,   [x2,#16]
	stp	x7, x8,   [x2,#32]

	ldp	x3, x4,   [x2,#48]
	ldp	x5, x6,   [x2,#64]
	ldp	x7, x8,   [x2,#80]

	ldp	x11, x12, [x0,#48]
	ldp	x13, x14, [x0,#64]
	ldp	x15, x16, [x0,#80]

        // x9 = 2 - x9
	neg	x9, x9
	add	x9, x9, #2

	subs	x3, x3, x9
	sbcs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	sbcs	x7, x7, x15
	sbcs	x8, x8, x16
	adc	x9, xzr, xzr

	ldp	x11, x12, [x1,#48]
	ldp	x13, x14, [x1,#64]
	ldp	x15, x16, [x1,#80]
	subs	x3, x3, x11
	sbcs	x4, x4, x12
	sbcs	x5, x5, x13
	sbcs	x6, x6, x14
	sbcs	x7, x7, x15
	sbcs	x8, x8, x16
	adc	x9, x9, xzr

	stp	x3, x4,   [x2,#48]
	stp	x5, x6,   [x2,#64]
	stp	x7, x8,   [x2,#80]

	ldp	x3,  x4, [x2,#96]
	ldp	x11, x12, [x0,#96]
	ldp	x13, x14, [x1,#96]

        // x9 = 2 - x9
	neg	x9, x9
	add	x9, x9, #2

	subs	x3, x3, x9
	sbcs	x3, x3, x11
	sbcs	x4, x4, x12
	subs	x3, x3, x13
	sbc	x4, x4, x14
	stp	x3, x4,   [x2,#96]

	ldp	x29, x30, [sp],#16
	ret
#endif  // !OPENSSL_NO_ASM
