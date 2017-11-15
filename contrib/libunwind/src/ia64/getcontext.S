/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
	Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#include "ucontext_i.h"

#define GR(n)   (SC_GR + (n)*8)
#define BR(n)   (SC_BR + (n)*8)
#define FR(n)   (SC_FR + (n)*16)

/* This should be compatible to the libc's getcontext(), except that
   the sc->sc_mask field is always cleared and that the name is
   prefixed with _Uia64_ so we don't step on the application's
   name-space.  */

	.align 32
	.protected _Uia64_getcontext
	.global _Uia64_getcontext
	.proc _Uia64_getcontext
_Uia64_getcontext:
	.prologue
	alloc rPFS = ar.pfs, 1, 0, 0, 0				// M2
	mov rPR = pr						// I0, 2 cycles
	add r2 = GR(1), in0					// I1
	;;

	.save ar.unat, rUNAT
	mov.m rUNAT = ar.unat					// M2, 5 cycles
	.body
	st8.spill [r2] = r1, (SC_FLAGS - GR(1))			// M3
	dep.z rFLAGS = -1, IA64_SC_FLAG_SYNCHRONOUS_BIT, 1	// I0, 1 cycle
	;;

	mov.m rRSC = ar.rsc					// M2, 12 cyc.
	st8 [r2] = rFLAGS, (SC_PR  - SC_FLAGS)			// M3
	add r3 = FR(2), in0
	;;

	mov.m rBSP = ar.bsp					// M2, 12 cyc.
	st8 [r2] = rPR, (GR(12) - SC_PR)			// M3
	add r8 = FR(16), in0
	;;

	mov.m rFPSR = ar.fpsr					// M2, 12 cyc.
	st8.spill [r2] = r12, (GR(4) - GR(12))			// M3
	add r9 = FR(24), in0
	;;

	stf.spill [r3] = f2					// M2
	stf.spill [r8] = f16					// M3
	add r3 = GR(7), in0
	;;

	flushrs							// M0
	stf.spill [r9] = f24, (FR(31) - FR(24))			// M2
	mov rB0 = b0						// I0, 2 cycles
	;;

	stf.spill [r9] = f31					// M2
	st8.spill [r2] = r4, (GR(5) - GR(4))			// M3, bank 1
	mov rB1 = b1						// I0, 2 cycles
	;;

.mem.offset 0,0; st8.spill [r2] = r5, (GR(6) - GR(5))		// M4, bank 0
.mem.offset 8,0; st8.spill [r3] = r7, (BR(0) - GR(7))		// M3, bank 0
	mov rB2 = b2						// I0, 2 cycles
	;;

	st8.spill [r2] = r6, (BR(1) - GR(6))			// M2, bank 1
	st8 [r3] = rB0, (BR(4) - BR(0))				// M3, bank 1
	mov rB4 = b4						// I0, 2 cycles
	;;

	mov.m rNAT = ar.unat					// M2, 5 cycles
	st8 [r2] = rB1, (BR(2) - BR(1))				// M3, bank 0
	mov rB3 = b3
	;;

	st8 [r2] = rB2, (BR(3) - BR(2))				// M2, bank 1
	st8 [r3] = rB4, (SC_LC - BR(4))				// M3, bank 1
	mov rB5 = b5						// I0, 2 cycles
	;;

	and rTMP = ~0x3, rRSC					// M0
	add rPOS = GR(0), in0	// rPOS <- &sc_gr[0]		// M1
	mov.i rLC = ar.lc					// I0, 2 cycles
	;;

	mov.m ar.rsc = rTMP	// put RSE into lazy mode	// M2, ? cycles
	st8 [r2] = rB3, (BR(5) - BR(3))				// M3, bank 0
	extr.u rPOS = rPOS, 3, 6 // get NaT bitnr for r0	// I0
	;;

	mov.m rRNAT = ar.rnat					// M2, 5 cycles
	st8 [r2] = rB5, (SC_PFS - BR(5))			// M3, bank 0
	sub rCPOS = 64, rPOS					// I0
	;;

	st8 [r2] = rPFS, (SC_UNAT - SC_PFS)			// M2
	st8 [r3] = rLC, (SC_BSP - SC_LC)			// M3
	shr.u rTMP = rNAT, rPOS					// I0, 3 cycles
	;;

	st8 [r2] = rUNAT, (SC_FPSR - SC_UNAT)			// M2
	st8 [r3] = rBSP						// M3
	add r8 = FR(3), in0
	;;

	st8 [r2] = rFPSR, (SC_RNAT - SC_FPSR)			// M2
	stf.spill [r8] = f3, (FR(4) - FR(3))			// M3
	add r9 = FR(5), in0
	;;

	stf.spill [r8] = f4, (FR(17) - FR(4))			// M2
	stf.spill [r9] = f5, (FR(19) - FR(5))			// M3
	shl rNAT = rNAT, rCPOS					// I0, 3 cycles
	;;

	st8 [r2] = rRNAT, (SC_NAT - SC_RNAT)			// M2
	stf.spill [r8] = f17, (FR(18) - FR(17))			// M3
	nop.i 0
	;;

	stf.spill [r8] = f18, (FR(20) - FR(18))			// M2
	stf.spill [r9] = f19, (FR(21) - FR(19))			// M3
	nop.i 0
	;;

	stf.spill [r8] = f20, (FR(22) - FR(20))			// M2
	stf.spill [r9] = f21, (FR(23) - FR(21))			// M3
	or rNAT = rNAT, rTMP					// I0
	;;

	st8 [r2] = rNAT						// M2
	stf.spill [r8] = f22, (FR(25) - FR(22))			// M3
	;;
	stf.spill [r9] = f23, (FR(26) - FR(23))			// M2
	stf.spill [r8] = f25, (FR(27) - FR(25))			// M3
	;;
	stf.spill [r9] = f26, (FR(28) - FR(26))			// M2
	stf.spill [r8] = f27, (FR(29) - FR(27))			// M3
	;;
	mov.m ar.rsc = rRSC	// restore RSE mode		// M2
	stf.spill [r9] = f28, (FR(30) - FR(28))			// M3
	;;
	mov.m ar.unat = rUNAT	// restore caller's UNaT	// M2
	stf.spill [r8] = f29					// M3
	;;
	stf.spill [r9] = f30					// M2
	mov r8 = 0
	br.ret.sptk.many rp
	.endp _Uia64_getcontext
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
