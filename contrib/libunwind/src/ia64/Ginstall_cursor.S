/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2003 Hewlett-Packard Co
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

#ifdef UNW_LOCAL_ONLY
# include "Lcursor_i.h"
# define ia64_install_cursor	_ULia64_install_cursor
#else
# include "Gcursor_i.h"
# define ia64_install_cursor	_Uia64_install_cursor
#endif

#define SYS_sigreturn	1181

#ifndef UNW_REMOTE_ONLY

/*  ia64_install_cursor (const cursor *c, long pri_unat, long *extra,
			 long bspstore, long dirty_size, long *dirty_partition,
			 long dirty_rnat)

  Restores the machine-state represented by C and thereby resumes execution
  in that frame.  If the frame or one of its descendants was interrupted
  by a signal, all registers are restored (including the signal mask).
  Otherwise, only the preserved registers, the global-pointer (r1), and
  the exception-arguments (r15-r18) are restored.  */

#define pRet	p6
#define pSig	p7

	.align 32
	.hidden ia64_install_cursor
	.global ia64_install_cursor
	.proc ia64_install_cursor
ia64_install_cursor:
	alloc r3 = ar.pfs, 7, 0, 0, 0
	invala
	add r2 = FR_LOC_OFF, in0
	;;

	ld8 r16 = [r2], LOC_SIZE	// r16 = loc[IA64_REG_FR16]
	mov.m r10 = ar.rsc		// (ar.rsc: ~ 12 cycle latency)
	add r3 = FR_LOC_OFF + 16, in0
	;;

	ld8 r17 = [r2], 2*LOC_SIZE	// r17 = loc[IA64_REG_FR17]
	ld8 r18 = [r3], 2*LOC_SIZE	// r18 = loc[IA64_REG_FR18]
	and r16 = -4, r16
	;;

	ld8 r19 = [r2], 2*LOC_SIZE	// r19 = loc[IA64_REG_FR19]
	ld8 r20 = [r3], 2*LOC_SIZE	// r20 = loc[IA64_REG_FR20]
	and r17 = -4, r17
	;;

	ldf.fill f16 = [r16]		// f16 restored (don't touch no more)
	ldf.fill f17 = [r17]		// f17 restored (don't touch no more)
	and r18 = -4, r18

	ld8 r21 = [r2], 2*LOC_SIZE	// r21 = loc[IA64_REG_FR21]
	ld8 r22 = [r3], 2*LOC_SIZE	// r22 = loc[IA64_REG_FR22]
	and r19 = -4, r19
	;;

	ldf.fill f18 = [r18]		// f18 restored (don't touch no more)
	ldf.fill f19 = [r19]		// f19 restored (don't touch no more)
	and r20 = -4, r20

	ld8 r23 = [r2], 2*LOC_SIZE	// r23 = loc[IA64_REG_FR23]
	ld8 r24 = [r3], 2*LOC_SIZE	// r24 = loc[IA64_REG_FR24]
	and r21 = -4, r21
	;;

	ldf.fill f20 = [r20]		// f20 restored (don't touch no more)
	ldf.fill f21 = [r21]		// f21 restored (don't touch no more)
	and r22 = -4, r22

	ld8 r25 = [r2], 2*LOC_SIZE	// r25 = loc[IA64_REG_FR25]
	ld8 r26 = [r3], 2*LOC_SIZE	// r26 = loc[IA64_REG_FR26]
	and r23 = -4, r23
	;;

	ldf.fill f22 = [r22]		// f22 restored (don't touch no more)
	ldf.fill f23 = [r23]		// f23 restored (don't touch no more)
	and r24 = -4, r24

	ld8 r27 = [r2], 2*LOC_SIZE	// r27 = loc[IA64_REG_FR27]
	ld8 r28 = [r3], 2*LOC_SIZE	// r28 = loc[IA64_REG_FR28]
	and r25 = -4, r25
	;;

	ldf.fill f24 = [r24]		// f24 restored (don't touch no more)
	ldf.fill f25 = [r25]		// f25 restored (don't touch no more)
	and r26 = -4, r26

	ld8 r29 = [r2], 2*LOC_SIZE	// r29 = loc[IA64_REG_FR29]
	ld8 r30 = [r3], 2*LOC_SIZE	// r30 = loc[IA64_REG_FR30]
	and r27 = -4, r27
	;;

	ldf.fill f26 = [r26]		// f26 restored (don't touch no more)
	ldf.fill f27 = [r27]		// f27 restored (don't touch no more)
	and r28 = -4, r28

	ld8 r31 = [r2]			// r31 = loc[IA64_REG_FR31]
	mov.m ar.unat = in1
	and r29 = -4, r29
	;;

	ldf.fill f28 = [r28]		// f28 restored (don't touch no more)
	ldf.fill f29 = [r29]		// f29 restored (don't touch no more)
	and r30 = -4, r30

	ld8 r1 = [in2], 8		// gp restored (don't touch no more)
	add r8 = SIGCONTEXT_ADDR_OFF, in0
	and r31 = -4, r31
	;;

	ld8 r8 = [r8]			// r8 = sigcontext_addr
	and r11 = 0x1c, r10		// clear all but rsc.be and rsc.pl
	add r2 = PFS_LOC_OFF, in0

	ldf.fill f30 = [r30]		// f30 restored (don't touch no more)
	ldf.fill f31 = [r31]		// f31 restored (don't touch no more)
	add r3 = 8, in2
	;;

	ld8.fill r4 = [in2], 16		// r4 restored (don't touch no more)
	ld8.fill r5 = [r3], 16		// r5 restored (don't touch no more)
	cmp.eq pRet, pSig = r0, r8	// sigcontext_addr == NULL?
	;;
	ld8.fill r6 = [in2], 16		// r6 restored (don't touch no more)
	ld8.fill r7 = [r3]		// r7 restored (don't touch no more)
	add r3 = IP_OFF, in0
	;;

	ld8 r14 = [r2], (B1_LOC_OFF - PFS_LOC_OFF)	// r14 = pfs_loc
	ld8 r15 = [r3]			// r15 = ip
	add r3 = (B2_LOC_OFF - IP_OFF), r3
	;;

	ld8 r16 = [r2], (B3_LOC_OFF - B1_LOC_OFF)	// r16 = b1_loc
	ld8 r17= [r3], (B4_LOC_OFF - B2_LOC_OFF)	// r17 = b2_loc
	and r14 = -4, r14
	;;

	ld8 r18 = [r2], (B5_LOC_OFF - B3_LOC_OFF)	// r18 = b3_loc
	ld8 r19 = [r3], (F2_LOC_OFF - B4_LOC_OFF)	// r19 = b4_loc
	and r16 = -4, r16
	;;

	ld8 r20 = [r2], (F3_LOC_OFF - B5_LOC_OFF)	// r20 = b5_loc
	ld8 r21 = [r3], (F4_LOC_OFF - F2_LOC_OFF)	// r21 = f2_loc
	and r17 = -4, r17
	;;

	ld8 r16 = [r16]			// r16 = *b1_loc
	ld8 r17 = [r17]			// r17 = *b2_loc
	and r18 = -4, r18

	ld8 r22 = [r2], (F5_LOC_OFF - F3_LOC_OFF)	// r21 = f3_loc
	ld8 r23 = [r3], (UNAT_LOC_OFF - F4_LOC_OFF)	// r22 = f4_loc
	and r19 = -4, r19
	;;

	ld8 r18 = [r18]			// r18 = *b3_loc
	ld8 r19 = [r19]			// r19 = *b4_loc
	and r20 = -4, r20

	ld8 r24 = [r2], (LC_LOC_OFF - F5_LOC_OFF)	// r24 = f5_loc
	ld8 r25 = [r3], (FPSR_LOC_OFF - UNAT_LOC_OFF)	// r25 = unat_loc
	and r21 = -4, r21
	;;

	and r22 = -4, r22
	and r23 = -4, r23
	and r24 = -4, r24

	ld8 r20 = [r20]			// r20 = *b5_loc
	ldf.fill f2 = [r21]		// f2 restored (don't touch no more)
	mov b1 = r16			// b1 restored (don't touch no more)
	;;

	ldf.fill f3 = [r22]		// f3 restored (don't touch no more)
	ldf.fill f4 = [r23]		// f4 restored (don't touch no more)
	mov b2 = r17			// b2 restored (don't touch no more)

	ld8 r26 = [r2], (RNAT_LOC_OFF - LC_LOC_OFF)	// r26 = lc_loc
	ld8 r27 = [r3]			// r27 = fpsr_loc
	and r25 = -4, r25

	add r3 = (PSP_OFF - FPSR_LOC_OFF), r3
	nop 0
	nop 0
	;;

	ldf.fill f5 = [r24]		// f5 restored (don't touch no more)
(pRet)	ld8 r25 = [r25]			// r25 = *unat_loc
	mov b3 = r18			// b3 restored (don't touch no more)

	ld8 r28 = [r2], (BSP_OFF - RNAT_LOC_OFF)	// r28 = rnat_loc
	ld8 r29 = [r3], (PR_OFF - PSP_OFF)		// r29 = sp
	mov b4 = r19			// b4 restored (don't touch no more)

	and r26 = -4, r26
	and r27 = -4, r27
	mov b5 = r20			// b5 restored (don't touch no more)
	;;

	ld8 r26 = [r26]			// r26 = *lc_loc
	ld8 r27 = [r27]			// r27 = *fpsr_loc
	and r28 = -4, r28

	mov r30 = in3			// make backup-copy of new bsp
	ld8 r31 = [r3]			// r31 = pr
	mov rp = r15
	;;

	ld8 r28 = [r28]			// r28 = rnat
	mov.m ar.rsc = r11		// put RSE into enforced lazy mode
	mov.i ar.lc = r26		// lc restored (don't touch no more)
	;;

	loadrs				// drop dirty partition
	mov r9 = in2			// make backup-copy of &extra[r16]
	cmp.eq p8, p0 = in4, r0		// dirty-size == 0?
(p8)	br.cond.dpnt.many .skip_load_dirty

	mov r2 = in4			// make backup-copy of dirty_size
	mov r15 = in5			// make backup-copy of dirty_partition
	mov r16 = in6			// make backup-copy of dirty_rnat
	;;

	alloc r3 = ar.pfs, 0, 0, 0, 0	// drop register frame
	dep r11 = r2, r11, 16, 16
	;;
	mov.m ar.bspstore = r15
	;;
	mov.m ar.rnat = r16
	mov.m ar.rsc = r11		// 14 cycles latency to loadrs
	;;
	loadrs				// loadup new dirty partition
	;;

.skip_load_dirty:
	mov.m ar.bspstore = r30		// restore register backing-store
	add r3 = 8, r9			// r3 = &extra[r16]
	;;

(pRet)	mov.m ar.fpsr = r27		// fpsr restored (don't touch no more)
	mov.m ar.rnat = r28
(pSig)	br.cond.dpnt.many .next

/****** Return via br.ret: */

	ld8 r14 = [r14]			// r14 = *pfs_loc
	ld8 r15 = [r9], 16		// r15 restored (don't touch no more)
	mov pr = r31, -1		// pr restored (don't touch no more)
	;;

	ld8 r16 = [r3], 16		// r16 restored (don't touch no more)
	ld8 r17 = [r9]			// r17 restored (don't touch no more)
	nop.i 0
	;;

	ld8 r18 = [r3]			// r18 restored (don't touch no more)
	mov.m ar.rsc = r10		// restore original ar.rsc
	mov sp = r29

	mov.m ar.unat = r25		// unat restored (don't touch no more)
	mov.i ar.pfs = r14
	br.ret.sptk.many rp
	;;

/****** Return via sigreturn(): */

.next:	mov.m ar.rsc = r10		// restore original ar.rsc
	add r2 = (SC_FR + 6*16), r8
	add r3 = (SC_FR + 7*16), r8
	;;

	ldf.fill f6 = [r2], 32
	ldf.fill f7 = [r3], 32
	nop 0
	;;

	ldf.fill f8 = [r2], 32
	ldf.fill f9 = [r3], 32
	nop 0
	;;

	ldf.fill f10 = [r2], 32
	ldf.fill f11 = [r3], 32
	nop 0
	;;

	ldf.fill f12 = [r2], 32
	ldf.fill f13 = [r3], 32
	nop 0
	;;

	ldf.fill f14 = [r2], 32
	ldf.fill f15 = [r3], 32
	mov sp = r29
	;;

#if NEW_SYSCALL
	add r2 = 8, tp;;
	ld8 r2 = [r2]
	mov r15 = SYS_sigreturn
	mov b7 = r2
	br.call.sptk.many b6 = b7
	;;
#else
	mov r15 = SYS_sigreturn
	break 0x100000
#endif
	break 0				// bug out if sigreturn() returns

	.endp ia64_install_cursor

#endif /* !UNW_REMOTE_ONLY */
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
