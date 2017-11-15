/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
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

#include "jmpbuf.h"

#define SIG_BLOCK 0

	.align 32

	.global __sigsetjmp
	.global sigprocmask

	.proc __sigsetjmp

__sigsetjmp:
	.prologue
	.save ar.pfs, r35
	alloc loc1 = ar.pfs, 2, 3, 3, 0
	add out2 = JB_MASK*8, in0
	.save rp, loc0
	mov loc0 = rp
	mov out0 = SIG_BLOCK
	.body
	;;
	cmp.ne p6, p0 = in1, r0
	mov out1 = r0
	mov loc2 = ar.bsp
(p6)	br.call.sptk.many rp = sigprocmask // sigjmp_buf[JB_MASK] = sigmask
	;;

	add r16 = JB_MASK_SAVED*8, in0
	st8 [in0] = sp, (JB_RP-JB_SP)*8	// sigjmp_buf[JB_SP] = sp
	mov r8 = 0
	;;
	st8 [in0] = loc0, (JB_BSP-JB_RP)*8	// sigjmp_buf[JB_RP] = rp
	st8 [r16] = in1			// sigjmp_buf[JB_MASK_SAVED] = savemask
	mov rp = loc0
	;;
	st8 [in0] = loc2		// sigjmp_buf[JB_BSP] = bsp
	mov.i ar.pfs = loc1
	br.ret.sptk.many rp

	.endp __sigsetjmp
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
