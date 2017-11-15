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

#define SIG_SETMASK	2

	.global _UI_siglongjmp_cont
	.global sigprocmask

	.align 32
	.proc siglongjmp_continuation
siglongjmp_continuation:
_UI_siglongjmp_cont:		// non-function label for siglongjmp.c
	.prologue
	.save rp, r15
	.body
	nop 0
	nop 0
	br.call.sptk.many b6 = .next
	;;
	.prologue
	.save ar.pfs, r33
.next:	alloc loc1 = ar.pfs, 0, 3, 3, 0
	/*
	 * Note: we can use the scratch stack are because the caller
	 * of sigsetjmp() by definition is not a leaf-procedure.
	 */
	st8 [sp] = r17			// store signal mask
	.save rp, loc0
	mov loc0 = r15			// final continuation point
	;;
	.body
	mov loc2 = r16			// value to return in r8

	mov out0 = SIG_SETMASK
	mov out1 = sp
	mov out2 = r0
	br.call.sptk.many rp = sigprocmask
	;;
	mov rp = loc0
	mov ar.pfs = loc1
	mov r8 = loc2
	br.ret.sptk.many rp
	.endp siglongjmp_continuation
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
