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

/* The setcontext() in glibc is a no-op (as of 4 Dec 2004), so we have
   to implement something useful on our own here.  */

#define FILL(n) ldw (LINUX_UC_MCONTEXT_OFF+LINUX_SC_GR_OFF+4*(n))(%r26),%r##n

#include "offsets.h"

	.align 4
	.global _Uhppa_setcontext
	.protected _Uhppa_setcontext
	.proc
	.callinfo
_Uhppa_setcontext:
	FILL (2)		/* return-pointer */
	FILL (3)		/* frame pointer */
	FILL (4)		/* 2nd-ary frame pointer */
	FILL (5)		/* preserved register */
	FILL (6)		/* preserved register */
	FILL (7)		/* preserved register */
	FILL (8)		/* preserved register */
	FILL (9)		/* preserved register */
	FILL (10)		/* preserved register */
	FILL (11)		/* preserved register */
	FILL (12)		/* preserved register */
	FILL (13)		/* preserved register */
	FILL (14)		/* preserved register */
	FILL (15)		/* preserved register */
	FILL (16)		/* preserved register */
	FILL (17)		/* preserved register */
	FILL (18)		/* preserved register */
	FILL (19)		/* linkage-table register */
	FILL (27)		/* global-data pointer */
	FILL (30)		/* stack pointer */

	ldo (LINUX_UC_MCONTEXT_OFF+LINUX_SC_FR_OFF)(%r26), %r29
	fldds,ma 8(%r29), %fr12
	fldds,ma 8(%r29), %fr13
	fldds,ma 8(%r29), %fr14
	fldds,ma 8(%r29), %fr15
	fldds,ma 8(%r29), %fr16
	fldds,ma 8(%r29), %fr17
	fldds,ma 8(%r29), %fr18
	fldds,ma 8(%r29), %fr19
	fldds,ma 8(%r29), %fr20
	fldds    8(%r29), %fr21

	bv,n	%r0(%rp)
	.procend
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
