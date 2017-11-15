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

#define SPILL(n) stw %r##n, (LINUX_UC_MCONTEXT_OFF+LINUX_SC_GR_OFF+4*(n))(%r26)

#include "offsets.h"

	.align 4
	.protected _Uhppa_getcontext
	.global _Uhppa_getcontext
	.proc
	.callinfo
_Uhppa_getcontext:
	SPILL (2)		/* return-pointer */
	SPILL (3)		/* frame pointer */
	SPILL (4)		/* 2nd-ary frame pointer */
	SPILL (5)		/* preserved register */
	SPILL (6)		/* preserved register */
	SPILL (7)		/* preserved register */
	SPILL (8)		/* preserved register */
	SPILL (9)		/* preserved register */
	SPILL (10)		/* preserved register */
	SPILL (11)		/* preserved register */
	SPILL (12)		/* preserved register */
	SPILL (13)		/* preserved register */
	SPILL (14)		/* preserved register */
	SPILL (15)		/* preserved register */
	SPILL (16)		/* preserved register */
	SPILL (17)		/* preserved register */
	SPILL (18)		/* preserved register */
	SPILL (19)		/* linkage-table register */
	SPILL (27)		/* global-data pointer */
	SPILL (30)		/* stack pointer */

	ldo (LINUX_UC_MCONTEXT_OFF+LINUX_SC_FR_OFF)(%r26), %r29
	fstds,ma %fr12, 8(%r29)
	fstds,ma %fr13, 8(%r29)
	fstds,ma %fr14, 8(%r29)
	fstds,ma %fr15, 8(%r29)
	fstds,ma %fr16, 8(%r29)
	fstds,ma %fr17, 8(%r29)
	fstds,ma %fr18, 8(%r29)
	fstds,ma %fr19, 8(%r29)
	fstds,ma %fr20, 8(%r29)
	fstds    %fr21, 8(%r29)

	bv,n	%r0(%rp)
	.procend
#ifdef __linux__
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
#endif
