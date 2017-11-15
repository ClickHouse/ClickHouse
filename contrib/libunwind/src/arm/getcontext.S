/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery

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

#include "offsets.h"

	.text
	.arm

	.global _Uarm_getcontext
	.type	_Uarm_getcontext, %function
	@ This is a stub version of getcontext() for ARM which only stores core
	@ registers.  It must be called in a special way, not as a regular
	@ function -- see also the libunwind-arm.h:unw_tdep_getcontext macro.
_Uarm_getcontext:
	stmfd sp!, {r0, r1}
	@ store r0
#if defined(__linux__)
	str r0, [r0, #LINUX_UC_MCONTEXT_OFF + LINUX_SC_R0_OFF]
	add r0, r0, #LINUX_UC_MCONTEXT_OFF + LINUX_SC_R0_OFF
#elif defined(__FreeBSD__)
	str r0, [r0, #FREEBSD_UC_MCONTEXT_OFF + FREEBSD_MC_R0_OFF]
	add r0, r0, #FREEBSD_UC_MCONTEXT_OFF + FREEBSD_MC_R0_OFF
#else
#error Fix me
#endif
	@ store r1 to r12
	stmib r0, {r1-r12}
	@ reconstruct r13 at call site, then store
	add r1, sp, #12
	str r1, [r0, #13 * 4]
	@ retrieve r14 from call site, then store
	ldr r1, [sp, #8]
	str r1, [r0, #14 * 4]
	@ point lr to instruction after call site's stack adjustment
	add r1, lr, #4
	str r1, [r0, #15 * 4]
	ldmfd sp!, {r0, r1}
	bx lr
#if defined(__linux__) || defined(__FreeBSD__)
 /* We do not need executable stack.  */
 .section  .note.GNU-stack,"",%progbits
#endif
