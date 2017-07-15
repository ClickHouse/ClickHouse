/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>

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
#include <endian.h>

	.text

#if _MIPS_SIM == _ABIO32
# if __BYTE_ORDER == __BIG_ENDIAN
#  define OFFSET 4
# else
#  define OFFSET 0
# endif
# define SREG(X) \
 sw $X, (LINUX_UC_MCONTEXT_GREGS + 8 * X + OFFSET) ($4); \
 sra $1, $X, 31; \
 sw $1, (LINUX_UC_MCONTEXT_GREGS + 8 * X + 4 - OFFSET) ($4)
/* Yes, we save the return address to PC. */
# define SPC \
 sw $31, (LINUX_UC_MCONTEXT_PC + OFFSET) ($4); \
 sra $1, $31, 31; \
 sw $1, (LINUX_UC_MCONTEXT_PC + 4 - OFFSET) ($4)
#else
# define SREG(X) sd $X, (LINUX_UC_MCONTEXT_GREGS + 8 * X) ($4)
# define SPC sd $31, (LINUX_UC_MCONTEXT_PC) ($4)
#endif

	.global _Umips_getcontext
	.type	_Umips_getcontext, %function
	# This is a stub version of getcontext() for MIPS which only stores core
	# registers.
_Umips_getcontext:
	.set noat
	SREG (1)
	SREG (0)
	SREG (2)
	SREG (3)
	SREG (4)
	SREG (5)
	SREG (6)
	SREG (7)
	SREG (8)
	SREG (9)
	SREG (10)
	SREG (11)
	SREG (12)
	SREG (13)
	SREG (14)
	SREG (15)
	SREG (16)
	SREG (17)
	SREG (18)
	SREG (19)
	SREG (20)
	SREG (21)
	SREG (22)
	SREG (23)
	SREG (24)
	SREG (25)
	SREG (26)
	SREG (27)
	SREG (28)
	SREG (29)
	SREG (30)
	SREG (31)
	SPC
	li	$2, 0
	j $31

	.size	_Umips_getcontext, .-_Umips_getcontext
