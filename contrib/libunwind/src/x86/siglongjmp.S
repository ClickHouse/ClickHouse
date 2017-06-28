/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
	Contributed by David Mosberger-Tang <davidm@hpl.hp.com>
   Copyright (C) 2011 Konstantin Belousov <kib@FreeBSD.org>

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

	.globl _UI_siglongjmp_cont

#if defined(__linux__)
#define SIG_SETMASK 2
#elif defined(__FreeBSD__)
#define SIG_SETMASK 3
#endif

	/* Stack layout at this point:

		+------------+ <- original $esp (at time of setjmp() call)
		| sigmask[1] |
		+------------+
		| sigmask[0] |
		+------------+
	 */

	.type _UI_siglongjmp_cont, @function
_UI_siglongjmp_cont:
	.cfi_startproc
#ifdef __linux__
	.cfi_register 8, 0	/* IP saved in EAX */
	.cfi_def_cfa_offset 8
	mov %esp, %ecx		/* pass address of signal mask in 3rd sc arg */
	push %eax		/* save target IP */
	.cfi_adjust_cfa_offset 4
	.cfi_offset 8, -12
	push %edx		/* save return value */
	.cfi_adjust_cfa_offset 4
	push %ebx		/* save %ebx (preserved) */
	.cfi_adjust_cfa_offset 4
	.cfi_offset 3, -20
	mov $SIG_SETMASK, %ebx	/* 1st syscall arg (how) */
	xor %edx, %edx		/* pass NULL as 3rd syscall arg (old maskp) */
	int $0x80
	pop %ebx		/* restore %ebx */
	.cfi_adjust_cfa_offset -4
	.cfi_restore 3
	pop %eax		/* fetch return value */
	.cfi_adjust_cfa_offset -4
	pop %edx		/* pop target IP */
	.cfi_adjust_cfa_offset -4
	.cfi_register 8, 2	/* saved IP is now n EDX */
	lea 8(%esp), %esp	/* pop sigmask */
	.cfi_adjust_cfa_offset -4
	jmp *%edx
#elif defined(__FreeBSD__)
	pushl	%eax
	pushl	%edx
	pushl	$0
	pushl	%ecx
	pushl	$SIG_SETMASK
	movl	$340,%eax
	pushl	%eax
	int	$0x80
	addl	$16,%esp
	popl	%eax
	popl	%edx
	jmp	*%edx
#else
#error Port me
#endif
	.cfi_endproc
	.size _UI_siglongjmp_cont, .-_UI_siglongjmp_cont
	/* We do not need executable stack.  */
	.section	.note.GNU-stack,"",@progbits
