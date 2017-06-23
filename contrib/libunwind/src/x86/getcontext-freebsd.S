/* libunwind - a platform-independent unwind library
   Copyright (C) 2010 Konstantin Belousov <kib@freebsd.org>

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

	.global _Ux86_getcontext
	.type _Ux86_getcontext, @function
_Ux86_getcontext:
	.cfi_startproc
	pushl	%eax
	.cfi_adjust_cfa_offset 4
	mov	8(%esp),%eax  /* ucontext_t* */
	popl	FREEBSD_UC_MCONTEXT_EAX_OFF(%eax)
	.cfi_adjust_cfa_offset 4
	movl	%ebx, FREEBSD_UC_MCONTEXT_EBX_OFF(%eax)
	movl	%ecx, FREEBSD_UC_MCONTEXT_ECX_OFF(%eax)
	movl	%edx, FREEBSD_UC_MCONTEXT_EDX_OFF(%eax)
	movl	%edi, FREEBSD_UC_MCONTEXT_EDI_OFF(%eax)
	movl	%esi, FREEBSD_UC_MCONTEXT_ESI_OFF(%eax)
	movl	%ebp, FREEBSD_UC_MCONTEXT_EBP_OFF(%eax)

	movl	(%esp), %ecx
	movl	%ecx, FREEBSD_UC_MCONTEXT_EIP_OFF(%eax)

	leal	4(%esp), %ecx		/* Exclude the return address.  */
	movl	%ecx, FREEBSD_UC_MCONTEXT_ESP_OFF(%eax)

	xorl	%ecx, %ecx
	movw	%fs, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_FS_OFF(%eax)
	movw	%gs, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_GS_OFF(%eax)
	movw	%ds, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_DS_OFF(%eax)
	movw	%es, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_ES_OFF(%eax)
	movw	%ss, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_SS_OFF(%eax)
	movw	%cs, %cx
	movl	%ecx, FREEBSD_UC_MCONTEXT_CS_OFF(%eax)

	pushfl
	.cfi_adjust_cfa_offset 4
	popl	FREEBSD_UC_MCONTEXT_EFLAGS_OFF(%eax)
	.cfi_adjust_cfa_offset -4

	movl	$0, FREEBSD_UC_MCONTEXT_TRAPNO_OFF(%eax)

	movl	$FREEBSD_UC_MCONTEXT_FPOWNED_FPU,\
		FREEBSD_UC_MCONTEXT_OWNEDFP_OFF(%eax)
	movl	$FREEBSD_UC_MCONTEXT_FPFMT_XMM,\
		FREEBSD_UC_MCONTEXT_FPFORMAT_OFF(%eax)

       /*
	* Require CPU with fxsave implemented, and enabled by OS.
	*
	* If passed ucontext is not aligned to 16-byte boundary,
	* save fpu context into temporary aligned location on stack
	* and then copy.
	*/
	leal	FREEBSD_UC_MCONTEXT_FPSTATE_OFF(%eax), %edx
	testl	$0xf, %edx
	jne	2f
	fxsave	(%edx)	/* fast path, passed ucontext save area was aligned */
1:	movl	$FREEBSD_UC_MCONTEXT_MC_LEN_VAL,\
		FREEBSD_UC_MCONTEXT_MC_LEN_OFF(%eax)

	xorl	%eax, %eax
	ret

2:	movl	%edx, %edi	/* not aligned, do the dance */
	subl	$512 + 16, %esp	/* save area and 16 bytes for alignment */
	.cfi_adjust_cfa_offset 512 + 16
	movl	%esp, %edx
	orl	$0xf, %edx	/* align *%edx to 16-byte up */
	incl	%edx
	fxsave	(%edx)
	movl	%edx, %esi	/* copy to the final destination */
	movl	$512/4,%ecx
	rep; movsl
	addl	$512 + 16, %esp	/* restore the stack */
	.cfi_adjust_cfa_offset -512 - 16
	movl	FREEBSD_UC_MCONTEXT_ESI_OFF(%eax), %esi
	movl	FREEBSD_UC_MCONTEXT_EDI_OFF(%eax), %edi
	jmp	1b

	.cfi_endproc
	.size	_Ux86_getcontext, . - _Ux86_getcontext

	/* We do not need executable stack.  */
	.section        .note.GNU-stack,"",@progbits
