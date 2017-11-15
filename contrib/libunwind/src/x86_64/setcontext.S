/* libunwind - a platform-independent unwind library
   Copyright (C) 2007 Google, Inc
	Contributed by Arun Sharma <arun.sharma@google.com>
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

#include "ucontext_i.h"

/*  int _Ux86_64_setcontext (const ucontext_t *ucp)

  Restores the machine context provided.
  Unlike the libc implementation, doesn't clobber %rax

*/
	.global _Ux86_64_setcontext
	.type _Ux86_64_setcontext, @function

_Ux86_64_setcontext:

#if defined __linux__
        /* restore fp state */
	mov    UC_MCONTEXT_FPREGS_PTR(%rdi),%r8
	fldenv (%r8)
	ldmxcsr FPREGS_OFFSET_MXCSR(%r8)
#elif defined __FreeBSD__
	/* restore fp state */
	cmpq $UC_MCONTEXT_FPOWNED_FPU,UC_MCONTEXT_OWNEDFP(%rdi)
	jne 1f
	cmpq $UC_MCONTEXT_FPFMT_XMM,UC_MCONTEXT_FPFORMAT(%rdi)
	jne 1f
	fxrstor UC_MCONTEXT_FPSTATE(%rdi)
1:
#else
#error Port me
#endif

	/* restore the rest of the state */
	mov    UC_MCONTEXT_GREGS_R8(%rdi),%r8
	mov    UC_MCONTEXT_GREGS_R9(%rdi),%r9
	mov    UC_MCONTEXT_GREGS_RBX(%rdi),%rbx
	mov    UC_MCONTEXT_GREGS_RBP(%rdi),%rbp
	mov    UC_MCONTEXT_GREGS_R12(%rdi),%r12
	mov    UC_MCONTEXT_GREGS_R13(%rdi),%r13
	mov    UC_MCONTEXT_GREGS_R14(%rdi),%r14
	mov    UC_MCONTEXT_GREGS_R15(%rdi),%r15
	mov    UC_MCONTEXT_GREGS_RSI(%rdi),%rsi
	mov    UC_MCONTEXT_GREGS_RDX(%rdi),%rdx
	mov    UC_MCONTEXT_GREGS_RAX(%rdi),%rax
	mov    UC_MCONTEXT_GREGS_RCX(%rdi),%rcx
	mov    UC_MCONTEXT_GREGS_RSP(%rdi),%rsp

        /* push the return address on the stack */
	mov    UC_MCONTEXT_GREGS_RIP(%rdi),%rcx
	push   %rcx

	mov    UC_MCONTEXT_GREGS_RCX(%rdi),%rcx
	mov    UC_MCONTEXT_GREGS_RDI(%rdi),%rdi
	retq

	.size _Ux86_64_setcontext, . - _Ux86_64_setcontext

      /* We do not need executable stack.  */
      .section        .note.GNU-stack,"",@progbits
