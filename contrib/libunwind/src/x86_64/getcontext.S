/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 Google, Inc
	Contributed by Paul Pluzhnikov <ppluzhnikov@google.com>
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

/*  int _Ux86_64_getcontext (ucontext_t *ucp)

  Saves the machine context in UCP necessary for libunwind.  
  Unlike the libc implementation, we don't save the signal mask
  and hence avoid the cost of a system call per unwind.
  
*/

	.global _Ux86_64_getcontext
	.type _Ux86_64_getcontext, @function
_Ux86_64_getcontext:
	.cfi_startproc

	/* Callee saved: RBX, RBP, R12-R15  */
	movq %r12, UC_MCONTEXT_GREGS_R12(%rdi)
	movq %r13, UC_MCONTEXT_GREGS_R13(%rdi)
	movq %r14, UC_MCONTEXT_GREGS_R14(%rdi)
	movq %r15, UC_MCONTEXT_GREGS_R15(%rdi)
	movq %rbp, UC_MCONTEXT_GREGS_RBP(%rdi)
	movq %rbx, UC_MCONTEXT_GREGS_RBX(%rdi)

	/* Save argument registers (not strictly needed, but setcontext 
	   restores them, so don't restore garbage).  */
	movq %r8,  UC_MCONTEXT_GREGS_R8(%rdi)
	movq %r9,  UC_MCONTEXT_GREGS_R9(%rdi)
	movq %rdi, UC_MCONTEXT_GREGS_RDI(%rdi)
	movq %rsi, UC_MCONTEXT_GREGS_RSI(%rdi)
	movq %rdx, UC_MCONTEXT_GREGS_RDX(%rdi)
	movq %rax, UC_MCONTEXT_GREGS_RAX(%rdi)
	movq %rcx, UC_MCONTEXT_GREGS_RCX(%rdi)

#if defined __linux__
	/* Save fp state (not needed, except for setcontext not
	   restoring garbage).  */
	leaq UC_MCONTEXT_FPREGS_MEM(%rdi),%r8
	movq %r8, UC_MCONTEXT_FPREGS_PTR(%rdi)
	fnstenv (%r8)
	stmxcsr FPREGS_OFFSET_MXCSR(%r8)
#elif defined __FreeBSD__
	fxsave UC_MCONTEXT_FPSTATE(%rdi)
	movq $UC_MCONTEXT_FPOWNED_FPU,UC_MCONTEXT_OWNEDFP(%rdi)
	movq $UC_MCONTEXT_FPFMT_XMM,UC_MCONTEXT_FPFORMAT(%rdi)
	/* Save rflags and segment registers, so that sigreturn(2)
	does not complain. */
	pushfq
	.cfi_adjust_cfa_offset 8
	popq UC_MCONTEXT_RFLAGS(%rdi)
	.cfi_adjust_cfa_offset -8
	movl $0, UC_MCONTEXT_FLAGS(%rdi)
	movw %cs, UC_MCONTEXT_CS(%rdi)
	movw %ss, UC_MCONTEXT_SS(%rdi)
#if 0
	/* Setting the flags to 0 above disables restore of segment
	   registers from the context */
	movw %ds, UC_MCONTEXT_DS(%rdi)
	movw %es, UC_MCONTEXT_ES(%rdi)
	movw %fs, UC_MCONTEXT_FS(%rdi)
	movw %gs, UC_MCONTEXT_GS(%rdi)
#endif
	movq $UC_MCONTEXT_MC_LEN_VAL, UC_MCONTEXT_MC_LEN(%rdi)
#else
#error Port me
#endif

	leaq 8(%rsp), %rax /* exclude this call.  */
	movq %rax, UC_MCONTEXT_GREGS_RSP(%rdi)

	movq 0(%rsp), %rax
	movq %rax, UC_MCONTEXT_GREGS_RIP(%rdi)

	xorq	%rax, %rax
	retq
	.cfi_endproc
	.size _Ux86_64_getcontext, . - _Ux86_64_getcontext

/*  int _Ux86_64_getcontext_trace (ucontext_t *ucp)

  Saves limited machine context in UCP necessary for libunwind.
  Unlike _Ux86_64_getcontext, saves only the parts needed for
  fast trace. If fast trace fails, caller will have to get the
  full context.
*/

	.global _Ux86_64_getcontext_trace
	.hidden _Ux86_64_getcontext_trace
	.type _Ux86_64_getcontext_trace, @function
_Ux86_64_getcontext_trace:
	.cfi_startproc

	/* Save only RBP, RBX, RSP, RIP - exclude this call. */
	movq %rbp, UC_MCONTEXT_GREGS_RBP(%rdi)
	movq %rbx, UC_MCONTEXT_GREGS_RBX(%rdi)

	leaq 8(%rsp), %rax
	movq %rax, UC_MCONTEXT_GREGS_RSP(%rdi)

	movq 0(%rsp), %rax
	movq %rax, UC_MCONTEXT_GREGS_RIP(%rdi)

	xorq	%rax, %rax
	retq
	.cfi_endproc
	.size _Ux86_64_getcontext_trace, . - _Ux86_64_getcontext_trace

      /* We do not need executable stack.  */
      .section        .note.GNU-stack,"",@progbits
