/*
 * Copyright 2008  Veselin Georgiev,
 * anrieffNOSPAM @ mgail_DOT.com (convert to gmail)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "libcpuid.h"
#include "asm-bits.h"

int cpuid_exists_by_eflags(void)
{
#if defined(PLATFORM_X64)
	return 1; /* CPUID is always present on the x86_64 */
#elif defined(PLATFORM_X86)
#  if defined(COMPILER_GCC)
	int result;
	__asm __volatile(
		"	pushfl\n"
		"	pop	%%eax\n"
		"	mov	%%eax,	%%ecx\n"
		"	xor	$0x200000,	%%eax\n"
		"	push	%%eax\n"
		"	popfl\n"
		"	pushfl\n"
		"	pop	%%eax\n"
		"	xor	%%ecx,	%%eax\n"
		"	mov	%%eax,	%0\n"
		"	push	%%ecx\n"
		"	popfl\n"
		: "=m"(result)
		: :"eax", "ecx", "memory");
	return (result != 0);
#  elif defined(COMPILER_MICROSOFT)
	int result;
	__asm {
		pushfd
		pop	eax
		mov	ecx,	eax
		xor	eax,	0x200000
		push	eax
		popfd
		pushfd
		pop	eax
		xor	eax,	ecx
		mov	result,	eax
		push	ecx
		popfd
	};
	return (result != 0);
#  else
	return 0;
#  endif /* COMPILER_MICROSOFT */
#else
	return 0;
#endif /* PLATFORM_X86 */
}

#ifdef INLINE_ASM_SUPPORTED
/* 
 * with MSVC/AMD64, the exec_cpuid() and cpu_rdtsc() functions
 * are implemented in separate .asm files. Otherwise, use inline assembly
 */
void exec_cpuid(uint32_t *regs)
{
#ifdef COMPILER_GCC
#	ifdef PLATFORM_X64
	__asm __volatile(
		"	mov	%0,	%%rdi\n"

		"	push	%%rbx\n"
		"	push	%%rcx\n"
		"	push	%%rdx\n"
		
		"	mov	(%%rdi),	%%eax\n"
		"	mov	4(%%rdi),	%%ebx\n"
		"	mov	8(%%rdi),	%%ecx\n"
		"	mov	12(%%rdi),	%%edx\n"
		
		"	cpuid\n"
		
		"	movl	%%eax,	(%%rdi)\n"
		"	movl	%%ebx,	4(%%rdi)\n"
		"	movl	%%ecx,	8(%%rdi)\n"
		"	movl	%%edx,	12(%%rdi)\n"
		"	pop	%%rdx\n"
		"	pop	%%rcx\n"
		"	pop	%%rbx\n"
		:
		:"m"(regs)
		:"memory", "eax", "rdi"
	);
#	else
	__asm __volatile(
		"	mov	%0,	%%edi\n"

		"	push	%%ebx\n"
		"	push	%%ecx\n"
		"	push	%%edx\n"
		
		"	mov	(%%edi),	%%eax\n"
		"	mov	4(%%edi),	%%ebx\n"
		"	mov	8(%%edi),	%%ecx\n"
		"	mov	12(%%edi),	%%edx\n"
		
		"	cpuid\n"
		
		"	mov	%%eax,	(%%edi)\n"
		"	mov	%%ebx,	4(%%edi)\n"
		"	mov	%%ecx,	8(%%edi)\n"
		"	mov	%%edx,	12(%%edi)\n"
		"	pop	%%edx\n"
		"	pop	%%ecx\n"
		"	pop	%%ebx\n"
		:
		:"m"(regs)
		:"memory", "eax", "edi"
	);
#	endif /* COMPILER_GCC */
#else
#  ifdef COMPILER_MICROSOFT
	__asm {
		push	ebx
		push	ecx
		push	edx
		push	edi
		mov	edi,	regs
		
		mov	eax,	[edi]
		mov	ebx,	[edi+4]
		mov	ecx,	[edi+8]
		mov	edx,	[edi+12]
		
		cpuid
		
		mov	[edi],		eax
		mov	[edi+4],	ebx
		mov	[edi+8],	ecx
		mov	[edi+12],	edx
		
		pop	edi
		pop	edx
		pop	ecx
		pop	ebx
	}
#  else
#    error "Unsupported compiler"
#  endif /* COMPILER_MICROSOFT */
#endif
}
#endif /* INLINE_ASSEMBLY_SUPPORTED */

#ifdef INLINE_ASM_SUPPORTED
void cpu_rdtsc(uint64_t* result)
{
	uint32_t low_part, hi_part;
#ifdef COMPILER_GCC
	__asm __volatile (
		"	rdtsc\n"
		"	mov	%%eax,	%0\n"
		"	mov	%%edx,	%1\n"
		:"=m"(low_part), "=m"(hi_part)::"memory", "eax", "edx"
	);
#else
#  ifdef COMPILER_MICROSOFT
	__asm {
		rdtsc
		mov	low_part,	eax
		mov	hi_part,	edx
	};
#  else
#    error "Unsupported compiler"
#  endif /* COMPILER_MICROSOFT */
#endif /* COMPILER_GCC */
	*result = (uint64_t)low_part + (((uint64_t) hi_part) << 32);
}
#endif /* INLINE_ASM_SUPPORTED */

#ifdef INLINE_ASM_SUPPORTED
void busy_sse_loop(int cycles)
{
#ifdef COMPILER_GCC
#ifndef __APPLE__
#	define XALIGN ".balign 16\n"
#else
#	define XALIGN ".align 4\n"
#endif
	__asm __volatile (
		"	xorps	%%xmm0,	%%xmm0\n"
		"	xorps	%%xmm1,	%%xmm1\n"
		"	xorps	%%xmm2,	%%xmm2\n"
		"	xorps	%%xmm3,	%%xmm3\n"
		"	xorps	%%xmm4,	%%xmm4\n"
		"	xorps	%%xmm5,	%%xmm5\n"
		"	xorps	%%xmm6,	%%xmm6\n"
		"	xorps	%%xmm7,	%%xmm7\n"
		XALIGN
		/* ".bsLoop:\n" */
		"1:\n"
		// 0:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 1:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 2:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 3:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 4:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 5:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 6:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 7:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 8:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		// 9:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//10:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//11:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//12:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//13:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//14:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//15:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//16:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//17:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//18:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//19:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//20:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//21:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//22:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//23:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//24:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//25:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//26:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//27:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//28:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//29:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//30:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		//31:
		"	addps	%%xmm1, %%xmm0\n"
		"	addps	%%xmm2, %%xmm1\n"
		"	addps	%%xmm3, %%xmm2\n"
		"	addps	%%xmm4, %%xmm3\n"
		"	addps	%%xmm5, %%xmm4\n"
		"	addps	%%xmm6, %%xmm5\n"
		"	addps	%%xmm7, %%xmm6\n"
		"	addps	%%xmm0, %%xmm7\n"
		
		"	dec	%%eax\n"
		/* "jnz	.bsLoop\n" */
		"	jnz	1b\n"
		::"a"(cycles)
	);
#else
#  ifdef COMPILER_MICROSOFT
	__asm {
		mov	eax,	cycles
		xorps	xmm0,	xmm0
		xorps	xmm1,	xmm1
		xorps	xmm2,	xmm2
		xorps	xmm3,	xmm3
		xorps	xmm4,	xmm4
		xorps	xmm5,	xmm5
		xorps	xmm6,	xmm6
		xorps	xmm7,	xmm7
		//--
		align 16
bsLoop:
		// 0:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 1:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 2:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 3:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 4:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 5:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 6:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 7:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 8:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 9:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 10:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 11:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 12:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 13:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 14:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 15:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 16:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 17:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 18:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 19:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 20:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 21:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 22:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 23:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 24:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 25:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 26:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 27:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 28:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 29:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 30:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		// 31:
		addps	xmm0,	xmm1
		addps	xmm1,	xmm2
		addps	xmm2,	xmm3
		addps	xmm3,	xmm4
		addps	xmm4,	xmm5
		addps	xmm5,	xmm6
		addps	xmm6,	xmm7
		addps	xmm7,	xmm0
		//----------------------
		dec		eax
		jnz		bsLoop
	}
#  else
#    error "Unsupported compiler"
#  endif /* COMPILER_MICROSOFT */
#endif /* COMPILER_GCC */
}
#endif /* INLINE_ASSEMBLY_SUPPORTED */
