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
#ifndef __ASM_BITS_H__
#define __ASM_BITS_H__
#include "libcpuid.h"

/* Determine Compiler: */
#if defined(_MSC_VER)
#	define COMPILER_MICROSOFT
#elif defined(__GNUC__)
#	define COMPILER_GCC
#endif

/* Determine Platform */
#if defined(__x86_64__) || defined(_M_AMD64)
#	define PLATFORM_X64
#elif defined(__i386__) || defined(_M_IX86)
#	define PLATFORM_X86
#endif

/* Under Windows/AMD64 with MSVC, inline assembly isn't supported */
#if (defined(COMPILER_GCC) && defined(PLATFORM_X64)) || defined(PLATFORM_X86)
#	define INLINE_ASM_SUPPORTED
#endif

int cpuid_exists_by_eflags(void);
void exec_cpuid(uint32_t *regs);
void busy_sse_loop(int cycles);

#endif /* __ASM_BITS_H__ */
