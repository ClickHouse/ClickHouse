/*
 * x86 feature check
 *
 * Copyright (C) 2013 Intel Corporation. All rights reserved.
 * Author:
 *  Jim Kukunas
 *
 * For conditions of distribution and use, see copyright notice in zlib.h
 */

#include "x86.h"

ZLIB_INTERNAL int x86_cpu_has_sse2;
ZLIB_INTERNAL int x86_cpu_has_sse42;
ZLIB_INTERNAL int x86_cpu_has_pclmulqdq;

#ifdef _MSC_VER
#include <intrin.h>
#else
// Newer versions of GCC and clang come with cpuid.h
#include <cpuid.h>
#endif

static void cpuid(int info, unsigned* eax, unsigned* ebx, unsigned* ecx, unsigned* edx) {
#ifdef _MSC_VER
	unsigned int registers[4];
	__cpuid(registers, info);

	*eax = registers[0];
	*ebx = registers[1];
	*ecx = registers[2];
	*edx = registers[3];
#else
	unsigned int _eax;
	unsigned int _ebx;
	unsigned int _ecx;
	unsigned int _edx;
	__cpuid(info, _eax, _ebx, _ecx, _edx);
	*eax = _eax;
	*ebx = _ebx;
	*ecx = _ecx;
	*edx = _edx;
#endif
}

void ZLIB_INTERNAL x86_check_features(void) {
	unsigned eax, ebx, ecx, edx;
	cpuid(1 /*CPU_PROCINFO_AND_FEATUREBITS*/, &eax, &ebx, &ecx, &edx);

	x86_cpu_has_sse2 = edx & 0x4000000;
	x86_cpu_has_sse42 = ecx & 0x100000;
	x86_cpu_has_pclmulqdq = ecx & 0x2;
}
