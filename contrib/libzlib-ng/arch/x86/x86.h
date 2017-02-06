 /* cpu.h -- check for CPU features
 * Copyright (C) 2013 Intel Corporation Jim Kukunas
 * For conditions of distribution and use, see copyright notice in zlib.h
 */

#ifndef CPU_H_
#define CPU_H_

#if defined(HAVE_INTERNAL)
#  define ZLIB_INTERNAL __attribute__((visibility ("internal")))
#elif defined(HAVE_HIDDEN)
# define ZLIB_INTERNAL __attribute__((visibility ("hidden")))
#else
# define ZLIB_INTERNAL
#endif

extern int x86_cpu_has_sse2;
extern int x86_cpu_has_sse42;
extern int x86_cpu_has_pclmulqdq;

void ZLIB_INTERNAL x86_check_features(void);

#endif /* CPU_H_ */
