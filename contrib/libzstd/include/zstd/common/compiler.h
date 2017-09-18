/*
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

#ifndef ZSTD_COMPILER_H
#define ZSTD_COMPILER_H

/*-*******************************************************
*  Compiler specifics
*********************************************************/
/* force inlining */
#if defined (__GNUC__) || defined(__cplusplus) || defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   /* C99 */
#  define INLINE_KEYWORD inline
#else
#  define INLINE_KEYWORD
#endif

#if defined(__GNUC__)
#  define FORCE_INLINE_ATTR __attribute__((always_inline))
#elif defined(_MSC_VER)
#  define FORCE_INLINE_ATTR __forceinline
#else
#  define FORCE_INLINE_ATTR
#endif

/**
 * FORCE_INLINE_TEMPLATE is used to define C "templates", which take constant
 * parameters. They must be inlined for the compiler to elimininate the constant
 * branches.
 */
#define FORCE_INLINE_TEMPLATE static INLINE_KEYWORD FORCE_INLINE_ATTR
/**
 * HINT_INLINE is used to help the compiler generate better code. It is *not*
 * used for "templates", so it can be tweaked based on the compilers
 * performance.
 *
 * gcc-4.8 and gcc-4.9 have been shown to benefit from leaving off the
 * always_inline attribute.
 *
 * clang up to 5.0.0 (trunk) benefit tremendously from the always_inline
 * attribute.
 */
#if !defined(__clang__) && defined(__GNUC__) && __GNUC__ >= 4 && __GNUC_MINOR__ >= 8 && __GNUC__ < 5
#  define HINT_INLINE static INLINE_KEYWORD
#else
#  define HINT_INLINE static INLINE_KEYWORD FORCE_INLINE_ATTR
#endif

/* force no inlining */
#ifdef _MSC_VER
#  define FORCE_NOINLINE static __declspec(noinline)
#else
#  ifdef __GNUC__
#    define FORCE_NOINLINE static __attribute__((__noinline__))
#  else
#    define FORCE_NOINLINE static
#  endif
#endif

/* prefetch */
#if defined(_MSC_VER) && (defined(_M_X64) || defined(_M_I86))  /* _mm_prefetch() is not defined outside of x86/x64 */
#  include <mmintrin.h>   /* https://msdn.microsoft.com/fr-fr/library/84szxsww(v=vs.90).aspx */
#  define PREFETCH(ptr)   _mm_prefetch((const char*)ptr, _MM_HINT_T0)
#elif defined(__GNUC__)
#  define PREFETCH(ptr)   __builtin_prefetch(ptr, 0, 0)
#else
#  define PREFETCH(ptr)   /* disabled */
#endif

/* disable warnings */
#ifdef _MSC_VER    /* Visual Studio */
#  include <intrin.h>                    /* For Visual 2005 */
#  pragma warning(disable : 4100)        /* disable: C4100: unreferenced formal parameter */
#  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4204)        /* disable: C4204: non-constant aggregate initializer */
#  pragma warning(disable : 4214)        /* disable: C4214: non-int bitfields */
#  pragma warning(disable : 4324)        /* disable: C4324: padded structure */
#endif

#endif /* ZSTD_COMPILER_H */
