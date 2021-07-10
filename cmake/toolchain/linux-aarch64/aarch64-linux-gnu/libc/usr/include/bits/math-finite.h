/* Entry points to finite-math-only compiler runs.
   Copyright (C) 2011-2018 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, see
   <http://www.gnu.org/licenses/>.  */

#ifndef _MATH_H
# error "Never use <bits/math-finite.h> directly; include <math.h> instead."
#endif

#define __REDIRFROM(...) __REDIRFROM_X(__VA_ARGS__)

#define __REDIRTO(...) __REDIRTO_X(__VA_ARGS__)

#define __MATH_REDIRCALL_X(from, args, to) \
  extern _Mdouble_ __REDIRECT_NTH (from, args, to)
#define __MATH_REDIRCALL(function, reentrant, args) \
  __MATH_REDIRCALL_X \
   (__REDIRFROM (function, reentrant), args, \
    __REDIRTO (function, reentrant))
#define __MATH_REDIRCALL_2(from, reentrant, args, to) \
  __MATH_REDIRCALL_X \
   (__REDIRFROM (from, reentrant), args, \
    __REDIRTO (to, reentrant))

#define __MATH_REDIRCALL_INTERNAL(function, reentrant, args) \
  __MATH_REDIRCALL_X \
   (__REDIRFROM (__CONCAT (__, function), \
		 __CONCAT (reentrant, _finite)), \
    args, __REDIRTO (function, _r))


/* acos.  */
__MATH_REDIRCALL (acos, , (_Mdouble_));

#if defined __USE_XOPEN_EXTENDED || defined __USE_ISOC99
/* acosh.  */
__MATH_REDIRCALL (acosh, , (_Mdouble_));
#endif

/* asin.  */
__MATH_REDIRCALL (asin, , (_Mdouble_));

/* atan2.  */
__MATH_REDIRCALL (atan2, , (_Mdouble_, _Mdouble_));

#if defined __USE_XOPEN_EXTENDED || defined __USE_ISOC99
/* atanh.  */
__MATH_REDIRCALL (atanh, , (_Mdouble_));
#endif

/* cosh.  */
__MATH_REDIRCALL (cosh, , (_Mdouble_));

/* exp.  */
__MATH_REDIRCALL (exp, , (_Mdouble_));

#if __GLIBC_USE (IEC_60559_FUNCS_EXT)
/* exp10.  */
__MATH_REDIRCALL (exp10, , (_Mdouble_));
#endif

#ifdef __USE_ISOC99
/* exp2.  */
__MATH_REDIRCALL (exp2, , (_Mdouble_));
#endif

/* fmod.  */
__MATH_REDIRCALL (fmod, , (_Mdouble_, _Mdouble_));

#if defined __USE_XOPEN || defined __USE_ISOC99
/* hypot.  */
__MATH_REDIRCALL (hypot, , (_Mdouble_, _Mdouble_));
#endif

#if (__MATH_DECLARING_DOUBLE && (defined __USE_MISC || defined __USE_XOPEN)) \
    || (!__MATH_DECLARING_DOUBLE && defined __USE_MISC)
/* j0.  */
__MATH_REDIRCALL (j0, , (_Mdouble_));

/* y0.  */
__MATH_REDIRCALL (y0, , (_Mdouble_));

/* j1.  */
__MATH_REDIRCALL (j1, , (_Mdouble_));

/* y1.  */
__MATH_REDIRCALL (y1, , (_Mdouble_));

/* jn.  */
__MATH_REDIRCALL (jn, , (int, _Mdouble_));

/* yn.  */
__MATH_REDIRCALL (yn, , (int, _Mdouble_));
#endif

#ifdef __USE_MISC
/* lgamma_r.  */
__MATH_REDIRCALL (lgamma, _r, (_Mdouble_, int *));
#endif

/* Redirect __lgammal_r_finite to __lgamma_r_finite when __NO_LONG_DOUBLE_MATH
   is set and to itself otherwise.  It also redirects __lgamma_r_finite and
   __lgammaf_r_finite to themselves.  */
__MATH_REDIRCALL_INTERNAL (lgamma, _r, (_Mdouble_, int *));

#if ((defined __USE_XOPEN || defined __USE_ISOC99) \
     && defined __extern_always_inline)
/* lgamma.  */
__extern_always_inline _Mdouble_
__NTH (__REDIRFROM (lgamma, ) (_Mdouble_ __d))
{
# if defined __USE_MISC || defined __USE_XOPEN
  return __REDIRTO (lgamma, _r) (__d, &signgam);
# else
  int __local_signgam = 0;
  return __REDIRTO (lgamma, _r) (__d, &__local_signgam);
# endif
}
#endif

#if ((defined __USE_MISC || (defined __USE_XOPEN && !defined __USE_XOPEN2K)) \
     && defined __extern_always_inline) && !__MATH_DECLARING_FLOATN
/* gamma.  */
__extern_always_inline _Mdouble_
__NTH (__REDIRFROM (gamma, ) (_Mdouble_ __d))
{
  return __REDIRTO (lgamma, _r) (__d, &signgam);
}
#endif

/* log.  */
__MATH_REDIRCALL (log, , (_Mdouble_));

/* log10.  */
__MATH_REDIRCALL (log10, , (_Mdouble_));

#ifdef __USE_ISOC99
/* log2.  */
__MATH_REDIRCALL (log2, , (_Mdouble_));
#endif

/* pow.  */
__MATH_REDIRCALL (pow, , (_Mdouble_, _Mdouble_));

#if defined __USE_XOPEN_EXTENDED || defined __USE_ISOC99
/* remainder.  */
__MATH_REDIRCALL (remainder, , (_Mdouble_, _Mdouble_));
#endif

#if ((__MATH_DECLARING_DOUBLE \
      && (defined __USE_MISC \
	  || (defined __USE_XOPEN_EXTENDED && !defined __USE_XOPEN2K8))) \
     || (!defined __MATH_DECLARE_LDOUBLE && defined __USE_MISC)) \
    && !__MATH_DECLARING_FLOATN
/* scalb.  */
__MATH_REDIRCALL (scalb, , (_Mdouble_, _Mdouble_));
#endif

/* sinh.  */
__MATH_REDIRCALL (sinh, , (_Mdouble_));

/* sqrt.  */
__MATH_REDIRCALL (sqrt, , (_Mdouble_));

#if defined __USE_ISOC99 && defined __extern_always_inline
/* tgamma.  */
extern _Mdouble_
__REDIRFROM (__gamma, _r_finite) (_Mdouble_, int *);

__extern_always_inline _Mdouble_
__NTH (__REDIRFROM (tgamma, ) (_Mdouble_ __d))
{
  int __local_signgam = 0;
  _Mdouble_ __res = __REDIRTO (gamma, _r) (__d, &__local_signgam);
  return __local_signgam < 0 ? -__res : __res;
}
#endif

#undef __REDIRFROM
#undef __REDIRTO
#undef __MATH_REDIRCALL
#undef __MATH_REDIRCALL_2
#undef __MATH_REDIRCALL_INTERNAL
#undef __MATH_REDIRCALL_X
