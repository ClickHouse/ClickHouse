/* Macros to swap the order of bytes in integer values.
   Copyright (C) 1997-2018 Free Software Foundation, Inc.
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

#if !defined _BYTESWAP_H && !defined _NETINET_IN_H && !defined _ENDIAN_H
# error "Never use <bits/byteswap.h> directly; include <byteswap.h> instead."
#endif

#ifndef _BITS_BYTESWAP_H
#define _BITS_BYTESWAP_H 1

#include <features.h>
#include <bits/types.h>
#include <bits/wordsize.h>

/* Swap bytes in 16 bit value.  */
#define __bswap_constant_16(x) \
     ((unsigned short int) ((((x) >> 8) & 0xff) | (((x) & 0xff) << 8)))

/* Get __bswap_16.  */
#include <bits/byteswap-16.h>

/* Swap bytes in 32 bit value.  */
#define __bswap_constant_32(x) \
     ((((x) & 0xff000000) >> 24) | (((x) & 0x00ff0000) >>  8) |		      \
      (((x) & 0x0000ff00) <<  8) | (((x) & 0x000000ff) << 24))

#ifdef __GNUC__
# if __GNUC_PREREQ (4, 3)
static __inline unsigned int
__bswap_32 (unsigned int __bsx)
{
  return __builtin_bswap32 (__bsx);
}
# elif __GNUC__ >= 2
#  if __WORDSIZE == 64 || (defined __i486__ || defined __pentium__	      \
			   || defined __pentiumpro__ || defined __pentium4__  \
			   || defined __k8__ || defined __athlon__	      \
			   || defined __k6__ || defined __nocona__	      \
			   || defined __core2__ || defined __geode__	      \
			   || defined __amdfam10__)
/* To swap the bytes in a word the i486 processors and up provide the
   `bswap' opcode.  On i386 we have to use three instructions.  */
#   define __bswap_32(x) \
      (__extension__							      \
       ({ unsigned int __v, __x = (x);					      \
	  if (__builtin_constant_p (__x))				      \
	    __v = __bswap_constant_32 (__x);				      \
	  else								      \
	    __asm__ ("bswap %0" : "=r" (__v) : "0" (__x));		      \
	  __v; }))
#  else
#   define __bswap_32(x)						      \
      (__extension__							      \
       ({ unsigned int __v, __x = (x);					      \
	  if (__builtin_constant_p (__x))				      \
	    __v = __bswap_constant_32 (__x);				      \
	  else								      \
	    __asm__ ("rorw $8, %w0;"					      \
		     "rorl $16, %0;"					      \
		     "rorw $8, %w0"					      \
		     : "=r" (__v)					      \
		     : "0" (__x)					      \
		     : "cc");						      \
	  __v; }))
#  endif
# else
#  define __bswap_32(x) \
     (__extension__							      \
      ({ unsigned int __x = (x); __bswap_constant_32 (__x); }))
# endif
#else
static __inline unsigned int
__bswap_32 (unsigned int __bsx)
{
  return __bswap_constant_32 (__bsx);
}
#endif


#if __GNUC_PREREQ (2, 0)
/* Swap bytes in 64 bit value.  */
# define __bswap_constant_64(x) \
     (__extension__ ((((x) & 0xff00000000000000ull) >> 56)		      \
		     | (((x) & 0x00ff000000000000ull) >> 40)		      \
		     | (((x) & 0x0000ff0000000000ull) >> 24)		      \
		     | (((x) & 0x000000ff00000000ull) >> 8)		      \
		     | (((x) & 0x00000000ff000000ull) << 8)		      \
		     | (((x) & 0x0000000000ff0000ull) << 24)		      \
		     | (((x) & 0x000000000000ff00ull) << 40)		      \
		     | (((x) & 0x00000000000000ffull) << 56)))

# if __GNUC_PREREQ (4, 3)
static __inline __uint64_t
__bswap_64 (__uint64_t __bsx)
{
  return __builtin_bswap64 (__bsx);
}
# elif __WORDSIZE == 64
#  define __bswap_64(x) \
     (__extension__							      \
      ({ __uint64_t __v, __x = (x);					      \
	 if (__builtin_constant_p (__x))				      \
	   __v = __bswap_constant_64 (__x);				      \
	 else								      \
	   __asm__ ("bswap %q0" : "=r" (__v) : "0" (__x));		      \
	 __v; }))
# else
#  define __bswap_64(x) \
     (__extension__                                                           \
      ({ union { __extension__ __uint64_t __ll;		                      \
		 unsigned int __l[2]; } __w, __r;                             \
	 if (__builtin_constant_p (x))                                        \
	   __r.__ll = __bswap_constant_64 (x);                                \
	 else                                                                 \
	   {                                                                  \
	     __w.__ll = (x);                                                  \
	     __r.__l[0] = __bswap_32 (__w.__l[1]);                            \
	     __r.__l[1] = __bswap_32 (__w.__l[0]);                            \
	   }                                                                  \
	 __r.__ll; }))
# endif
#else
# define __bswap_constant_64(x) \
     ((((x) & 0xff00000000000000ull) >> 56)				      \
      | (((x) & 0x00ff000000000000ull) >> 40)				      \
      | (((x) & 0x0000ff0000000000ull) >> 24)				      \
      | (((x) & 0x000000ff00000000ull) >> 8)				      \
      | (((x) & 0x00000000ff000000ull) << 8)				      \
      | (((x) & 0x0000000000ff0000ull) << 24)				      \
      | (((x) & 0x000000000000ff00ull) << 40)				      \
      | (((x) & 0x00000000000000ffull) << 56))

static __inline __uint64_t
__bswap_64 (__uint64_t __bsx)
{
  return __bswap_constant_64 (__bsx);
}
#endif

#endif /* _BITS_BYTESWAP_H */
