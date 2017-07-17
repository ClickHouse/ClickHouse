/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2005 Hewlett-Packard Co
   Copyright (C) 2007 David Mosberger-Tang
        Contributed by David Mosberger-Tang <dmosberger@gmail.com>

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

/* Compiler specific useful bits that are used in libunwind, and also in the
 * tests. */

#ifndef COMPILER_H
#define COMPILER_H

#ifdef __GNUC__
# define ALIGNED(x)     __attribute__((aligned(x)))
# define CONST_ATTR     __attribute__((__const__))
# define UNUSED         __attribute__((unused))
# define NOINLINE       __attribute__((noinline))
# define NORETURN       __attribute__((noreturn))
# define ALIAS(name)    __attribute__((alias (#name)))
# if (__GNUC__ > 3) || (__GNUC__ == 3 && __GNUC_MINOR__ > 2)
#  define ALWAYS_INLINE inline __attribute__((always_inline))
#  define HIDDEN        __attribute__((visibility ("hidden")))
#  define PROTECTED     __attribute__((visibility ("protected")))
# else
#  define ALWAYS_INLINE
#  define HIDDEN
#  define PROTECTED
# endif
# define WEAK           __attribute__((weak))
# if (__GNUC__ >= 3)
#  define likely(x)     __builtin_expect ((x), 1)
#  define unlikely(x)   __builtin_expect ((x), 0)
# else
#  define likely(x)     (x)
#  define unlikely(x)   (x)
# endif
#else
# define ALIGNED(x)
# define ALWAYS_INLINE
# define CONST_ATTR
# define UNUSED
# define NOINLINE
# define NORETURN
# define ALIAS(name)
# define HIDDEN
# define PROTECTED
# define WEAK
# define likely(x)      (x)
# define unlikely(x)    (x)
#endif

#define ARRAY_SIZE(a)   (sizeof (a) / sizeof ((a)[0]))

#endif /* COMPILER_H */
