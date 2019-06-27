/* Definitions of macros to access `dev_t' values.
   Copyright (C) 1996-2018 Free Software Foundation, Inc.
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

#ifndef _SYS_SYSMACROS_H_OUTER

#ifndef __SYSMACROS_DEPRECATED_INCLUSION
# define _SYS_SYSMACROS_H_OUTER 1
#endif

/* If <sys/sysmacros.h> is included after <sys/types.h>, these macros
   will already be defined, and we need to redefine them without the
   deprecation warnings.  (If they are included in the opposite order,
   the outer #ifndef will suppress this entire file and the macros
   will be usable without warnings.)  */
#undef major
#undef minor
#undef makedev

/* This is the macro that must be defined to satisfy the misuse check
   in bits/sysmacros.h. */
#ifndef _SYS_SYSMACROS_H
#define _SYS_SYSMACROS_H 1

#include <features.h>
#include <bits/types.h>
#include <bits/sysmacros.h>

/* Caution: The text of this deprecation message is unquoted, so that
   #symbol can be substituted.  (It is converted to a string by
   __SYSMACROS_DM1.)  This means the message must be a sequence of
   complete pp-tokens; in particular, English contractions (it's,
   can't) cannot be used.

   The message has been manually word-wrapped to fit in 80 columns
   when output by GCC 5 and 6.  The first line is shorter to leave
   some room for the "foo.c:23: warning:" annotation.  */
#define __SYSMACROS_DM(symbol) __SYSMACROS_DM1 \
 (In the GNU C Library, #symbol is defined\n\
  by <sys/sysmacros.h>. For historical compatibility, it is\n\
  currently defined by <sys/types.h> as well, but we plan to\n\
  remove this soon.  To use #symbol, include <sys/sysmacros.h>\n\
  directly.  If you did not intend to use a system-defined macro\n\
  #symbol, you should undefine it after including <sys/types.h>.)

/* This macro is variadic because the deprecation message above
   contains commas.  */
#define __SYSMACROS_DM1(...) __glibc_macro_warning (#__VA_ARGS__)

#define __SYSMACROS_DECL_TEMPL(rtype, name, proto)			     \
  extern rtype gnu_dev_##name proto __THROW __attribute_const__;

#define __SYSMACROS_IMPL_TEMPL(rtype, name, proto)			     \
  __extension__ __extern_inline __attribute_const__ rtype		     \
  __NTH (gnu_dev_##name proto)

__BEGIN_DECLS

__SYSMACROS_DECLARE_MAJOR (__SYSMACROS_DECL_TEMPL)
__SYSMACROS_DECLARE_MINOR (__SYSMACROS_DECL_TEMPL)
__SYSMACROS_DECLARE_MAKEDEV (__SYSMACROS_DECL_TEMPL)

#ifdef __USE_EXTERN_INLINES

__SYSMACROS_DEFINE_MAJOR (__SYSMACROS_IMPL_TEMPL)
__SYSMACROS_DEFINE_MINOR (__SYSMACROS_IMPL_TEMPL)
__SYSMACROS_DEFINE_MAKEDEV (__SYSMACROS_IMPL_TEMPL)

#endif

__END_DECLS

#endif /* _SYS_SYSMACROS_H */

#ifndef __SYSMACROS_NEED_IMPLEMENTATION
# undef __SYSMACROS_DECL_TEMPL
# undef __SYSMACROS_IMPL_TEMPL
# undef __SYSMACROS_DECLARE_MAJOR
# undef __SYSMACROS_DECLARE_MINOR
# undef __SYSMACROS_DECLARE_MAKEDEV
# undef __SYSMACROS_DEFINE_MAJOR
# undef __SYSMACROS_DEFINE_MINOR
# undef __SYSMACROS_DEFINE_MAKEDEV
#endif

#ifdef __SYSMACROS_DEPRECATED_INCLUSION
# define major(dev) __SYSMACROS_DM (major) gnu_dev_major (dev)
# define minor(dev) __SYSMACROS_DM (minor) gnu_dev_minor (dev)
# define makedev(maj, min) __SYSMACROS_DM (makedev) gnu_dev_makedev (maj, min)
#else
# define major(dev) gnu_dev_major (dev)
# define minor(dev) gnu_dev_minor (dev)
# define makedev(maj, min) gnu_dev_makedev (maj, min)
#endif

#endif /* sys/sysmacros.h */
