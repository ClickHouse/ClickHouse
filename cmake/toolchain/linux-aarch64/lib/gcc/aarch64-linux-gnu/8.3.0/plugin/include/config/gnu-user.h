/* Definitions for systems using, at least optionally, a GNU
   (glibc-based) userspace or other userspace with libc derived from
   glibc (e.g. uClibc) or for which similar specs are appropriate.
   Copyright (C) 1995-2018 Free Software Foundation, Inc.
   Contributed by Eric Youngdale.
   Modified for stabs-in-ELF by H.J. Lu (hjl@lucon.org).

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option)
any later version.

GCC is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

/* Don't assume anything about the header files.  */
#define NO_IMPLICIT_EXTERN_C

#undef ASM_APP_ON
#define ASM_APP_ON "#APP\n"

#undef ASM_APP_OFF
#define ASM_APP_OFF "#NO_APP\n"

#if ENABLE_OFFLOADING == 1
#define CRTOFFLOADBEGIN "%{fopenacc|fopenmp:crtoffloadbegin%O%s}"
#define CRTOFFLOADEND "%{fopenacc|fopenmp:crtoffloadend%O%s}"
#else
#define CRTOFFLOADBEGIN ""
#define CRTOFFLOADEND ""
#endif

/* Provide a STARTFILE_SPEC appropriate for GNU userspace.  Here we add
   the GNU userspace magical crtbegin.o file (see crtstuff.c) which
   provides part of the support for getting C++ file-scope static
   object constructed before entering `main'.  */

#if defined HAVE_LD_PIE
#define GNU_USER_TARGET_STARTFILE_SPEC \
  "%{shared:; \
     pg|p|profile:%{static-pie:grcrt1.o%s;:gcrt1.o%s}; \
     static:crt1.o%s; \
     static-pie:rcrt1.o%s; \
     " PIE_SPEC ":Scrt1.o%s; \
     :crt1.o%s} \
   crti.o%s \
   %{static:crtbeginT.o%s; \
     shared|static-pie|" PIE_SPEC ":crtbeginS.o%s; \
     :crtbegin.o%s} \
   %{fvtable-verify=none:%s; \
     fvtable-verify=preinit:vtv_start_preinit.o%s; \
     fvtable-verify=std:vtv_start.o%s} \
   " CRTOFFLOADBEGIN
#else
#define GNU_USER_TARGET_STARTFILE_SPEC \
  "%{shared:; \
     pg|p|profile:gcrt1.o%s; \
     :crt1.o%s} \
   crti.o%s \
   %{static:crtbeginT.o%s; \
     shared|pie|static-pie:crtbeginS.o%s; \
     :crtbegin.o%s} \
   %{fvtable-verify=none:%s; \
     fvtable-verify=preinit:vtv_start_preinit.o%s; \
     fvtable-verify=std:vtv_start.o%s} \
   " CRTOFFLOADBEGIN
#endif
#undef  STARTFILE_SPEC
#define STARTFILE_SPEC GNU_USER_TARGET_STARTFILE_SPEC

/* Provide a ENDFILE_SPEC appropriate for GNU userspace.  Here we tack on
   the GNU userspace magical crtend.o file (see crtstuff.c) which
   provides part of the support for getting C++ file-scope static
   object constructed before entering `main', followed by a normal
   GNU userspace "finalizer" file, `crtn.o'.  */

#if defined HAVE_LD_PIE
#define GNU_USER_TARGET_ENDFILE_SPEC \
  "%{fvtable-verify=none:%s; \
     fvtable-verify=preinit:vtv_end_preinit.o%s; \
     fvtable-verify=std:vtv_end.o%s} \
   %{static:crtend.o%s; \
     shared|static-pie|" PIE_SPEC ":crtendS.o%s; \
     :crtend.o%s} \
   crtn.o%s \
   " CRTOFFLOADEND
#else
#define GNU_USER_TARGET_ENDFILE_SPEC \
  "%{fvtable-verify=none:%s; \
     fvtable-verify=preinit:vtv_end_preinit.o%s; \
     fvtable-verify=std:vtv_end.o%s} \
   %{static:crtend.o%s; \
     shared|pie|static-pie:crtendS.o%s; \
     :crtend.o%s} \
   crtn.o%s \
   " CRTOFFLOADEND
#endif
#undef  ENDFILE_SPEC
#define ENDFILE_SPEC GNU_USER_TARGET_ENDFILE_SPEC

/* This is for -profile to use -lc_p instead of -lc.  */
#define GNU_USER_TARGET_CC1_SPEC "%{profile:-p}"
#ifndef CC1_SPEC
#define CC1_SPEC GNU_USER_TARGET_CC1_SPEC
#endif

/* The GNU C++ standard library requires that these macros be defined.  */
#undef CPLUSPLUS_CPP_SPEC
#define CPLUSPLUS_CPP_SPEC "-D_GNU_SOURCE %(cpp)"

#define GNU_USER_TARGET_NO_PTHREADS_LIB_SPEC \
  "%{shared:-lc} \
   %{!shared:%{profile:-lc_p}%{!profile:-lc}}"

#define GNU_USER_TARGET_LIB_SPEC \
  "%{pthread:-lpthread} " \
  GNU_USER_TARGET_NO_PTHREADS_LIB_SPEC

#undef  LIB_SPEC
#define LIB_SPEC GNU_USER_TARGET_LIB_SPEC

#if defined(HAVE_LD_EH_FRAME_HDR)
#define LINK_EH_SPEC "%{!static|static-pie:--eh-frame-hdr} "
#endif

#undef LINK_GCC_C_SEQUENCE_SPEC
#define LINK_GCC_C_SEQUENCE_SPEC \
  "%{static|static-pie:--start-group} %G %L \
   %{static|static-pie:--end-group}%{!static:%{!static-pie:%G}}"

/* Use --as-needed -lgcc_s for eh support.  */
#ifdef HAVE_LD_AS_NEEDED
#define USE_LD_AS_NEEDED 1
#endif

#define TARGET_POSIX_IO

#undef TARGET_LIBC_HAS_FUNCTION
#define TARGET_LIBC_HAS_FUNCTION gnu_libc_has_function

/* Link -lasan early on the command line.  For -static-libasan, don't link
   it for -shared link, the executable should be compiled with -static-libasan
   in that case, and for executable link with --{,no-}whole-archive around
   it to force everything into the executable.  And similarly for -ltsan
   and -llsan.  */
#if defined(HAVE_LD_STATIC_DYNAMIC)
#undef LIBASAN_EARLY_SPEC
#define LIBASAN_EARLY_SPEC "%{!shared:libasan_preinit%O%s} " \
  "%{static-libasan:%{!shared:" \
  LD_STATIC_OPTION " --whole-archive -lasan --no-whole-archive " \
  LD_DYNAMIC_OPTION "}}%{!static-libasan:-lasan}"
#undef LIBTSAN_EARLY_SPEC
#define LIBTSAN_EARLY_SPEC "%{!shared:libtsan_preinit%O%s} " \
  "%{static-libtsan:%{!shared:" \
  LD_STATIC_OPTION " --whole-archive -ltsan --no-whole-archive " \
  LD_DYNAMIC_OPTION "}}%{!static-libtsan:-ltsan}"
#undef LIBLSAN_EARLY_SPEC
#define LIBLSAN_EARLY_SPEC "%{!shared:liblsan_preinit%O%s} " \
  "%{static-liblsan:%{!shared:" \
  LD_STATIC_OPTION " --whole-archive -llsan --no-whole-archive " \
  LD_DYNAMIC_OPTION "}}%{!static-liblsan:-llsan}"
#endif
