/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

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

#ifndef LIBUNWIND_H
#define LIBUNWIND_H

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#include <sys/types.h>
#include <inttypes.h>
#include <ucontext.h>

#define UNW_TARGET      x86
#define UNW_TARGET_X86  1

#define _U_TDEP_QP_TRUE 0       /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */
#define UNW_TDEP_CURSOR_LEN     127

typedef uint32_t unw_word_t;
typedef int32_t unw_sword_t;

typedef union {
  struct { uint8_t b[4]; } val32;
  struct { uint8_t b[10]; } val80;
  struct { uint8_t b[16]; } val128;
} unw_tdep_fpreg_t;

typedef enum
  {
    /* Note: general registers are expected to start with index 0.
       This convention facilitates architecture-independent
       implementation of the C++ exception handling ABI.  See
       _Unwind_SetGR() and _Unwind_GetGR() for details.

       The described register usage convention is based on "System V
       Application Binary Interface, Intel386 Architecture Processor
       Supplement, Fourth Edition" at

         http://www.linuxbase.org/spec/refspecs/elf/abi386-4.pdf

       It would have been nice to use the same register numbering as
       DWARF, but that doesn't work because the libunwind requires
       that the exception argument registers be consecutive, which the
       wouldn't be with the DWARF numbering.  */
    UNW_X86_EAX,        /* scratch (exception argument 1) */
    UNW_X86_EDX,        /* scratch (exception argument 2) */
    UNW_X86_ECX,        /* scratch */
    UNW_X86_EBX,        /* preserved */
    UNW_X86_ESI,        /* preserved */
    UNW_X86_EDI,        /* preserved */
    UNW_X86_EBP,        /* (optional) frame-register */
    UNW_X86_ESP,        /* (optional) frame-register */
    UNW_X86_EIP,        /* frame-register */
    UNW_X86_EFLAGS,     /* scratch (except for "direction", which is fixed */
    UNW_X86_TRAPNO,     /* scratch */

    /* MMX/stacked-fp registers */
    UNW_X86_ST0,        /* fp return value */
    UNW_X86_ST1,        /* scratch */
    UNW_X86_ST2,        /* scratch */
    UNW_X86_ST3,        /* scratch */
    UNW_X86_ST4,        /* scratch */
    UNW_X86_ST5,        /* scratch */
    UNW_X86_ST6,        /* scratch */
    UNW_X86_ST7,        /* scratch */

    UNW_X86_FCW,        /* scratch */
    UNW_X86_FSW,        /* scratch */
    UNW_X86_FTW,        /* scratch */
    UNW_X86_FOP,        /* scratch */
    UNW_X86_FCS,        /* scratch */
    UNW_X86_FIP,        /* scratch */
    UNW_X86_FEA,        /* scratch */
    UNW_X86_FDS,        /* scratch */

    /* SSE registers */
    UNW_X86_XMM0_lo,    /* scratch */
    UNW_X86_XMM0_hi,    /* scratch */
    UNW_X86_XMM1_lo,    /* scratch */
    UNW_X86_XMM1_hi,    /* scratch */
    UNW_X86_XMM2_lo,    /* scratch */
    UNW_X86_XMM2_hi,    /* scratch */
    UNW_X86_XMM3_lo,    /* scratch */
    UNW_X86_XMM3_hi,    /* scratch */
    UNW_X86_XMM4_lo,    /* scratch */
    UNW_X86_XMM4_hi,    /* scratch */
    UNW_X86_XMM5_lo,    /* scratch */
    UNW_X86_XMM5_hi,    /* scratch */
    UNW_X86_XMM6_lo,    /* scratch */
    UNW_X86_XMM6_hi,    /* scratch */
    UNW_X86_XMM7_lo,    /* scratch */
    UNW_X86_XMM7_hi,    /* scratch */

    UNW_X86_MXCSR,      /* scratch */

    /* segment registers */
    UNW_X86_GS,         /* special */
    UNW_X86_FS,         /* special */
    UNW_X86_ES,         /* special */
    UNW_X86_DS,         /* special */
    UNW_X86_SS,         /* special */
    UNW_X86_CS,         /* special */
    UNW_X86_TSS,        /* special */
    UNW_X86_LDT,        /* special */

    /* frame info (read-only) */
    UNW_X86_CFA,

    UNW_X86_XMM0,       /* scratch */
    UNW_X86_XMM1,       /* scratch */
    UNW_X86_XMM2,       /* scratch */
    UNW_X86_XMM3,       /* scratch */
    UNW_X86_XMM4,       /* scratch */
    UNW_X86_XMM5,       /* scratch */
    UNW_X86_XMM6,       /* scratch */
    UNW_X86_XMM7,       /* scratch */

    UNW_TDEP_LAST_REG = UNW_X86_XMM7,

    UNW_TDEP_IP = UNW_X86_EIP,
    UNW_TDEP_SP = UNW_X86_ESP,
    UNW_TDEP_EH = UNW_X86_EAX
  }
x86_regnum_t;

#define UNW_TDEP_NUM_EH_REGS    2       /* eax and edx are exception args */

typedef struct unw_tdep_save_loc
  {
    /* Additional target-dependent info on a save location.  */
  }
unw_tdep_save_loc_t;

/* On x86, we can directly use ucontext_t as the unwind context.  */
typedef ucontext_t unw_tdep_context_t;

#include "libunwind-dynamic.h"

typedef struct
  {
    /* no x86-specific auxiliary proc-info */
  }
unw_tdep_proc_info_t;

#include "libunwind-common.h"

#define unw_tdep_getcontext             UNW_ARCH_OBJ(getcontext)
extern int unw_tdep_getcontext (unw_tdep_context_t *);

#define unw_tdep_is_fpreg               UNW_ARCH_OBJ(is_fpreg)
extern int unw_tdep_is_fpreg (int);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
