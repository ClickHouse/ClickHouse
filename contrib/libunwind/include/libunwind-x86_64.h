/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

   Modified for x86_64 by Max Asbock <masbock@us.ibm.com>

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

#define UNW_TARGET              x86_64
#define UNW_TARGET_X86_64       1

#define _U_TDEP_QP_TRUE 0       /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */
#define UNW_TDEP_CURSOR_LEN     127

typedef uint64_t unw_word_t;
typedef int64_t unw_sword_t;

typedef long double unw_tdep_fpreg_t;

typedef enum
  {
    UNW_X86_64_RAX,
    UNW_X86_64_RDX,
    UNW_X86_64_RCX,
    UNW_X86_64_RBX,
    UNW_X86_64_RSI,
    UNW_X86_64_RDI,
    UNW_X86_64_RBP,
    UNW_X86_64_RSP,
    UNW_X86_64_R8,
    UNW_X86_64_R9,
    UNW_X86_64_R10,
    UNW_X86_64_R11,
    UNW_X86_64_R12,
    UNW_X86_64_R13,
    UNW_X86_64_R14,
    UNW_X86_64_R15,
    UNW_X86_64_RIP,
#ifdef CONFIG_MSABI_SUPPORT
    UNW_X86_64_XMM0,
    UNW_X86_64_XMM1,
    UNW_X86_64_XMM2,
    UNW_X86_64_XMM3,
    UNW_X86_64_XMM4,
    UNW_X86_64_XMM5,
    UNW_X86_64_XMM6,
    UNW_X86_64_XMM7,
    UNW_X86_64_XMM8,
    UNW_X86_64_XMM9,
    UNW_X86_64_XMM10,
    UNW_X86_64_XMM11,
    UNW_X86_64_XMM12,
    UNW_X86_64_XMM13,
    UNW_X86_64_XMM14,
    UNW_X86_64_XMM15,
    UNW_TDEP_LAST_REG = UNW_X86_64_XMM15,
#else
    UNW_TDEP_LAST_REG = UNW_X86_64_RIP,
#endif

    /* XXX Add other regs here */

    /* frame info (read-only) */
    UNW_X86_64_CFA,

    UNW_TDEP_IP = UNW_X86_64_RIP,
    UNW_TDEP_SP = UNW_X86_64_RSP,
    UNW_TDEP_BP = UNW_X86_64_RBP,
    UNW_TDEP_EH = UNW_X86_64_RAX
  }
x86_64_regnum_t;

#define UNW_TDEP_NUM_EH_REGS    2       /* XXX Not sure what this means */

typedef struct unw_tdep_save_loc
  {
    /* Additional target-dependent info on a save location.  */
    char unused;
  }
unw_tdep_save_loc_t;

/* On x86_64, we can directly use ucontext_t as the unwind context.  */
typedef ucontext_t unw_tdep_context_t;

typedef struct
  {
    /* no x86-64-specific auxiliary proc-info */
    char unused;
  }
unw_tdep_proc_info_t;

#include "libunwind-dynamic.h"
#include "libunwind-common.h"

#define unw_tdep_getcontext             UNW_ARCH_OBJ(getcontext)
#define unw_tdep_is_fpreg               UNW_ARCH_OBJ(is_fpreg)

extern int unw_tdep_getcontext (unw_tdep_context_t *);
extern int unw_tdep_is_fpreg (int);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
