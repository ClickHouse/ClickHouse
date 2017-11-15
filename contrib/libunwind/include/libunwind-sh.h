/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>

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

#include <inttypes.h>
#include <stddef.h>
#include <ucontext.h>

#define UNW_TARGET      sh
#define UNW_TARGET_SH   1

#define _U_TDEP_QP_TRUE 0       /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */

#define UNW_TDEP_CURSOR_LEN     4096

typedef uint32_t unw_word_t;
typedef int32_t unw_sword_t;

typedef long double unw_tdep_fpreg_t;

typedef enum
  {
    UNW_SH_R0,
    UNW_SH_R1,
    UNW_SH_R2,
    UNW_SH_R3,
    UNW_SH_R4,
    UNW_SH_R5,
    UNW_SH_R6,
    UNW_SH_R7,
    UNW_SH_R8,
    UNW_SH_R9,
    UNW_SH_R10,
    UNW_SH_R11,
    UNW_SH_R12,
    UNW_SH_R13,
    UNW_SH_R14,
    UNW_SH_R15,

    UNW_SH_PC,
    UNW_SH_PR,

    UNW_TDEP_LAST_REG = UNW_SH_PR,

    UNW_TDEP_IP = UNW_SH_PR,
    UNW_TDEP_SP = UNW_SH_R15,
    UNW_TDEP_EH = UNW_SH_R0
  }
sh_regnum_t;

#define UNW_TDEP_NUM_EH_REGS    2

typedef ucontext_t unw_tdep_context_t;

#define unw_tdep_getcontext(uc)         (getcontext (uc), 0)

typedef struct unw_tdep_save_loc
  {
    /* Additional target-dependent info on a save location.  */
  }
unw_tdep_save_loc_t;

#include "libunwind-dynamic.h"

typedef struct
  {
    /* no sh-specific auxiliary proc-info */
  }
unw_tdep_proc_info_t;

#include "libunwind-common.h"

#define unw_tdep_is_fpreg               UNW_ARCH_OBJ(is_fpreg)
extern int unw_tdep_is_fpreg (int);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
