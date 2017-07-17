/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co

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
#include <ucontext.h>

#define UNW_TARGET      hppa
#define UNW_TARGET_HPPA 1

#define _U_TDEP_QP_TRUE 0       /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */
#define UNW_TDEP_CURSOR_LEN     511

typedef uint32_t unw_word_t;
typedef int32_t unw_sword_t;

typedef union
  {
    struct { unw_word_t bits[2]; } raw;
    double val;
  }
unw_tdep_fpreg_t;

typedef enum
  {
    /* Note: general registers are expected to start with index 0.
       This convention facilitates architecture-independent
       implementation of the C++ exception handling ABI.  See
       _Unwind_SetGR() and _Unwind_GetGR() for details.  */
    UNW_HPPA_GR = 0,
     UNW_HPPA_RP = 2,                   /* return pointer */
     UNW_HPPA_FP = 3,                   /* frame pointer */
     UNW_HPPA_SP = UNW_HPPA_GR + 30,

    UNW_HPPA_FR = UNW_HPPA_GR + 32,

    UNW_HPPA_IP = UNW_HPPA_FR + 32,     /* instruction pointer */

    /* other "preserved" registers (fpsr etc.)... */

    /* PA-RISC has 4 exception-argument registers but they're not
       contiguous.  To deal with this, we define 4 pseudo
       exception-handling registers which we then alias to the actual
       physical register.  */

    UNW_HPPA_EH0 = UNW_HPPA_IP + 1,     /* alias for UNW_HPPA_GR + 20 */
    UNW_HPPA_EH1 = UNW_HPPA_EH0 + 1,    /* alias for UNW_HPPA_GR + 21 */
    UNW_HPPA_EH2 = UNW_HPPA_EH1 + 1,    /* alias for UNW_HPPA_GR + 22 */
    UNW_HPPA_EH3 = UNW_HPPA_EH2 + 1,    /* alias for UNW_HPPA_GR + 31 */

    /* frame info (read-only) */
    UNW_HPPA_CFA,

    UNW_TDEP_LAST_REG = UNW_HPPA_IP,

    UNW_TDEP_IP = UNW_HPPA_IP,
    UNW_TDEP_SP = UNW_HPPA_SP,
    UNW_TDEP_EH = UNW_HPPA_EH0
  }
hppa_regnum_t;

#define UNW_TDEP_NUM_EH_REGS    4

typedef struct unw_tdep_save_loc
  {
    /* Additional target-dependent info on a save location.  */
  }
unw_tdep_save_loc_t;

/* On PA-RISC, we can directly use ucontext_t as the unwind context.  */
typedef ucontext_t unw_tdep_context_t;

#define unw_tdep_is_fpreg(r)            ((unsigned) ((r) - UNW_HPPA_FR) < 32)

#include "libunwind-dynamic.h"

typedef struct
  {
    /* no PA-RISC-specific auxiliary proc-info */
  }
unw_tdep_proc_info_t;

#include "libunwind-common.h"

#define unw_tdep_getcontext             UNW_ARCH_OBJ (getcontext)
extern int unw_tdep_getcontext (unw_tdep_context_t *);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
