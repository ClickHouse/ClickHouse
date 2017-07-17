/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright (C) 2014 Tilera Corp.

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

#define UNW_TARGET           tilegx
#define UNW_TARGET_TILEGX    1

#define _U_TDEP_QP_TRUE        0    /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */

#define UNW_TDEP_CURSOR_LEN   4096

/* The size of a "word" varies on TILEGX.  This type is used for memory
   addresses and register values. */
typedef uint64_t unw_word_t;
typedef int64_t unw_sword_t;

typedef long double unw_tdep_fpreg_t;

typedef enum
{
  UNW_TILEGX_R0,
  UNW_TILEGX_R1,
  UNW_TILEGX_R2,
  UNW_TILEGX_R3,
  UNW_TILEGX_R4,
  UNW_TILEGX_R5,
  UNW_TILEGX_R6,
  UNW_TILEGX_R7,
  UNW_TILEGX_R8,
  UNW_TILEGX_R9,
  UNW_TILEGX_R10,
  UNW_TILEGX_R11,
  UNW_TILEGX_R12,
  UNW_TILEGX_R13,
  UNW_TILEGX_R14,
  UNW_TILEGX_R15,
  UNW_TILEGX_R16,
  UNW_TILEGX_R17,
  UNW_TILEGX_R18,
  UNW_TILEGX_R19,
  UNW_TILEGX_R20,
  UNW_TILEGX_R21,
  UNW_TILEGX_R22,
  UNW_TILEGX_R23,
  UNW_TILEGX_R24,
  UNW_TILEGX_R25,
  UNW_TILEGX_R26,
  UNW_TILEGX_R27,
  UNW_TILEGX_R28,
  UNW_TILEGX_R29,
  UNW_TILEGX_R30,
  UNW_TILEGX_R31,
  UNW_TILEGX_R32,
  UNW_TILEGX_R33,
  UNW_TILEGX_R34,
  UNW_TILEGX_R35,
  UNW_TILEGX_R36,
  UNW_TILEGX_R37,
  UNW_TILEGX_R38,
  UNW_TILEGX_R39,
  UNW_TILEGX_R40,
  UNW_TILEGX_R41,
  UNW_TILEGX_R42,
  UNW_TILEGX_R43,
  UNW_TILEGX_R44,
  UNW_TILEGX_R45,
  UNW_TILEGX_R46,
  UNW_TILEGX_R47,
  UNW_TILEGX_R48,
  UNW_TILEGX_R49,
  UNW_TILEGX_R50,
  UNW_TILEGX_R51,
  UNW_TILEGX_R52,
  UNW_TILEGX_R53,
  UNW_TILEGX_R54,
  UNW_TILEGX_R55,

  /* FIXME: Other registers!  */

  UNW_TILEGX_PC,
  /* For TILEGX, the CFA is the value of SP (r54) at the call site in the
     previous frame.  */
  UNW_TILEGX_CFA,

  UNW_TDEP_LAST_REG = UNW_TILEGX_PC,

  UNW_TDEP_IP = UNW_TILEGX_R55,  /* R55 is link register for Tilegx */
  UNW_TDEP_SP = UNW_TILEGX_R54,
  UNW_TDEP_EH = UNW_TILEGX_R0   /* FIXME.  */
} tilegx_regnum_t;

typedef enum
{
  UNW_TILEGX_ABI_N64 = 2
} tilegx_abi_t;

#define UNW_TDEP_NUM_EH_REGS    2   /* FIXME for TILEGX.  */

typedef struct unw_tdep_save_loc
{
  /* Additional target-dependent info on a save location.  */
} unw_tdep_save_loc_t;

typedef ucontext_t unw_tdep_context_t;

#include "libunwind-dynamic.h"

typedef struct
{
    /* no tilegx-specific auxiliary proc-info */
} unw_tdep_proc_info_t;

#include "libunwind-common.h"

#define unw_tdep_getcontext  getcontext

#define unw_tdep_is_fpreg    UNW_ARCH_OBJ(is_fpreg)
extern int unw_tdep_is_fpreg (int);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
