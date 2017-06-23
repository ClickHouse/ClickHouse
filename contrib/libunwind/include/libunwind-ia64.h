/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2004 Hewlett-Packard Co
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

#include <inttypes.h>
#include <ucontext.h>

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#ifdef ia64
  /* This works around a bug in Intel's ECC v7.0 which defines "ia64"
     as "1".  */
# undef ia64
#endif

#ifdef __hpux
  /* On HP-UX, there is no hope of supporting UNW_LOCAL_ONLY, because
     it's impossible to obtain the address of the members in the
     sigcontext structure.  */
# undef UNW_LOCAL_ONLY
# define UNW_GENERIC_ONLY
#endif

#define UNW_TARGET      ia64
#define UNW_TARGET_IA64 1

#define _U_TDEP_QP_TRUE 0       /* see libunwind-dynamic.h  */

/* This needs to be big enough to accommodate "struct cursor", while
   leaving some slack for future expansion.  Changing this value will
   require recompiling all users of this library.  Stack allocation is
   relatively cheap and unwind-state copying is relatively rare, so we
   want to err on making it rather too big than too small.  */
#define UNW_TDEP_CURSOR_LEN     511

/* If this bit is it indicates that the procedure saved all of ar.bsp,
   ar.bspstore, and ar.rnat.  If, additionally, ar.bsp != saved ar.bsp,
   then this procedure has performed a register-backing-store switch.  */
#define UNW_PI_FLAG_IA64_RBS_SWITCH_BIT (UNW_PI_FLAG_FIRST_TDEP_BIT + 0)

#define UNW_PI_FLAG_IA64_RBS_SWITCH     (1 << UNW_PI_FLAG_IA64_RBS_SWITCH_BIT)

typedef uint64_t unw_word_t;
typedef int64_t unw_sword_t;

/* On IA-64, we want to access the contents of floating-point
   registers as a pair of "words", but to ensure 16-byte alignment, we
   make it a union that contains a "long double".  This will do the
   Right Thing on all known IA-64 platforms, including HP-UX.  */
typedef union
  {
    struct { unw_word_t bits[2]; } raw;
    long double dummy;  /* dummy to force 16-byte alignment */
  }
unw_tdep_fpreg_t;

typedef struct
  {
    /* no ia64-specific auxiliary proc-info */
  }
unw_tdep_proc_info_t;

typedef enum
  {
    /* Note: general registers are excepted to start with index 0.
       This convention facilitates architecture-independent
       implementation of the C++ exception handling ABI.  See
       _Unwind_SetGR() and _Unwind_GetGR() for details.  */
    UNW_IA64_GR = 0,                    /* general registers (r0..r127) */
     UNW_IA64_GP = UNW_IA64_GR + 1,
     UNW_IA64_TP = UNW_IA64_GR + 13,

    UNW_IA64_NAT = UNW_IA64_GR + 128,   /* NaT registers (nat0..nat127) */

    UNW_IA64_FR = UNW_IA64_NAT + 128,   /* fp registers (f0..f127) */

    UNW_IA64_AR = UNW_IA64_FR + 128,    /* application registers (ar0..r127) */
     UNW_IA64_AR_RSC = UNW_IA64_AR + 16,
     UNW_IA64_AR_BSP = UNW_IA64_AR + 17,
     UNW_IA64_AR_BSPSTORE = UNW_IA64_AR + 18,
     UNW_IA64_AR_RNAT = UNW_IA64_AR + 19,
     UNW_IA64_AR_CSD = UNW_IA64_AR + 25,
     UNW_IA64_AR_26 = UNW_IA64_AR + 26,
     UNW_IA64_AR_SSD = UNW_IA64_AR_26,
     UNW_IA64_AR_CCV = UNW_IA64_AR + 32,
     UNW_IA64_AR_UNAT = UNW_IA64_AR + 36,
     UNW_IA64_AR_FPSR = UNW_IA64_AR + 40,
     UNW_IA64_AR_PFS = UNW_IA64_AR + 64,
     UNW_IA64_AR_LC = UNW_IA64_AR + 65,
     UNW_IA64_AR_EC = UNW_IA64_AR + 66,

    UNW_IA64_BR = UNW_IA64_AR + 128,    /* branch registers (b0..p7) */
      UNW_IA64_RP = UNW_IA64_BR + 0,    /* return pointer (rp) */
    UNW_IA64_PR = UNW_IA64_BR + 8,      /* predicate registers (p0..p63) */
    UNW_IA64_CFM,

    /* frame info: */
    UNW_IA64_BSP,
    UNW_IA64_IP,
    UNW_IA64_SP,

    UNW_TDEP_LAST_REG = UNW_IA64_SP,

    UNW_TDEP_IP = UNW_IA64_IP,
    UNW_TDEP_SP = UNW_IA64_SP,
    UNW_TDEP_EH = UNW_IA64_GR + 15
  }
ia64_regnum_t;

#define UNW_TDEP_NUM_EH_REGS    4       /* r15-r18 are exception args */

typedef struct unw_tdep_save_loc
  {
    /* Additional target-dependent info on a save location.  On IA-64,
       we use this to provide the bit number in which a NaT bit gets
       saved.  */
    uint8_t nat_bitnr;

    /* Padding reserved for future use.  */
    uint8_t reserved[7];
  }
unw_tdep_save_loc_t;

/* On IA-64, we can directly use ucontext_t as the unwind context.  */
typedef ucontext_t unw_tdep_context_t;

#define unw_tdep_is_fpreg(r)            ((unsigned) ((r) - UNW_IA64_FR) < 128)

#include "libunwind-dynamic.h"
#include "libunwind-common.h"

#ifdef __hpux
  /* In theory, we could use _Uia64_getcontext() on HP-UX as well, but
     the benefit of doing so would be marginal given that it can't
     support UNW_LOCAL_ONLY.  */
# define unw_tdep_getcontext            getcontext
#else
# define unw_tdep_getcontext            UNW_ARCH_OBJ (getcontext)
  extern int unw_tdep_getcontext (unw_tdep_context_t *);
#endif

/* This is a helper routine to search an ia64 unwind table.  If the
   address-space argument AS points to something other than the local
   address-space, the memory for the unwind-info will be allocated
   with malloc(), and should be free()d during the put_unwind_info()
   callback.  This routine is signal-safe for the local-address-space
   case ONLY.  */
#define unw_search_ia64_unwind_table    UNW_OBJ(search_unwind_table)
extern int unw_search_ia64_unwind_table (unw_addr_space_t, unw_word_t,
                                         unw_dyn_info_t *, unw_proc_info_t *,
                                         int, void *);

/* This is a helper routine which the get_dyn_info_list_addr()
   callback can use to locate the special dynamic-info list entry in
   an IA-64 unwind table.  If the entry exists in the table, the
   list-address is returned.  In all other cases, 0 is returned.  */
extern unw_word_t _Uia64_find_dyn_list (unw_addr_space_t, unw_dyn_info_t *,
                                        void *);

/* This is a helper routine to obtain the kernel-unwind info.  It is
   signal-safe.  */
extern int _Uia64_get_kernel_table (unw_dyn_info_t *);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* LIBUNWIND_H */
