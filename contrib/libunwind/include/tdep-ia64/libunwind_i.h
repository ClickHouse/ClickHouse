/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2005 Hewlett-Packard Co
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

#ifndef IA64_LIBUNWIND_I_H
#define IA64_LIBUNWIND_I_H

/* Target-dependent definitions that are internal to libunwind but need
   to be shared with target-independent code.  */

#include "elf64.h"
#include "mempool.h"

typedef struct
  {
    /* no ia64-specific fast trace */
  }
unw_tdep_frame_t;

enum ia64_pregnum
  {
    /* primary unat: */
    IA64_REG_PRI_UNAT_GR,
    IA64_REG_PRI_UNAT_MEM,

    /* memory stack (order matters: see build_script() */
    IA64_REG_PSP,                       /* previous memory stack pointer */
    /* register stack */
    IA64_REG_BSP,                       /* register stack pointer */
    IA64_REG_BSPSTORE,
    IA64_REG_PFS,                       /* previous function state */
    IA64_REG_RNAT,
    /* instruction pointer: */
    IA64_REG_IP,

    /* preserved registers: */
    IA64_REG_R4, IA64_REG_R5, IA64_REG_R6, IA64_REG_R7,
    IA64_REG_NAT4, IA64_REG_NAT5, IA64_REG_NAT6, IA64_REG_NAT7,
    IA64_REG_UNAT, IA64_REG_PR, IA64_REG_LC, IA64_REG_FPSR,
    IA64_REG_B1, IA64_REG_B2, IA64_REG_B3, IA64_REG_B4, IA64_REG_B5,
    IA64_REG_F2, IA64_REG_F3, IA64_REG_F4, IA64_REG_F5,
    IA64_REG_F16, IA64_REG_F17, IA64_REG_F18, IA64_REG_F19,
    IA64_REG_F20, IA64_REG_F21, IA64_REG_F22, IA64_REG_F23,
    IA64_REG_F24, IA64_REG_F25, IA64_REG_F26, IA64_REG_F27,
    IA64_REG_F28, IA64_REG_F29, IA64_REG_F30, IA64_REG_F31,
    IA64_NUM_PREGS
  };

#ifdef UNW_LOCAL_ONLY

typedef unw_word_t ia64_loc_t;

#else /* !UNW_LOCAL_ONLY */

typedef struct ia64_loc
  {
    unw_word_t w0, w1;
  }
ia64_loc_t;

#endif /* !UNW_LOCAL_ONLY */

#include "script.h"

#define ABI_UNKNOWN                     0
#define ABI_LINUX                       1
#define ABI_HPUX                        2
#define ABI_FREEBSD                     3
#define ABI_OPENVMS                     4
#define ABI_NSK                         5       /* Tandem/HP Non-Stop Kernel */
#define ABI_WINDOWS                     6

struct unw_addr_space
  {
    struct unw_accessors acc;
    int big_endian;
    int abi;    /* abi < 0 => unknown, 0 => SysV, 1 => HP-UX, 2 => Windows */
    unw_caching_policy_t caching_policy;
#ifdef HAVE_ATOMIC_OPS_H
    AO_t cache_generation;
#else
    uint32_t cache_generation;
#endif
    unw_word_t dyn_generation;
    unw_word_t dyn_info_list_addr;      /* (cached) dyn_info_list_addr */
#ifndef UNW_REMOTE_ONLY
    unsigned long long shared_object_removals;
#endif

    struct ia64_script_cache global_cache;
   };

/* Note: The ABI numbers in the ABI-markers (.unwabi directive) are
   not the same as the above ABI numbers.  */
#define ABI_MARKER_OLD_LINUX_SIGTRAMP   ((0 << 8) | 's')
#define ABI_MARKER_OLD_LINUX_INTERRUPT  ((0 << 8) | 'i')
#define ABI_MARKER_HP_UX_SIGTRAMP       ((1 << 8) | 1)
#define ABI_MARKER_LINUX_SIGTRAMP       ((3 << 8) | 's')
#define ABI_MARKER_LINUX_INTERRUPT      ((3 << 8) | 'i')

struct cursor
  {
    void *as_arg;               /* argument to address-space callbacks */
    unw_addr_space_t as;        /* reference to per-address-space info */

    /* IP, CFM, and predicate cache (these are always equal to the
       values stored in ip_loc, cfm_loc, and pr_loc,
       respectively).  */
    unw_word_t ip;              /* instruction pointer value */
    unw_word_t cfm;             /* current frame mask */
    unw_word_t pr;              /* current predicate values */

    /* current frame info: */
    unw_word_t bsp;             /* backing store pointer value */
    unw_word_t sp;              /* stack pointer value */
    unw_word_t psp;             /* previous sp value */
    ia64_loc_t cfm_loc;         /* cfm save location (or NULL) */
    ia64_loc_t ec_loc;          /* ar.ec save location (usually cfm_loc) */
    ia64_loc_t loc[IA64_NUM_PREGS];

    unw_word_t eh_args[4];      /* exception handler arguments */
    unw_word_t sigcontext_addr; /* address of sigcontext or 0 */
    unw_word_t sigcontext_off;  /* sigcontext-offset relative to signal sp */

    short hint;
    short prev_script;

    uint8_t nat_bitnr[4];       /* NaT bit numbers for r4-r7 */
    uint16_t abi_marker;        /* abi_marker for current frame (if any) */
    uint16_t last_abi_marker;   /* last abi_marker encountered so far */
    uint8_t eh_valid_mask;

    unsigned int pi_valid :1;           /* is proc_info valid? */
    unsigned int pi_is_dynamic :1; /* proc_info found via dynamic proc info? */
    unw_proc_info_t pi;         /* info about current procedure */

    /* In case of stack-discontiguities, such as those introduced by
       signal-delivery on an alternate signal-stack (see
       sigaltstack(2)), we use the following data-structure to keep
       track of the register-backing-store areas across on which the
       current frame may be backed up.  Since there are at most 96
       stacked registers and since we only have to track the current
       frame and only areas that are not empty, this puts an upper
       limit on the # of backing-store areas we have to track.

       Note that the rbs-area indexed by rbs_curr identifies the
       rbs-area that was in effect at the time AR.BSP had the value
       c->bsp.  However, this rbs area may not actually contain the
       value in the register that c->bsp corresponds to because that
       register may not have gotten spilled until much later, when a
       possibly different rbs-area might have been in effect
       already.  */
    uint8_t rbs_curr;           /* index of curr. rbs-area (contains c->bsp) */
    uint8_t rbs_left_edge;      /* index of inner-most valid rbs-area */
    struct rbs_area
      {
        unw_word_t end;
        unw_word_t size;
        ia64_loc_t rnat_loc;
      }
    rbs_area[96 + 2];   /* 96 stacked regs + 1 extra stack on each side... */
};

struct ia64_global_unwind_state
  {
    pthread_mutex_t lock;               /* global data lock */

    volatile char init_done;

    /* Table of registers that prologues can save (and order in which
       they're saved).  */
    const unsigned char save_order[8];

    /*
     * uc_addr() may return pointers to these variables.  We need to
     * make sure they don't get written via ia64_put() or
     * ia64_putfp().  To make it possible to test for these variables
     * quickly, we collect them in a single sub-structure.
     */
    struct
      {
        unw_word_t  r0;                 /* r0 is byte-order neutral */
        unw_fpreg_t f0;                 /* f0 is byte-order neutral */
        unw_fpreg_t f1_le, f1_be;       /* f1 is byte-order dependent */
      }
    read_only;
    unw_fpreg_t nat_val_le, nat_val_be;
    unw_fpreg_t int_val_le, int_val_be;

    struct mempool reg_state_pool;
    struct mempool labeled_state_pool;

# if UNW_DEBUG
    const char *preg_name[IA64_NUM_PREGS];
# endif
  };

#define tdep_getcontext_trace           unw_getcontext
#define tdep_init_done                  unw.init_done
#define tdep_init                       UNW_OBJ(init)
/* Platforms that support UNW_INFO_FORMAT_TABLE need to define
   tdep_search_unwind_table.  */
#define tdep_search_unwind_table        unw_search_ia64_unwind_table
#define tdep_find_unwind_table  ia64_find_unwind_table
#define tdep_find_proc_info             UNW_OBJ(find_proc_info)
#define tdep_uc_addr                    UNW_OBJ(uc_addr)
#define tdep_get_elf_image              UNW_ARCH_OBJ(get_elf_image)
#define tdep_get_exe_image_path         UNW_ARCH_OBJ(get_exe_image_path)
#define tdep_access_reg                 UNW_OBJ(access_reg)
#define tdep_access_fpreg               UNW_OBJ(access_fpreg)
#define tdep_fetch_frame(c,ip,n)        do {} while(0)
#define tdep_cache_frame(c)             0
#define tdep_reuse_frame(c,frame)       do {} while(0)
#define tdep_stash_frame(c,rs)          do {} while(0)
#define tdep_trace(cur,addr,n)          (-UNW_ENOINFO)
#define tdep_get_as(c)                  ((c)->as)
#define tdep_get_as_arg(c)              ((c)->as_arg)
#define tdep_get_ip(c)                  ((c)->ip)
#define tdep_big_endian(as)             ((c)->as->big_endian != 0)

#ifndef UNW_LOCAL_ONLY
# define tdep_put_unwind_info           UNW_OBJ(put_unwind_info)
#endif

/* This can't be an UNW_ARCH_OBJ() because we need separate
   unw.initialized flags for the local-only and generic versions of
   the library.  Also, if we wanted to have a single, shared global
   data structure, we couldn't declare "unw" as HIDDEN/PROTECTED.  */
#define unw                             UNW_OBJ(data)

extern void tdep_init (void);
extern int tdep_find_unwind_table (struct elf_dyn_info *edi,
                                   unw_addr_space_t as, char *path,
                                   unw_word_t segbase, unw_word_t mapoff,
                                   unw_word_t ip);
extern int tdep_find_proc_info (unw_addr_space_t as, unw_word_t ip,
                                unw_proc_info_t *pi, int need_unwind_info,
                                void *arg);
extern void tdep_put_unwind_info (unw_addr_space_t as,
                                  unw_proc_info_t *pi, void *arg);
extern void *tdep_uc_addr (ucontext_t *uc, unw_regnum_t regnum,
                           uint8_t *nat_bitnr);
extern int tdep_get_elf_image (struct elf_image *ei, pid_t pid, unw_word_t ip,
                               unsigned long *segbase, unsigned long *mapoff,
                               char *path, size_t pathlen);
extern void tdep_get_exe_image_path (char *path);
extern int tdep_access_reg (struct cursor *c, unw_regnum_t reg,
                            unw_word_t *valp, int write);
extern int tdep_access_fpreg (struct cursor *c, unw_regnum_t reg,
                              unw_fpreg_t *valp, int write);

extern struct ia64_global_unwind_state unw;

/* In user-level, we have no reasonable way of determining the base of
   an arbitrary backing-store.  We default to half the
   address-space.  */
#define rbs_get_base(c,bspstore,rbs_basep)                              \
        (*(rbs_basep) = (bspstore) - (((unw_word_t) 1) << 63), 0)

#endif /* IA64_LIBUNWIND_I_H */
