/* libunwind - a platform-independent unwind library
   Copyright (C) 2006-2007 IBM
   Contributed by
     Corey Ashford <cjashfor@us.ibm.com>
     Jose Flavio Aguilar Paulino <jflavio@br.ibm.com> <joseflavio@gmail.com>

   Copied from libunwind-x86_64.h, modified slightly for building
   frysk successfully on ppc64, by Wu Zhou <woodzltc@cn.ibm.com>
   Will be replaced when libunwind is ready on ppc64 platform.

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

#ifndef PPC32_LIBUNWIND_I_H
#define PPC32_LIBUNWIND_I_H

/* Target-dependent definitions that are internal to libunwind but need
   to be shared with target-independent code.  */

#include <stdlib.h>
#include <libunwind.h>

#include "elf32.h"
#include "mempool.h"
#include "dwarf.h"

typedef struct
  {
    /* no ppc32-specific fast trace */
  }
unw_tdep_frame_t;

struct unw_addr_space
{
  struct unw_accessors acc;
  unw_caching_policy_t caching_policy;
#ifdef HAVE_ATOMIC_OPS_H
  AO_t cache_generation;
#else
  uint32_t cache_generation;
#endif
  unw_word_t dyn_generation;    /* see dyn-common.h */
  unw_word_t dyn_info_list_addr;        /* (cached) dyn_info_list_addr */
  struct dwarf_rs_cache global_cache;
  struct unw_debug_frame_list *debug_frames;
  int validate;
};

struct cursor
{
  struct dwarf_cursor dwarf;    /* must be first */

  /* Format of sigcontext structure and address at which it is
     stored: */
  enum
  {
    PPC_SCF_NONE,               /* no signal frame encountered */
    PPC_SCF_LINUX_RT_SIGFRAME   /* POSIX ucontext_t */
  }
  sigcontext_format;
  unw_word_t sigcontext_addr;
};

#define DWARF_GET_LOC(l)        ((l).val)

#ifdef UNW_LOCAL_ONLY
# define DWARF_NULL_LOC         DWARF_LOC (0, 0)
# define DWARF_IS_NULL_LOC(l)   (DWARF_GET_LOC (l) == 0)
# define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r) })
# define DWARF_IS_REG_LOC(l)    0
# define DWARF_IS_FP_LOC(l)     0
# define DWARF_IS_V_LOC(l)      0
# define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
# define DWARF_REG_LOC(c,r)     (DWARF_LOC((unw_word_t)                      \
                                 tdep_uc_addr((c)->as_arg, (r)), 0))
# define DWARF_FPREG_LOC(c,r)   (DWARF_LOC((unw_word_t)                      \
                                 tdep_uc_addr((c)->as_arg, (r)), 0))
# define DWARF_VREG_LOC(c,r)    (DWARF_LOC((unw_word_t)                      \
                                 tdep_uc_addr((c)->as_arg, (r)), 0))
#else /* !UNW_LOCAL_ONLY */

# define DWARF_LOC_TYPE_FP      (1 << 0)
# define DWARF_LOC_TYPE_REG     (1 << 1)
# define DWARF_LOC_TYPE_V       (1 << 2)
# define DWARF_NULL_LOC         DWARF_LOC (0, 0)
# define DWARF_IS_NULL_LOC(l)                                           \
                ({ dwarf_loc_t _l = (l); _l.val == 0 && _l.type == 0; })
# define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r), .type = (t) })
# define DWARF_IS_REG_LOC(l)    (((l).type & DWARF_LOC_TYPE_REG) != 0)
# define DWARF_IS_FP_LOC(l)     (((l).type & DWARF_LOC_TYPE_FP) != 0)
# define DWARF_IS_V_LOC(l)      (((l).type & DWARF_LOC_TYPE_V) != 0)
# define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
# define DWARF_REG_LOC(c,r)     DWARF_LOC((r), DWARF_LOC_TYPE_REG)
# define DWARF_FPREG_LOC(c,r)   DWARF_LOC((r), (DWARF_LOC_TYPE_REG      \
                                                | DWARF_LOC_TYPE_FP))
# define DWARF_VREG_LOC(c,r)    DWARF_LOC((r), (DWARF_LOC_TYPE_REG      \
                                                | DWARF_LOC_TYPE_V))

#endif /* !UNW_LOCAL_ONLY */

static inline int
dwarf_getvr (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t * val)
{
  unw_word_t *valp = (unw_word_t *) val;
  unw_word_t addr;
  int ret;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  assert (DWARF_IS_V_LOC (loc));
  assert (!DWARF_IS_FP_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                      val, 0, c->as_arg);

  addr = DWARF_GET_LOC (loc);

  if ((ret = (*c->as->acc.access_mem) (c->as, addr + 0, valp,
                                       0, c->as_arg)) < 0)
    return ret;

  return (*c->as->acc.access_mem) (c->as, addr + 8, valp + 1, 0, c->as_arg);
}

static inline int
dwarf_putvr (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  unw_word_t *valp = (unw_word_t *) & val;
  unw_word_t addr;
  int ret;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  assert (DWARF_IS_V_LOC (loc));
  assert (!DWARF_IS_FP_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                      &val, 1, c->as_arg);

  addr = DWARF_GET_LOC (loc);
  if ((ret = (*c->as->acc.access_mem) (c->as, addr + 0, valp,
                                       1, c->as_arg)) < 0)
    return ret;

  return (*c->as->acc.access_mem) (c->as, addr + 8, valp + 1, 1, c->as_arg);
}

static inline int
dwarf_getfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t * val)
{
  unw_word_t *valp = (unw_word_t *) val;
  unw_word_t addr;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  assert (DWARF_IS_FP_LOC (loc));
  assert (!DWARF_IS_V_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                       val, 0, c->as_arg);

  addr = DWARF_GET_LOC (loc);
  return (*c->as->acc.access_mem) (c->as, addr + 0, valp, 0, c->as_arg);

}

static inline int
dwarf_putfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  unw_word_t *valp = (unw_word_t *) & val;
  unw_word_t addr;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  assert (DWARF_IS_FP_LOC (loc));
  assert (!DWARF_IS_V_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                       &val, 1, c->as_arg);

  addr = DWARF_GET_LOC (loc);

  return (*c->as->acc.access_mem) (c->as, addr + 0, valp, 1, c->as_arg);
}

static inline int
dwarf_get (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t * val)
{
  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  /* If a code-generator were to save a value of type unw_word_t in a
     floating-point register, we would have to support this case.  I
     suppose it could happen with MMX registers, but does it really
     happen?  */
  assert (!DWARF_IS_FP_LOC (loc));
  assert (!DWARF_IS_V_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_reg) (c->as, DWARF_GET_LOC (loc), val,
                                     0, c->as_arg);
  else
    return (*c->as->acc.access_mem) (c->as, DWARF_GET_LOC (loc), val,
                                     0, c->as_arg);
}

static inline int
dwarf_put (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t val)
{
  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  /* If a code-generator were to save a value of type unw_word_t in a
     floating-point register, we would have to support this case.  I
     suppose it could happen with MMX registers, but does it really
     happen?  */
  assert (!DWARF_IS_FP_LOC (loc));
  assert (!DWARF_IS_V_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_reg) (c->as, DWARF_GET_LOC (loc), &val,
                                     1, c->as_arg);
  else
    return (*c->as->acc.access_mem) (c->as, DWARF_GET_LOC (loc), &val,
                                     1, c->as_arg);
}

#define tdep_getcontext_trace           unw_getcontext
#define tdep_init_done                  UNW_OBJ(init_done)
#define tdep_init                       UNW_OBJ(init)
/* Platforms that support UNW_INFO_FORMAT_TABLE need to define
   tdep_search_unwind_table.  */
#define tdep_search_unwind_table        dwarf_search_unwind_table
#define tdep_find_unwind_table          dwarf_find_unwind_table
#define tdep_uc_addr                    UNW_ARCH_OBJ(uc_addr)
#define tdep_get_elf_image              UNW_ARCH_OBJ(get_elf_image)
#define tdep_get_exe_image_path         UNW_ARCH_OBJ(get_exe_image_path)
#define tdep_access_reg                 UNW_OBJ(access_reg)
#define tdep_access_fpreg               UNW_OBJ(access_fpreg)
#define tdep_fetch_frame(c,ip,n)        do {} while(0)
#define tdep_cache_frame(c)             0
#define tdep_reuse_frame(c,frame)       do {} while(0)
#define tdep_stash_frame(c,rs)          do {} while(0)
#define tdep_trace(cur,addr,n)          (-UNW_ENOINFO)
#define tdep_get_func_addr              UNW_OBJ(get_func_addr)

#ifdef UNW_LOCAL_ONLY
# define tdep_find_proc_info(c,ip,n)                            \
        dwarf_find_proc_info((c)->as, (ip), &(c)->pi, (n),      \
                                       (c)->as_arg)
# define tdep_put_unwind_info(as,pi,arg)                \
        dwarf_put_unwind_info((as), (pi), (arg))
#else
# define tdep_find_proc_info(c,ip,n)                                    \
        (*(c)->as->acc.find_proc_info)((c)->as, (ip), &(c)->pi, (n),    \
                                       (c)->as_arg)
# define tdep_put_unwind_info(as,pi,arg)                        \
        (*(as)->acc.put_unwind_info)((as), (pi), (arg))
#endif

extern int tdep_fetch_proc_info_post (struct dwarf_cursor *c, unw_word_t ip,
                                      int need_unwind_info);

#define tdep_get_as(c)                  ((c)->dwarf.as)
#define tdep_get_as_arg(c)              ((c)->dwarf.as_arg)
#define tdep_get_ip(c)                  ((c)->dwarf.ip)
#define tdep_big_endian(as)             1

extern int tdep_init_done;

extern void tdep_init (void);
extern int tdep_search_unwind_table (unw_addr_space_t as, unw_word_t ip,
                                     unw_dyn_info_t * di,
                                     unw_proc_info_t * pi,
                                     int need_unwind_info, void *arg);
extern void *tdep_uc_addr (ucontext_t * uc, int reg);
extern int tdep_get_elf_image (struct elf_image *ei, pid_t pid, unw_word_t ip,
                               unsigned long *segbase, unsigned long *mapoff,
                               char *path, size_t pathlen);
extern void tdep_get_exe_image_path (char *path);
extern int tdep_access_reg (struct cursor *c, unw_regnum_t reg,
                            unw_word_t * valp, int write);
extern int tdep_access_fpreg (struct cursor *c, unw_regnum_t reg,
                              unw_fpreg_t * valp, int write);
extern int tdep_get_func_addr (unw_addr_space_t as, unw_word_t addr,
                               unw_word_t *entry_point);

#endif /* PPC64_LIBUNWIND_I_H */
