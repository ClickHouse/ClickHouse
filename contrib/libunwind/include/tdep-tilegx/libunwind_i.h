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

#ifndef TILEGX_LIBUNWIND_I_H
#define TILEGX_LIBUNWIND_I_H

/* Target-dependent definitions that are internal to libunwind but need
   to be shared with target-independent code.  */

#include <stdlib.h>
#include <libunwind.h>

# include "elf64.h"
#include "mempool.h"
#include "dwarf.h"

typedef struct
{
  /* no Tilegx-specific fast trace */
} unw_tdep_frame_t;

struct unw_addr_space
{
  struct unw_accessors acc;

  int big_endian;
  tilegx_abi_t abi;
  unsigned int addr_size;

  unw_caching_policy_t caching_policy;
#ifdef HAVE_ATOMIC_OPS_H
  AO_t cache_generation;
#else
  uint32_t cache_generation;
#endif
  unw_word_t dyn_generation;          /* see dyn-common.h */
  unw_word_t dyn_info_list_addr;      /* (cached) dyn_info_list_addr */
  struct dwarf_rs_cache global_cache;
  struct unw_debug_frame_list *debug_frames;
};

#define tdep_big_endian(as)            ((as)->big_endian)

struct cursor
{
  struct dwarf_cursor dwarf;         /* must be first */
  unw_word_t sigcontext_addr;
  unw_word_t sigcontext_sp;
  unw_word_t sigcontext_pc;
};

#define DWARF_GET_LOC(l)      ((l).val)

#ifndef UNW_REMOTE_ONLY
typedef long tilegx_reg_t;
#endif

#ifdef UNW_LOCAL_ONLY
#define DWARF_NULL_LOC         DWARF_LOC (0, 0)
#define DWARF_IS_NULL_LOC(l)   (DWARF_GET_LOC (l) == 0)
#define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r) })
#define DWARF_IS_REG_LOC(l)    0
#define DWARF_REG_LOC(c,r)     (DWARF_LOC((unw_word_t) (intptr_t)       \
                                          tdep_uc_addr((c)->as_arg, (r)), 0))
#define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
#define DWARF_FPREG_LOC(c,r)   (DWARF_LOC((unw_word_t) (intptr_t)       \
                                          tdep_uc_addr((c)->as_arg, (r)), 0))

/* Tilegx has no FP. */
static inline int
dwarf_getfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t *val)
{
  Debug (1, "Tielgx has no fp!\n");
  abort();
  return 0;
}

static inline int
dwarf_putfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  Debug (1, "Tielgx has no fp!\n");
  abort();
  return 0;
}

static inline int
dwarf_get (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t *val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;

  *val = *(tilegx_reg_t *) (intptr_t) DWARF_GET_LOC (loc);
  return 0;
}

static inline int
dwarf_put (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;

  *(tilegx_reg_t *) (intptr_t) DWARF_GET_LOC (loc) = val;
  return 0;
}

#else /* !UNW_LOCAL_ONLY */
#define DWARF_LOC_TYPE_FP      (1 << 0)
#define DWARF_LOC_TYPE_REG     (1 << 1)
#define DWARF_NULL_LOC         DWARF_LOC (0, 0)
#define DWARF_IS_NULL_LOC(l)                                    \
  ({ dwarf_loc_t _l = (l); _l.val == 0 && _l.type == 0; })
#define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r), .type = (t) })
#define DWARF_IS_REG_LOC(l)    (((l).type & DWARF_LOC_TYPE_REG) != 0)
#define DWARF_IS_FP_LOC(l)     (((l).type & DWARF_LOC_TYPE_FP) != 0)
#define DWARF_REG_LOC(c,r)     DWARF_LOC((r), DWARF_LOC_TYPE_REG)
#define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
#define DWARF_FPREG_LOC(c,r)   DWARF_LOC((r), (DWARF_LOC_TYPE_REG       \
                                               | DWARF_LOC_TYPE_FP))

/* TILEGX has no fp. */
static inline int
dwarf_getfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t *val)
{
  Debug (1, "Tielgx has no fp!\n");
  abort();
  return 0;
}

static inline int
dwarf_putfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  Debug (1, "Tielgx has no fp!\n");
  abort();
  return 0;
}

static inline int
dwarf_get (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t *val)
{
  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  /* If a code-generator were to save a value of type unw_word_t in a
     floating-point register, we would have to support this case.  I
     suppose it could happen with MMX registers, but does it really
     happen?  */
  assert (!DWARF_IS_FP_LOC (loc));

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_reg) (c->as, DWARF_GET_LOC (loc), val,
                                     0, c->as_arg);

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

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_reg) (c->as, DWARF_GET_LOC (loc), &val,
                                     1, c->as_arg);

  return (*c->as->acc.access_mem) (c->as, DWARF_GET_LOC (loc), &val,
                                   1, c->as_arg);
}

#endif /* !UNW_LOCAL_ONLY */

#define tdep_getcontext_trace           unw_getcontext
#define tdep_init_done                  UNW_OBJ(init_done)
#define tdep_needs_initialization       UNW_OBJ(needs_initialization)
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

#ifdef UNW_LOCAL_ONLY
#define tdep_find_proc_info(c,ip,n)                            \
  dwarf_find_proc_info((c)->as, (ip), &(c)->pi, (n),           \
                       (c)->as_arg)
#define tdep_put_unwind_info(as,pi,arg)         \
  dwarf_put_unwind_info((as), (pi), (arg))
#else
#define tdep_find_proc_info(c,ip,n)                                     \
  (*(c)->as->acc.find_proc_info)((c)->as, (ip), &(c)->pi, (n),          \
                                 (c)->as_arg)
#define tdep_put_unwind_info(as,pi,arg)                 \
  (*(as)->acc.put_unwind_info)((as), (pi), (arg))
#endif

#define tdep_get_as(c)                  ((c)->dwarf.as)
#define tdep_get_as_arg(c)              ((c)->dwarf.as_arg)
#define tdep_get_ip(c)                  ((c)->dwarf.ip)

extern int tdep_init_done;

extern void tdep_init (void);
extern int tdep_search_unwind_table (unw_addr_space_t as,
                                     unw_word_t ip,
                                     unw_dyn_info_t *di,
                                     unw_proc_info_t *pi,
                                     int need_unwind_info,
                                     void *arg);
extern void *tdep_uc_addr (ucontext_t *uc, int reg);
extern int tdep_get_elf_image (struct elf_image *ei,
                               pid_t pid, unw_word_t ip,
                               unsigned long *segbase,
                               unsigned long *mapoff,
                               char *path, size_t pathlen);
extern void tdep_get_exe_image_path (char *path);
extern int tdep_access_reg (struct cursor *c,
                            unw_regnum_t reg,
                            unw_word_t *valp,
                            int write);
extern int tdep_access_fpreg (struct cursor *c,
                              unw_regnum_t reg,
                              unw_fpreg_t *valp,
                              int write);

#endif /* TILEGX_LIBUNWIND_I_H */
