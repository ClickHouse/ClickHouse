/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery

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

#ifndef MIPS_LIBUNWIND_I_H
#define MIPS_LIBUNWIND_I_H

/* Target-dependent definitions that are internal to libunwind but need
   to be shared with target-independent code.  */

#include <stdlib.h>
#include <libunwind.h>

#if !defined(UNW_REMOTE_ONLY) && _MIPS_SIM == _ABI64
# include "elf64.h"
#else
# include "elf32.h"
#endif
#include "mempool.h"
#include "dwarf.h"

typedef struct
  {
    /* no mips-specific fast trace */
  }
unw_tdep_frame_t;

struct unw_addr_space
  {
    struct unw_accessors acc;

    int big_endian;
    mips_abi_t abi;
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

#define tdep_big_endian(as)             ((as)->big_endian)

struct cursor
  {
    struct dwarf_cursor dwarf;          /* must be first */
    unw_word_t sigcontext_addr;
  };

#define DWARF_GET_LOC(l)        ((l).val)

#ifndef UNW_REMOTE_ONLY
# if _MIPS_SIM == _ABIN32
typedef long long mips_reg_t;
# else
typedef long mips_reg_t;
# endif
#endif

#ifdef UNW_LOCAL_ONLY
# define DWARF_NULL_LOC         DWARF_LOC (0, 0)
# define DWARF_IS_NULL_LOC(l)   (DWARF_GET_LOC (l) == 0)
# define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r) })
# define DWARF_IS_REG_LOC(l)    0
# define DWARF_REG_LOC(c,r)     (DWARF_LOC((unw_word_t) (intptr_t)           \
                                 tdep_uc_addr((c)->as_arg, (r)), 0))
# define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
# define DWARF_FPREG_LOC(c,r)   (DWARF_LOC((unw_word_t) (intptr_t)           \
                                 tdep_uc_addr((c)->as_arg, (r)), 0))

/* FIXME: Implement these for the MIPS FPU.  */
static inline int
dwarf_getfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t *val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;
  *val = *(unw_fpreg_t *) (intptr_t) DWARF_GET_LOC (loc);
  return 0;
}

static inline int
dwarf_putfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;
  *(unw_fpreg_t *) (intptr_t) DWARF_GET_LOC (loc) = val;
  return 0;
}

static inline int
dwarf_get (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t *val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;
  *val = *(mips_reg_t *) (intptr_t) DWARF_GET_LOC (loc);
  return 0;
}

static inline int
dwarf_put (struct dwarf_cursor *c, dwarf_loc_t loc, unw_word_t val)
{
  if (!DWARF_GET_LOC (loc))
    return -1;
  *(mips_reg_t *) (intptr_t) DWARF_GET_LOC (loc) = val;
  return 0;
}

#else /* !UNW_LOCAL_ONLY */
# define DWARF_LOC_TYPE_FP      (1 << 0)
# define DWARF_LOC_TYPE_REG     (1 << 1)
# define DWARF_NULL_LOC         DWARF_LOC (0, 0)
# define DWARF_IS_NULL_LOC(l)                                           \
                ({ dwarf_loc_t _l = (l); _l.val == 0 && _l.type == 0; })
# define DWARF_LOC(r, t)        ((dwarf_loc_t) { .val = (r), .type = (t) })
# define DWARF_IS_REG_LOC(l)    (((l).type & DWARF_LOC_TYPE_REG) != 0)
# define DWARF_IS_FP_LOC(l)     (((l).type & DWARF_LOC_TYPE_FP) != 0)
# define DWARF_REG_LOC(c,r)     DWARF_LOC((r), DWARF_LOC_TYPE_REG)
# define DWARF_MEM_LOC(c,m)     DWARF_LOC ((m), 0)
# define DWARF_FPREG_LOC(c,r)   DWARF_LOC((r), (DWARF_LOC_TYPE_REG      \
                                                | DWARF_LOC_TYPE_FP))

static inline int
read_s32 (struct dwarf_cursor *c, unw_word_t addr, unw_word_t *val)
{
  int offset = addr & 4;
  int ret;
  unw_word_t memval;

  ret = (*c->as->acc.access_mem) (c->as, addr - offset, &memval, 0, c->as_arg);
  if (ret < 0)
    return ret;

  if ((offset != 0) == tdep_big_endian (c->as))
    *val = (int32_t) memval;
  else
    *val = (int32_t) (memval >> 32);

  return 0;
}

static inline int
write_s32 (struct dwarf_cursor *c, unw_word_t addr, const unw_word_t *val)
{
  int offset = addr & 4;
  int ret;
  unw_word_t memval;

  ret = (*c->as->acc.access_mem) (c->as, addr - offset, &memval, 0, c->as_arg);
  if (ret < 0)
    return ret;

  if ((offset != 0) == tdep_big_endian (c->as))
    memval = (memval & ~0xffffffffLL) | (uint32_t) *val;
  else
    memval = (memval & 0xffffffffLL) | (uint32_t) (*val << 32);

  return (*c->as->acc.access_mem) (c->as, addr - offset, &memval, 1, c->as_arg);
}

/* FIXME: Implement these for the MIPS FPU.  */
static inline int
dwarf_getfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t *val)
{
  char *valp = (char *) &val;
  unw_word_t addr;
  int ret;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                       val, 0, c->as_arg);

  addr = DWARF_GET_LOC (loc);
  if ((ret = (*c->as->acc.access_mem) (c->as, addr + 0, (unw_word_t *) valp,
                                       0, c->as_arg)) < 0)
    return ret;

  return (*c->as->acc.access_mem) (c->as, addr + 4, (unw_word_t *) valp + 1, 0,
                                   c->as_arg);
}

static inline int
dwarf_putfp (struct dwarf_cursor *c, dwarf_loc_t loc, unw_fpreg_t val)
{
  char *valp = (char *) &val;
  unw_word_t addr;
  int ret;

  if (DWARF_IS_NULL_LOC (loc))
    return -UNW_EBADREG;

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_fpreg) (c->as, DWARF_GET_LOC (loc),
                                       &val, 1, c->as_arg);

  addr = DWARF_GET_LOC (loc);
  if ((ret = (*c->as->acc.access_mem) (c->as, addr + 0, (unw_word_t *) valp,
                                       1, c->as_arg)) < 0)
    return ret;

  return (*c->as->acc.access_mem) (c->as, addr + 4, (unw_word_t *) valp + 1,
                                   1, c->as_arg);
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
  else if (c->as->abi == UNW_MIPS_ABI_O32)
    return read_s32 (c, DWARF_GET_LOC (loc), val);
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

  if (DWARF_IS_REG_LOC (loc))
    return (*c->as->acc.access_reg) (c->as, DWARF_GET_LOC (loc), &val,
                                     1, c->as_arg);
  else if (c->as->abi == UNW_MIPS_ABI_O32)
    return write_s32 (c, DWARF_GET_LOC (loc), &val);
  else
    return (*c->as->acc.access_mem) (c->as, DWARF_GET_LOC (loc), &val,
                                     1, c->as_arg);
}

#endif /* !UNW_LOCAL_ONLY */

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
# define tdep_put_unwind_info(as,pi,arg)                \
        (*(as)->acc.put_unwind_info)((as), (pi), (arg))
#endif

#define tdep_get_as(c)                  ((c)->dwarf.as)
#define tdep_get_as_arg(c)              ((c)->dwarf.as_arg)
#define tdep_get_ip(c)                  ((c)->dwarf.ip)

extern int tdep_init_done;

extern void tdep_init (void);
extern int tdep_search_unwind_table (unw_addr_space_t as, unw_word_t ip,
                                     unw_dyn_info_t *di, unw_proc_info_t *pi,
                                     int need_unwind_info, void *arg);
extern void *tdep_uc_addr (ucontext_t *uc, int reg);
extern int tdep_get_elf_image (struct elf_image *ei, pid_t pid, unw_word_t ip,
                               unsigned long *segbase, unsigned long *mapoff,
                               char *path, size_t pathlen);
extern void tdep_get_exe_image_path (char *path);
extern int tdep_access_reg (struct cursor *c, unw_regnum_t reg,
                            unw_word_t *valp, int write);
extern int tdep_access_fpreg (struct cursor *c, unw_regnum_t reg,
                              unw_fpreg_t *valp, int write);

#endif /* MIPS_LIBUNWIND_I_H */
