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

#include "unwind_i.h"

/* forward declaration: */
static int create_state_record_for (struct cursor *c,
                                    struct ia64_state_record *sr,
                                    unw_word_t ip);

typedef unsigned long unw_word;

#define alloc_reg_state()       (mempool_alloc (&unw.reg_state_pool))
#define free_reg_state(rs)      (mempool_free (&unw.reg_state_pool, rs))
#define alloc_labeled_state()   (mempool_alloc (&unw.labeled_state_pool))
#define free_labeled_state(s)   (mempool_free (&unw.labeled_state_pool, s))

/* Routines to manipulate the state stack.  */

static inline void
push (struct ia64_state_record *sr)
{
  struct ia64_reg_state *rs;

  rs = alloc_reg_state ();
  if (!rs)
    {
      print_error ("libunwind: cannot stack reg state!\n");
      return;
    }
  memcpy (rs, &sr->curr, sizeof (*rs));
  sr->curr.next = rs;
}

static void
pop (struct ia64_state_record *sr)
{
  struct ia64_reg_state *rs = sr->curr.next;

  if (!rs)
    {
      print_error ("libunwind: stack underflow!\n");
      return;
    }
  memcpy (&sr->curr, rs, sizeof (*rs));
  free_reg_state (rs);
}

/* Make a copy of the state stack.  Non-recursive to avoid stack overflows.  */
static struct ia64_reg_state *
dup_state_stack (struct ia64_reg_state *rs)
{
  struct ia64_reg_state *copy, *prev = NULL, *first = NULL;

  while (rs)
    {
      copy = alloc_reg_state ();
      if (!copy)
        {
          print_error ("unwind.dup_state_stack: out of memory\n");
          return NULL;
        }
      memcpy (copy, rs, sizeof (*copy));
      if (first)
        prev->next = copy;
      else
        first = copy;
      rs = rs->next;
      prev = copy;
    }
  return first;
}

/* Free all stacked register states (but not RS itself).  */
static void
free_state_stack (struct ia64_reg_state *rs)
{
  struct ia64_reg_state *p, *next;

  for (p = rs->next; p != NULL; p = next)
    {
      next = p->next;
      free_reg_state (p);
    }
  rs->next = NULL;
}

/* Unwind decoder routines */

static enum ia64_pregnum CONST_ATTR
decode_abreg (unsigned char abreg, int memory)
{
  switch (abreg)
    {
    case 0x04 ... 0x07:
      return IA64_REG_R4 + (abreg - 0x04);
    case 0x22 ... 0x25:
      return IA64_REG_F2 + (abreg - 0x22);
    case 0x30 ... 0x3f:
      return IA64_REG_F16 + (abreg - 0x30);
    case 0x41 ... 0x45:
      return IA64_REG_B1 + (abreg - 0x41);
    case 0x60:
      return IA64_REG_PR;
    case 0x61:
      return IA64_REG_PSP;
    case 0x62:
      return memory ? IA64_REG_PRI_UNAT_MEM : IA64_REG_PRI_UNAT_GR;
    case 0x63:
      return IA64_REG_IP;
    case 0x64:
      return IA64_REG_BSP;
    case 0x65:
      return IA64_REG_BSPSTORE;
    case 0x66:
      return IA64_REG_RNAT;
    case 0x67:
      return IA64_REG_UNAT;
    case 0x68:
      return IA64_REG_FPSR;
    case 0x69:
      return IA64_REG_PFS;
    case 0x6a:
      return IA64_REG_LC;
    default:
      break;
    }
  Dprintf ("libunwind: bad abreg=0x%x\n", abreg);
  return IA64_REG_LC;
}

static void
set_reg (struct ia64_reg_info *reg, enum ia64_where where, int when,
         unsigned long val)
{
  reg->val = val;
  reg->where = where;
  if (reg->when == IA64_WHEN_NEVER)
    reg->when = when;
}

static void
alloc_spill_area (unsigned long *offp, unsigned long regsize,
                  struct ia64_reg_info *lo, struct ia64_reg_info *hi)
{
  struct ia64_reg_info *reg;

  for (reg = hi; reg >= lo; --reg)
    {
      if (reg->where == IA64_WHERE_SPILL_HOME)
        {
          reg->where = IA64_WHERE_PSPREL;
          *offp -= regsize;
          reg->val = *offp;
        }
    }
}

static inline void
spill_next_when (struct ia64_reg_info **regp, struct ia64_reg_info *lim,
                 unw_word t)
{
  struct ia64_reg_info *reg;

  for (reg = *regp; reg <= lim; ++reg)
    {
      if (reg->where == IA64_WHERE_SPILL_HOME)
        {
          reg->when = t;
          *regp = reg + 1;
          return;
        }
    }
  Dprintf ("libunwind: excess spill!\n");
}

static inline void
finish_prologue (struct ia64_state_record *sr)
{
  struct ia64_reg_info *reg;
  unsigned long off;
  int i;

  /* First, resolve implicit register save locations (see Section
     "11.4.2.3 Rules for Using Unwind Descriptors", rule 3). */
  for (i = 0; i < (int) ARRAY_SIZE (unw.save_order); ++i)
    {
      reg = sr->curr.reg + unw.save_order[i];
      if (reg->where == IA64_WHERE_GR_SAVE)
        {
          reg->where = IA64_WHERE_GR;
          reg->val = sr->gr_save_loc++;
        }
    }

  /* Next, compute when the fp, general, and branch registers get
     saved.  This must come before alloc_spill_area() because we need
     to know which registers are spilled to their home locations.  */

  if (sr->imask)
    {
      unsigned char kind, mask = 0, *cp = sr->imask;
      unsigned long t;
      static const unsigned char limit[3] =
        {
          IA64_REG_F31, IA64_REG_R7, IA64_REG_B5
        };
      struct ia64_reg_info *(regs[3]);

      regs[0] = sr->curr.reg + IA64_REG_F2;
      regs[1] = sr->curr.reg + IA64_REG_R4;
      regs[2] = sr->curr.reg + IA64_REG_B1;

      for (t = 0; (int) t < sr->region_len; ++t)
        {
          if ((t & 3) == 0)
            mask = *cp++;
          kind = (mask >> 2 * (3 - (t & 3))) & 3;
          if (kind > 0)
            spill_next_when (&regs[kind - 1], sr->curr.reg + limit[kind - 1],
                             sr->region_start + t);
        }
    }

  /* Next, lay out the memory stack spill area.  */

  if (sr->any_spills)
    {
      off = sr->spill_offset;
      alloc_spill_area (&off, 16, sr->curr.reg + IA64_REG_F2,
                        sr->curr.reg + IA64_REG_F31);
      alloc_spill_area (&off, 8, sr->curr.reg + IA64_REG_B1,
                        sr->curr.reg + IA64_REG_B5);
      alloc_spill_area (&off, 8, sr->curr.reg + IA64_REG_R4,
                        sr->curr.reg + IA64_REG_R7);
    }
}

/* Region header descriptors.  */

static void
desc_prologue (int body, unw_word rlen, unsigned char mask,
               unsigned char grsave, struct ia64_state_record *sr)
{
  int i, region_start;

  if (!(sr->in_body || sr->first_region))
    finish_prologue (sr);
  sr->first_region = 0;

  /* check if we're done: */
  if (sr->when_target < sr->region_start + sr->region_len)
    {
      sr->done = 1;
      return;
    }

  region_start = sr->region_start + sr->region_len;

  for (i = 0; i < sr->epilogue_count; ++i)
    pop (sr);
  sr->epilogue_count = 0;
  sr->when_sp_restored = IA64_WHEN_NEVER;

  sr->region_start = region_start;
  sr->region_len = rlen;
  sr->in_body = body;

  if (!body)
    {
      push (sr);

      if (mask)
        for (i = 0; i < 4; ++i)
          {
            if (mask & 0x8)
              set_reg (sr->curr.reg + unw.save_order[i], IA64_WHERE_GR,
                       sr->region_start + sr->region_len - 1, grsave++);
            mask <<= 1;
          }
      sr->gr_save_loc = grsave;
      sr->any_spills = 0;
      sr->imask = 0;
      sr->spill_offset = 0x10;  /* default to psp+16 */
    }
}

/* Prologue descriptors.  */

static inline void
desc_abi (unsigned char abi, unsigned char context,
          struct ia64_state_record *sr)
{
  sr->abi_marker = (abi << 8) | context;
}

static inline void
desc_br_gr (unsigned char brmask, unsigned char gr,
            struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 5; ++i)
    {
      if (brmask & 1)
        set_reg (sr->curr.reg + IA64_REG_B1 + i, IA64_WHERE_GR,
                 sr->region_start + sr->region_len - 1, gr++);
      brmask >>= 1;
    }
}

static inline void
desc_br_mem (unsigned char brmask, struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 5; ++i)
    {
      if (brmask & 1)
        {
          set_reg (sr->curr.reg + IA64_REG_B1 + i, IA64_WHERE_SPILL_HOME,
                   sr->region_start + sr->region_len - 1, 0);
          sr->any_spills = 1;
        }
      brmask >>= 1;
    }
}

static inline void
desc_frgr_mem (unsigned char grmask, unw_word frmask,
               struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 4; ++i)
    {
      if ((grmask & 1) != 0)
        {
          set_reg (sr->curr.reg + IA64_REG_R4 + i, IA64_WHERE_SPILL_HOME,
                   sr->region_start + sr->region_len - 1, 0);
          sr->any_spills = 1;
        }
      grmask >>= 1;
    }
  for (i = 0; i < 20; ++i)
    {
      if ((frmask & 1) != 0)
        {
          int base = (i < 4) ? IA64_REG_F2 : IA64_REG_F16 - 4;
          set_reg (sr->curr.reg + base + i, IA64_WHERE_SPILL_HOME,
                   sr->region_start + sr->region_len - 1, 0);
          sr->any_spills = 1;
        }
      frmask >>= 1;
    }
}

static inline void
desc_fr_mem (unsigned char frmask, struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 4; ++i)
    {
      if ((frmask & 1) != 0)
        {
          set_reg (sr->curr.reg + IA64_REG_F2 + i, IA64_WHERE_SPILL_HOME,
                   sr->region_start + sr->region_len - 1, 0);
          sr->any_spills = 1;
        }
      frmask >>= 1;
    }
}

static inline void
desc_gr_gr (unsigned char grmask, unsigned char gr,
            struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 4; ++i)
    {
      if ((grmask & 1) != 0)
        set_reg (sr->curr.reg + IA64_REG_R4 + i, IA64_WHERE_GR,
                 sr->region_start + sr->region_len - 1, gr++);
      grmask >>= 1;
    }
}

static inline void
desc_gr_mem (unsigned char grmask, struct ia64_state_record *sr)
{
  int i;

  for (i = 0; i < 4; ++i)
    {
      if ((grmask & 1) != 0)
        {
          set_reg (sr->curr.reg + IA64_REG_R4 + i, IA64_WHERE_SPILL_HOME,
                   sr->region_start + sr->region_len - 1, 0);
          sr->any_spills = 1;
        }
      grmask >>= 1;
    }
}

static inline void
desc_mem_stack_f (unw_word t, unw_word size, struct ia64_state_record *sr)
{
  set_reg (sr->curr.reg + IA64_REG_PSP, IA64_WHERE_NONE,
           sr->region_start + MIN ((int) t, sr->region_len - 1), 16 * size);
}

static inline void
desc_mem_stack_v (unw_word t, struct ia64_state_record *sr)
{
  sr->curr.reg[IA64_REG_PSP].when =
    sr->region_start + MIN ((int) t, sr->region_len - 1);
}

static inline void
desc_reg_gr (unsigned char reg, unsigned char dst,
             struct ia64_state_record *sr)
{
  set_reg (sr->curr.reg + reg, IA64_WHERE_GR,
           sr->region_start + sr->region_len - 1, dst);
}

static inline void
desc_reg_psprel (unsigned char reg, unw_word pspoff,
                 struct ia64_state_record *sr)
{
  set_reg (sr->curr.reg + reg, IA64_WHERE_PSPREL,
           sr->region_start + sr->region_len - 1, 0x10 - 4 * pspoff);
}

static inline void
desc_reg_sprel (unsigned char reg, unw_word spoff,
                struct ia64_state_record *sr)
{
  set_reg (sr->curr.reg + reg, IA64_WHERE_SPREL,
           sr->region_start + sr->region_len - 1, 4 * spoff);
}

static inline void
desc_rp_br (unsigned char dst, struct ia64_state_record *sr)
{
  sr->return_link_reg = dst;
}

static inline void
desc_reg_when (unsigned char regnum, unw_word t, struct ia64_state_record *sr)
{
  struct ia64_reg_info *reg = sr->curr.reg + regnum;

  if (reg->where == IA64_WHERE_NONE)
    reg->where = IA64_WHERE_GR_SAVE;
  reg->when = sr->region_start + MIN ((int) t, sr->region_len - 1);
}

static inline void
desc_spill_base (unw_word pspoff, struct ia64_state_record *sr)
{
  sr->spill_offset = 0x10 - 4 * pspoff;
}

static inline unsigned char *
desc_spill_mask (unsigned char *imaskp, struct ia64_state_record *sr)
{
  sr->imask = imaskp;
  return imaskp + (2 * sr->region_len + 7) / 8;
}

/* Body descriptors.  */

static inline void
desc_epilogue (unw_word t, unw_word ecount, struct ia64_state_record *sr)
{
  sr->when_sp_restored = sr->region_start + sr->region_len - 1 - t;
  sr->epilogue_count = ecount + 1;
}

static inline void
desc_copy_state (unw_word label, struct ia64_state_record *sr)
{
  struct ia64_labeled_state *ls;

  for (ls = sr->labeled_states; ls; ls = ls->next)
    {
      if (ls->label == label)
        {
          free_state_stack (&sr->curr);
          memcpy (&sr->curr, &ls->saved_state, sizeof (sr->curr));
          sr->curr.next = dup_state_stack (ls->saved_state.next);
          return;
        }
    }
  print_error ("libunwind: failed to find labeled state\n");
}

static inline void
desc_label_state (unw_word label, struct ia64_state_record *sr)
{
  struct ia64_labeled_state *ls;

  ls = alloc_labeled_state ();
  if (!ls)
    {
      print_error ("unwind.desc_label_state(): out of memory\n");
      return;
    }
  ls->label = label;
  memcpy (&ls->saved_state, &sr->curr, sizeof (ls->saved_state));
  ls->saved_state.next = dup_state_stack (sr->curr.next);

  /* insert into list of labeled states: */
  ls->next = sr->labeled_states;
  sr->labeled_states = ls;
}

/* General descriptors.  */

static inline int
desc_is_active (unsigned char qp, unw_word t, struct ia64_state_record *sr)
{
  if (sr->when_target <= sr->region_start + MIN ((int) t, sr->region_len - 1))
    return 0;
  if (qp > 0)
    {
      if ((sr->pr_val & ((unw_word_t) 1 << qp)) == 0)
        return 0;
      sr->pr_mask |= ((unw_word_t) 1 << qp);
    }
  return 1;
}

static inline void
desc_restore_p (unsigned char qp, unw_word t, unsigned char abreg,
                struct ia64_state_record *sr)
{
  struct ia64_reg_info *r;

  if (!desc_is_active (qp, t, sr))
    return;

  r = sr->curr.reg + decode_abreg (abreg, 0);
  r->where = IA64_WHERE_NONE;
  r->when = IA64_WHEN_NEVER;
  r->val = 0;
}

static inline void
desc_spill_reg_p (unsigned char qp, unw_word t, unsigned char abreg,
                  unsigned char x, unsigned char ytreg,
                  struct ia64_state_record *sr)
{
  enum ia64_where where = IA64_WHERE_GR;
  struct ia64_reg_info *r;

  if (!desc_is_active (qp, t, sr))
    return;

  if (x)
    where = IA64_WHERE_BR;
  else if (ytreg & 0x80)
    where = IA64_WHERE_FR;

  r = sr->curr.reg + decode_abreg (abreg, 0);
  r->where = where;
  r->when = sr->region_start + MIN ((int) t, sr->region_len - 1);
  r->val = (ytreg & 0x7f);
}

static inline void
desc_spill_psprel_p (unsigned char qp, unw_word t, unsigned char abreg,
                     unw_word pspoff, struct ia64_state_record *sr)
{
  struct ia64_reg_info *r;

  if (!desc_is_active (qp, t, sr))
    return;

  r = sr->curr.reg + decode_abreg (abreg, 1);
  r->where = IA64_WHERE_PSPREL;
  r->when = sr->region_start + MIN ((int) t, sr->region_len - 1);
  r->val = 0x10 - 4 * pspoff;
}

static inline void
desc_spill_sprel_p (unsigned char qp, unw_word t, unsigned char abreg,
                    unw_word spoff, struct ia64_state_record *sr)
{
  struct ia64_reg_info *r;

  if (!desc_is_active (qp, t, sr))
    return;

  r = sr->curr.reg + decode_abreg (abreg, 1);
  r->where = IA64_WHERE_SPREL;
  r->when = sr->region_start + MIN ((int) t, sr->region_len - 1);
  r->val = 4 * spoff;
}

#define UNW_DEC_BAD_CODE(code)                                          \
        print_error ("libunwind: unknown code encountered\n")

/* Register names.  */
#define UNW_REG_BSP             IA64_REG_BSP
#define UNW_REG_BSPSTORE        IA64_REG_BSPSTORE
#define UNW_REG_FPSR            IA64_REG_FPSR
#define UNW_REG_LC              IA64_REG_LC
#define UNW_REG_PFS             IA64_REG_PFS
#define UNW_REG_PR              IA64_REG_PR
#define UNW_REG_RNAT            IA64_REG_RNAT
#define UNW_REG_PSP             IA64_REG_PSP
#define UNW_REG_RP              IA64_REG_IP
#define UNW_REG_UNAT            IA64_REG_UNAT

/* Region headers.  */
#define UNW_DEC_PROLOGUE_GR(fmt,r,m,gr,arg)     desc_prologue(0,r,m,gr,arg)
#define UNW_DEC_PROLOGUE(fmt,b,r,arg)           desc_prologue(b,r,0,32,arg)

/* Prologue descriptors.  */
#define UNW_DEC_ABI(fmt,a,c,arg)                desc_abi(a,c,arg)
#define UNW_DEC_BR_GR(fmt,b,g,arg)              desc_br_gr(b,g,arg)
#define UNW_DEC_BR_MEM(fmt,b,arg)               desc_br_mem(b,arg)
#define UNW_DEC_FRGR_MEM(fmt,g,f,arg)           desc_frgr_mem(g,f,arg)
#define UNW_DEC_FR_MEM(fmt,f,arg)               desc_fr_mem(f,arg)
#define UNW_DEC_GR_GR(fmt,m,g,arg)              desc_gr_gr(m,g,arg)
#define UNW_DEC_GR_MEM(fmt,m,arg)               desc_gr_mem(m,arg)
#define UNW_DEC_MEM_STACK_F(fmt,t,s,arg)        desc_mem_stack_f(t,s,arg)
#define UNW_DEC_MEM_STACK_V(fmt,t,arg)          desc_mem_stack_v(t,arg)
#define UNW_DEC_REG_GR(fmt,r,d,arg)             desc_reg_gr(r,d,arg)
#define UNW_DEC_REG_PSPREL(fmt,r,o,arg)         desc_reg_psprel(r,o,arg)
#define UNW_DEC_REG_SPREL(fmt,r,o,arg)          desc_reg_sprel(r,o,arg)
#define UNW_DEC_REG_WHEN(fmt,r,t,arg)           desc_reg_when(r,t,arg)
#define UNW_DEC_PRIUNAT_WHEN_GR(fmt,t,arg) \
        desc_reg_when(IA64_REG_PRI_UNAT_GR,t,arg)
#define UNW_DEC_PRIUNAT_WHEN_MEM(fmt,t,arg) \
        desc_reg_when(IA64_REG_PRI_UNAT_MEM,t,arg)
#define UNW_DEC_PRIUNAT_GR(fmt,r,arg) \
        desc_reg_gr(IA64_REG_PRI_UNAT_GR,r,arg)
#define UNW_DEC_PRIUNAT_PSPREL(fmt,o,arg) \
        desc_reg_psprel(IA64_REG_PRI_UNAT_MEM,o,arg)
#define UNW_DEC_PRIUNAT_SPREL(fmt,o,arg) \
        desc_reg_sprel(IA64_REG_PRI_UNAT_MEM,o,arg)
#define UNW_DEC_RP_BR(fmt,d,arg)                desc_rp_br(d,arg)
#define UNW_DEC_SPILL_BASE(fmt,o,arg)           desc_spill_base(o,arg)
#define UNW_DEC_SPILL_MASK(fmt,m,arg)           (m = desc_spill_mask(m,arg))

/* Body descriptors.  */
#define UNW_DEC_EPILOGUE(fmt,t,c,arg)           desc_epilogue(t,c,arg)
#define UNW_DEC_COPY_STATE(fmt,l,arg)           desc_copy_state(l,arg)
#define UNW_DEC_LABEL_STATE(fmt,l,arg)          desc_label_state(l,arg)

/* General unwind descriptors.  */
#define UNW_DEC_SPILL_REG_P(f,p,t,a,x,y,arg)    desc_spill_reg_p(p,t,a,x,y,arg)
#define UNW_DEC_SPILL_REG(f,t,a,x,y,arg)        desc_spill_reg_p(0,t,a,x,y,arg)
#define UNW_DEC_SPILL_PSPREL_P(f,p,t,a,o,arg) \
        desc_spill_psprel_p(p,t,a,o,arg)
#define UNW_DEC_SPILL_PSPREL(f,t,a,o,arg) \
        desc_spill_psprel_p(0,t,a,o,arg)
#define UNW_DEC_SPILL_SPREL_P(f,p,t,a,o,arg)    desc_spill_sprel_p(p,t,a,o,arg)
#define UNW_DEC_SPILL_SPREL(f,t,a,o,arg)        desc_spill_sprel_p(0,t,a,o,arg)
#define UNW_DEC_RESTORE_P(f,p,t,a,arg)          desc_restore_p(p,t,a,arg)
#define UNW_DEC_RESTORE(f,t,a,arg)              desc_restore_p(0,t,a,arg)

#include "unwind_decoder.h"

#ifdef _U_dyn_op

/* parse dynamic unwind info */

static struct ia64_reg_info *
lookup_preg (int regnum, int memory, struct ia64_state_record *sr)
{
  int preg;

  switch (regnum)
    {
    case UNW_IA64_AR_BSP:               preg = IA64_REG_BSP; break;
    case UNW_IA64_AR_BSPSTORE:          preg = IA64_REG_BSPSTORE; break;
    case UNW_IA64_AR_FPSR:              preg = IA64_REG_FPSR; break;
    case UNW_IA64_AR_LC:                preg = IA64_REG_LC; break;
    case UNW_IA64_AR_PFS:               preg = IA64_REG_PFS; break;
    case UNW_IA64_AR_RNAT:              preg = IA64_REG_RNAT; break;
    case UNW_IA64_AR_UNAT:              preg = IA64_REG_UNAT; break;
    case UNW_IA64_BR + 0:               preg = IA64_REG_IP; break;
    case UNW_IA64_PR:                   preg = IA64_REG_PR; break;
    case UNW_IA64_SP:                   preg = IA64_REG_PSP; break;

    case UNW_IA64_NAT:
      if (memory)
        preg = IA64_REG_PRI_UNAT_MEM;
      else
        preg = IA64_REG_PRI_UNAT_GR;
      break;

    case UNW_IA64_GR + 4 ... UNW_IA64_GR + 7:
      preg = IA64_REG_R4 + (regnum - (UNW_IA64_GR + 4));
      break;

    case UNW_IA64_BR + 1 ... UNW_IA64_BR + 5:
      preg = IA64_REG_B1 + (regnum - UNW_IA64_BR);
      break;

    case UNW_IA64_FR + 2 ... UNW_IA64_FR + 5:
      preg = IA64_REG_F2 + (regnum - (UNW_IA64_FR + 2));
      break;

    case UNW_IA64_FR + 16 ... UNW_IA64_FR + 31:
      preg = IA64_REG_F16 + (regnum - (UNW_IA64_FR + 16));
      break;

    default:
      Dprintf ("%s: invalid register number %d\n", __FUNCTION__, regnum);
      return NULL;
    }
  return sr->curr.reg + preg;
}

/* An alias directive inside a region of length RLEN is interpreted to
   mean that the region behaves exactly like the first RLEN
   instructions at the aliased IP.  RLEN=0 implies that the current
   state matches exactly that of before the instruction at the aliased
   IP is executed.  */

static int
desc_alias (unw_dyn_op_t *op, struct cursor *c, struct ia64_state_record *sr)
{
  struct ia64_state_record orig_sr = *sr;
  int i, ret, when, rlen = sr->region_len;
  unw_word_t new_ip;

  when = MIN (sr->when_target, rlen);
  new_ip = op->val + ((when / 3) * 16 + (when % 3));

  if ((ret = ia64_fetch_proc_info (c, new_ip, 1)) < 0)
    return ret;

  if ((ret = create_state_record_for (c, sr, new_ip)) < 0)
    return ret;

  sr->first_region = orig_sr.first_region;
  sr->done = 0;
  sr->any_spills |= orig_sr.any_spills;
  sr->in_body = orig_sr.in_body;
  sr->region_start = orig_sr.region_start;
  sr->region_len = orig_sr.region_len;
  if (sr->when_sp_restored != IA64_WHEN_NEVER)
    sr->when_sp_restored = op->when + MIN (orig_sr.when_sp_restored, rlen);
  sr->epilogue_count = orig_sr.epilogue_count;
  sr->when_target = orig_sr.when_target;

  for (i = 0; i < IA64_NUM_PREGS; ++i)
    if (sr->curr.reg[i].when != IA64_WHEN_NEVER)
      sr->curr.reg[i].when = op->when + MIN (sr->curr.reg[i].when, rlen);

  ia64_free_state_record (sr);
  sr->labeled_states = orig_sr.labeled_states;
  sr->curr.next = orig_sr.curr.next;
  return 0;
}

static inline int
parse_dynamic (struct cursor *c, struct ia64_state_record *sr)
{
  unw_dyn_info_t *di = c->pi.unwind_info;
  unw_dyn_proc_info_t *proc = &di->u.pi;
  unw_dyn_region_info_t *r;
  struct ia64_reg_info *ri;
  enum ia64_where where;
  int32_t when, len;
  unw_dyn_op_t *op;
  unw_word_t val;
  int memory, ret;
  int8_t qp;

  for (r = proc->regions; r; r = r->next)
    {
      len = r->insn_count;
      if (len < 0)
        {
          if (r->next)
            {
              Debug (1, "negative region length allowed in last region only!");
              return -UNW_EINVAL;
            }
          len = -len;
          /* hack old region info to set the start where we need it: */
          sr->region_start = (di->end_ip - di->start_ip) / 0x10 * 3 - len;
          sr->region_len = 0;
        }
      /* all regions are treated as prologue regions: */
      desc_prologue (0, len, 0, 0, sr);

      if (sr->done)
        return 0;

      for (op = r->op; op < r->op + r->op_count; ++op)
        {
          when = op->when;
          val = op->val;
          qp = op->qp;

          if (!desc_is_active (qp, when, sr))
            continue;

          when = sr->region_start + MIN ((int) when, sr->region_len - 1);

          switch (op->tag)
            {
            case UNW_DYN_SAVE_REG:
              memory = 0;
              if ((unsigned) (val - UNW_IA64_GR) < 128)
                where = IA64_WHERE_GR;
              else if ((unsigned) (val - UNW_IA64_FR) < 128)
                where = IA64_WHERE_FR;
              else if ((unsigned) (val - UNW_IA64_BR) < 8)
                where = IA64_WHERE_BR;
              else
                {
                  Dprintf ("%s: can't save to register number %d\n",
                           __FUNCTION__, (int) op->reg);
                  return -UNW_EBADREG;
                }
              /* fall through */
            update_reg_info:
              ri = lookup_preg (op->reg, memory, sr);
              if (!ri)
                return -UNW_EBADREG;
              ri->where = where;
              ri->when = when;
              ri->val = val;
              break;

            case UNW_DYN_SPILL_FP_REL:
              memory = 1;
              where = IA64_WHERE_PSPREL;
              val = 0x10 - val;
              goto update_reg_info;

            case UNW_DYN_SPILL_SP_REL:
              memory = 1;
              where = IA64_WHERE_SPREL;
              goto update_reg_info;

            case UNW_DYN_ADD:
              if (op->reg == UNW_IA64_SP)
                {
                  if (val & 0xf)
                    {
                      Dprintf ("%s: frame-size %ld not an integer "
                               "multiple of 16\n",
                               __FUNCTION__, (long) op->val);
                      return -UNW_EINVAL;
                    }
                  desc_mem_stack_f (when, -((int64_t) val / 16), sr);
                }
              else
                {
                  Dprintf ("%s: can only ADD to stack-pointer\n",
                           __FUNCTION__);
                  return -UNW_EBADREG;
                }
              break;

            case UNW_DYN_POP_FRAMES:
              sr->when_sp_restored = when;
              sr->epilogue_count = op->val;
              break;

            case UNW_DYN_LABEL_STATE:
              desc_label_state (op->val, sr);
              break;

            case UNW_DYN_COPY_STATE:
              desc_copy_state (op->val, sr);
              break;

            case UNW_DYN_ALIAS:
              if ((ret = desc_alias (op, c, sr)) < 0)
                return ret;

            case UNW_DYN_STOP:
              goto end_of_ops;
            }
        }
    end_of_ops:
      ;
    }
  return 0;
}
#else
# define parse_dynamic(c,sr)    (-UNW_EINVAL)
#endif /* _U_dyn_op */


HIDDEN int
ia64_fetch_proc_info (struct cursor *c, unw_word_t ip, int need_unwind_info)
{
  int ret, dynamic = 1;

  if (c->pi_valid && !need_unwind_info)
    return 0;

  /* check dynamic info first --- it overrides everything else */
  ret = unwi_find_dynamic_proc_info (c->as, ip, &c->pi, need_unwind_info,
                                     c->as_arg);
  if (ret == -UNW_ENOINFO)
    {
      dynamic = 0;
      ret = ia64_find_proc_info (c, ip, need_unwind_info);
    }

  c->pi_valid = 1;
  c->pi_is_dynamic = dynamic;
  return ret;
}

static inline void
put_unwind_info (struct cursor *c, unw_proc_info_t *pi)
{
  if (!c->pi_valid)
    return;

  if (c->pi_is_dynamic)
    unwi_put_dynamic_unwind_info (c->as, pi, c->as_arg);
  else
    ia64_put_unwind_info (c, pi);
}

static int
create_state_record_for (struct cursor *c, struct ia64_state_record *sr,
                         unw_word_t ip)
{
  unw_word_t predicates = c->pr;
  struct ia64_reg_info *r;
  uint8_t *dp, *desc_end;
  int ret;

  assert (c->pi_valid);

  /* build state record */
  memset (sr, 0, sizeof (*sr));
  for (r = sr->curr.reg; r < sr->curr.reg + IA64_NUM_PREGS; ++r)
    r->when = IA64_WHEN_NEVER;
  sr->pr_val = predicates;
  sr->first_region = 1;

  if (!c->pi.unwind_info)
    {
      /* No info, return default unwinder (leaf proc, no mem stack, no
         saved regs), rp in b0, pfs in ar.pfs.  */
      Debug (1, "no unwind info for ip=0x%lx (gp=%lx)\n",
             (long) ip, (long) c->pi.gp);
      sr->curr.reg[IA64_REG_IP].where = IA64_WHERE_BR;
      sr->curr.reg[IA64_REG_IP].when = -1;
      sr->curr.reg[IA64_REG_IP].val = 0;
      goto out;
    }

  sr->when_target = (3 * ((ip & ~(unw_word_t) 0xf) - c->pi.start_ip) / 16
                     + (ip & 0xf));

  switch (c->pi.format)
    {
    case UNW_INFO_FORMAT_TABLE:
    case UNW_INFO_FORMAT_REMOTE_TABLE:
      dp = c->pi.unwind_info;
      desc_end = dp + c->pi.unwind_info_size;
      while (!sr->done && dp < desc_end)
        dp = unw_decode (dp, sr->in_body, sr);
      ret = 0;
      break;

    case UNW_INFO_FORMAT_DYNAMIC:
      ret = parse_dynamic (c, sr);
      break;

    default:
      ret = -UNW_EINVAL;
    }

  put_unwind_info (c, &c->pi);

  if (ret < 0)
    return ret;

  if (sr->when_target > sr->when_sp_restored)
    {
      /* sp has been restored and all values on the memory stack below
         psp also have been restored.  */
      sr->curr.reg[IA64_REG_PSP].val = 0;
      sr->curr.reg[IA64_REG_PSP].where = IA64_WHERE_NONE;
      sr->curr.reg[IA64_REG_PSP].when = IA64_WHEN_NEVER;
      for (r = sr->curr.reg; r < sr->curr.reg + IA64_NUM_PREGS; ++r)
        if ((r->where == IA64_WHERE_PSPREL && r->val <= 0x10)
            || r->where == IA64_WHERE_SPREL)
          {
            r->val = 0;
            r->where = IA64_WHERE_NONE;
            r->when = IA64_WHEN_NEVER;
          }
    }

  /* If RP did't get saved, generate entry for the return link
     register.  */
  if (sr->curr.reg[IA64_REG_IP].when >= sr->when_target)
    {
      sr->curr.reg[IA64_REG_IP].where = IA64_WHERE_BR;
      sr->curr.reg[IA64_REG_IP].when = -1;
      sr->curr.reg[IA64_REG_IP].val = sr->return_link_reg;
    }

  if (sr->when_target > sr->curr.reg[IA64_REG_BSP].when
      && sr->when_target > sr->curr.reg[IA64_REG_BSPSTORE].when
      && sr->when_target > sr->curr.reg[IA64_REG_RNAT].when)
    {
      Debug (8, "func 0x%lx may switch the register-backing-store\n",
             c->pi.start_ip);
      c->pi.flags |= UNW_PI_FLAG_IA64_RBS_SWITCH;
    }
 out:
#if UNW_DEBUG
  if (unwi_debug_level > 2)
    {
      Dprintf ("%s: state record for func 0x%lx, t=%u (flags=0x%lx):\n",
               __FUNCTION__,
               (long) c->pi.start_ip, sr->when_target, (long) c->pi.flags);
      for (r = sr->curr.reg; r < sr->curr.reg + IA64_NUM_PREGS; ++r)
        {
          if (r->where != IA64_WHERE_NONE || r->when != IA64_WHEN_NEVER)
            {
              Dprintf ("  %s <- ", unw.preg_name[r - sr->curr.reg]);
              switch (r->where)
                {
                case IA64_WHERE_GR:
                  Dprintf ("r%lu", (long) r->val);
                  break;
                case IA64_WHERE_FR:
                  Dprintf ("f%lu", (long) r->val);
                  break;
                case IA64_WHERE_BR:
                  Dprintf ("b%lu", (long) r->val);
                  break;
                case IA64_WHERE_SPREL:
                  Dprintf ("[sp+0x%lx]", (long) r->val);
                  break;
                case IA64_WHERE_PSPREL:
                  Dprintf ("[psp+0x%lx]", (long) r->val);
                  break;
                case IA64_WHERE_NONE:
                  Dprintf ("%s+0x%lx",
                           unw.preg_name[r - sr->curr.reg], (long) r->val);
                  break;
                default:
                  Dprintf ("BADWHERE(%d)", r->where);
                  break;
                }
              Dprintf ("\t\t%d\n", r->when);
            }
        }
    }
#endif
  return 0;
}

/* The proc-info must be valid for IP before this routine can be
   called.  */
HIDDEN int
ia64_create_state_record (struct cursor *c, struct ia64_state_record *sr)
{
  return create_state_record_for (c, sr, c->ip);
}

HIDDEN int
ia64_free_state_record (struct ia64_state_record *sr)
{
  struct ia64_labeled_state *ls, *next;

  /* free labeled register states & stack: */

  for (ls = sr->labeled_states; ls; ls = next)
    {
      next = ls->next;
      free_state_stack (&ls->saved_state);
      free_labeled_state (ls);
    }
  free_state_stack (&sr->curr);

  return 0;
}

HIDDEN int
ia64_make_proc_info (struct cursor *c)
{
  int ret, caching = c->as->caching_policy != UNW_CACHE_NONE;

  if (!caching || ia64_get_cached_proc_info (c) < 0)
    {
      /* Lookup it up the slow way... */
      if ((ret = ia64_fetch_proc_info (c, c->ip, 0)) < 0)
        return ret;
      if (caching)
        ia64_cache_proc_info (c);
    }
  return 0;
}
