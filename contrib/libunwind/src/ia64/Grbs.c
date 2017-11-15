/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2005 Hewlett-Packard Co
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

/* Logically, we like to think of the stack as a contiguous region of
memory.  Unfortunately, this logical view doesn't work for the
register backing store, because the RSE is an asynchronous engine and
because UNIX/Linux allow for stack-switching via sigaltstack(2).
Specifically, this means that any given stacked register may or may
not be backed up by memory in the current stack.  If not, then the
backing memory may be found in any of the "more inner" (younger)
stacks.  The routines in this file help manage the discontiguous
nature of the register backing store.  The routines are completely
independent of UNIX/Linux, but each stack frame that switches the
backing store is expected to reserve 4 words for use by libunwind. For
example, in the Linux sigcontext, sc_fr[0] and sc_fr[1] serve this
purpose.  */

#include "unwind_i.h"

#if UNW_DEBUG

HIDDEN const char *
ia64_strloc (ia64_loc_t loc)
{
  static char buf[128];

  if (IA64_IS_NULL_LOC (loc))
    return "<null>";

  buf[0] = '\0';

  if (IA64_IS_MEMSTK_NAT (loc))
    strcat (buf, "memstk_nat(");
  if (IA64_IS_UC_LOC (loc))
    strcat (buf, "uc(");
  if (IA64_IS_FP_LOC (loc))
    strcat (buf, "fp(");

  if (IA64_IS_REG_LOC (loc))
    sprintf (buf + strlen (buf), "%s", unw_regname (IA64_GET_REG (loc)));
  else
    sprintf (buf + strlen (buf), "0x%llx",
             (unsigned long long) IA64_GET_ADDR (loc));

  if (IA64_IS_FP_LOC (loc))
    strcat (buf, ")");
  if (IA64_IS_UC_LOC (loc))
    strcat (buf, ")");
  if (IA64_IS_MEMSTK_NAT (loc))
    strcat (buf, ")");

  return buf;
}

#endif /* UNW_DEBUG */

HIDDEN int
rbs_switch (struct cursor *c,
            unw_word_t saved_bsp, unw_word_t saved_bspstore,
            ia64_loc_t saved_rnat_loc)
{
  struct rbs_area *rbs = &c->rbs_area[c->rbs_curr];
  unw_word_t lo, ndirty, rbs_base;
  int ret;

  Debug (10, "(left=%u, curr=%u)\n", c->rbs_left_edge, c->rbs_curr);

  /* Calculate address "lo" at which the backing store starts:  */
  ndirty = rse_num_regs (saved_bspstore, saved_bsp);
  lo = rse_skip_regs (c->bsp, -ndirty);

  rbs->size = (rbs->end - lo);

  /* If the previously-recorded rbs-area is empty we don't need to
     track it and we can simply overwrite it... */
  if (rbs->size)
    {
      Debug (10, "inner=[0x%lx-0x%lx)\n",
             (long) (rbs->end - rbs->size), (long) rbs->end);

      c->rbs_curr = (c->rbs_curr + 1) % ARRAY_SIZE (c->rbs_area);
      rbs = c->rbs_area + c->rbs_curr;

      if (c->rbs_curr == c->rbs_left_edge)
        c->rbs_left_edge = (c->rbs_left_edge + 1) % ARRAY_SIZE (c->rbs_area);
    }

  if ((ret = rbs_get_base (c, saved_bspstore, &rbs_base)) < 0)
    return ret;

  rbs->end = saved_bspstore;
  rbs->size = saved_bspstore - rbs_base;
  rbs->rnat_loc = saved_rnat_loc;

  c->bsp = saved_bsp;

  Debug (10, "outer=[0x%llx-0x%llx), rnat@%s\n", (long long) rbs_base,
         (long long) rbs->end, ia64_strloc (rbs->rnat_loc));
  return 0;
}

HIDDEN int
rbs_find_stacked (struct cursor *c, unw_word_t regs_to_skip,
                  ia64_loc_t *locp, ia64_loc_t *rnat_locp)
{
  unw_word_t nregs, bsp = c->bsp, curr = c->rbs_curr, n;
  unw_word_t left_edge = c->rbs_left_edge;
#if UNW_DEBUG
  int reg = 32 + regs_to_skip;
#endif

  while (!rbs_contains (&c->rbs_area[curr], bsp))
    {
      if (curr == left_edge)
        {
          Debug (1, "could not find register r%d!\n", reg);
          return -UNW_EBADREG;
        }

      n = rse_num_regs (c->rbs_area[curr].end, bsp);
      curr = (curr + ARRAY_SIZE (c->rbs_area) - 1) % ARRAY_SIZE (c->rbs_area);
      bsp = rse_skip_regs (c->rbs_area[curr].end - c->rbs_area[curr].size, n);
    }

  while (1)
    {
      nregs = rse_num_regs (bsp, c->rbs_area[curr].end);

      if (regs_to_skip < nregs)
        {
          /* found it: */
          unw_word_t addr;

          addr = rse_skip_regs (bsp, regs_to_skip);
          if (locp)
            *locp = rbs_loc (c->rbs_area + curr, addr);
          if (rnat_locp)
            *rnat_locp = rbs_get_rnat_loc (c->rbs_area + curr, addr);
          return 0;
        }

      if (curr == left_edge)
        {
          Debug (1, "could not find register r%d!\n", reg);
          return -UNW_EBADREG;
        }

      regs_to_skip -= nregs;

      curr = (curr + ARRAY_SIZE (c->rbs_area) - 1) % ARRAY_SIZE (c->rbs_area);
      bsp = c->rbs_area[curr].end - c->rbs_area[curr].size;
    }
}

#ifdef NEED_RBS_COVER_AND_FLUSH

static inline int
get_rnat (struct cursor *c, struct rbs_area *rbs, unw_word_t bsp,
          unw_word_t *__restrict rnatp)
{
  ia64_loc_t rnat_locp = rbs_get_rnat_loc (rbs, bsp);

  return ia64_get (c, rnat_locp, rnatp);
}

/* Simulate the effect of "cover" followed by a "flushrs" for the
   target-frame.  However, since the target-frame's backing store
   may not have space for the registers that got spilled onto other
   rbs-areas, we save those registers to DIRTY_PARTITION where
   we can then load them via a single "loadrs".

   This function returns the size of the dirty-partition that was
   created or a negative error-code in case of error.

   Note: This does not modify the rbs_area[] structure in any way.  */
HIDDEN int
rbs_cover_and_flush (struct cursor *c, unw_word_t nregs,
                     unw_word_t *dirty_partition, unw_word_t *dirty_rnat,
                     unw_word_t *bspstore)
{
  unw_word_t n, src_mask, dst_mask, bsp, *dst, src_rnat, dst_rnat = 0;
  unw_word_t curr = c->rbs_curr, left_edge = c->rbs_left_edge;
  struct rbs_area *rbs = c->rbs_area + curr;
  int ret;

  bsp = c->bsp;
  c->bsp = rse_skip_regs (bsp, nregs);

  if (likely (rbs_contains (rbs, bsp)))
    {
      /* at least _some_ registers are on rbs... */
      n = rse_num_regs (bsp, rbs->end);
      if (likely (n >= nregs))
        {
          /* common case #1: all registers are on current rbs... */
          /* got lucky: _all_ registers are on rbs... */
          ia64_loc_t rnat_loc = rbs_get_rnat_loc (rbs, c->bsp);

          *bspstore = c->bsp;

          if (IA64_IS_REG_LOC (rnat_loc))
            {
              unw_word_t rnat_addr = (unw_word_t)
                tdep_uc_addr (c->as_arg, UNW_IA64_AR_RNAT, NULL);
              rnat_loc = IA64_LOC_ADDR (rnat_addr, 0);
            }
          c->loc[IA64_REG_RNAT] = rnat_loc;
          return 0;     /* all done */
        }
      nregs -= n;       /* account for registers already on the rbs */

      assert (rse_skip_regs (c->bsp, -nregs) == rse_skip_regs (rbs->end, 0));
    }
  else
    /* Earlier frames also didn't get spilled; need to "loadrs" those,
       too... */
    nregs += rse_num_regs (rbs->end, bsp);

  /* OK, we need to copy NREGS registers to the dirty partition.  */

  *bspstore = bsp = rbs->end;
  c->loc[IA64_REG_RNAT] = rbs->rnat_loc;
  assert (!IA64_IS_REG_LOC (rbs->rnat_loc));

  dst = dirty_partition;

  while (nregs > 0)
    {
      if (unlikely (!rbs_contains (rbs, bsp)))
        {
          /* switch to next non-empty rbs-area: */
          do
            {
              if (curr == left_edge)
                {
                  Debug (0, "rbs-underflow while flushing %lu regs, "
                         "bsp=0x%lx, dst=0x%p\n", (unsigned long) nregs,
                         (unsigned long) bsp, dst);
                  return -UNW_EBADREG;
                }

              assert (rse_num_regs (rbs->end, bsp) == 0);

              curr = (curr + ARRAY_SIZE (c->rbs_area) - 1)
                      % ARRAY_SIZE (c->rbs_area);
              rbs = c->rbs_area + curr;
              bsp = rbs->end - rbs->size;
            }
          while (rbs->size == 0);

          if ((ret = get_rnat (c, rbs, bsp, &src_rnat)) < 0)
            return ret;
        }

      if (unlikely (rse_is_rnat_slot (bsp)))
        {
          bsp += 8;
          if ((ret = get_rnat (c, rbs, bsp, &src_rnat)) < 0)
            return ret;
        }
      if (unlikely (rse_is_rnat_slot ((unw_word_t) dst)))
        {
          *dst++ = dst_rnat;
          dst_rnat = 0;
        }

      src_mask = ((unw_word_t) 1) << rse_slot_num (bsp);
      dst_mask = ((unw_word_t) 1) << rse_slot_num ((unw_word_t) dst);

      if (src_rnat & src_mask)
        dst_rnat |= dst_mask;
      else
        dst_rnat &= ~dst_mask;

      /* copy one slot: */
      if ((ret = ia64_get (c, rbs_loc (rbs, bsp), dst)) < 0)
        return ret;

      /* advance to next slot: */
      --nregs;
      bsp += 8;
      ++dst;
    }
  if (unlikely (rse_is_rnat_slot ((unw_word_t) dst)))
    {
      /* The LOADRS instruction loads "the N bytes below the current
         BSP" but BSP can never point to an RNaT slot so if the last
         destination word happens to be an RNaT slot, we need to write
         that slot now. */
      *dst++ = dst_rnat;
      dst_rnat = 0;
    }
  *dirty_rnat = dst_rnat;
  return (char *) dst - (char *) dirty_partition;
}

#endif /* !UNW_REMOTE_ONLY */
