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

#include "offsets.h"
#include "unwind_i.h"

static inline int
linux_sigtramp (struct cursor *c, ia64_loc_t prev_cfm_loc,
                unw_word_t *num_regsp)
{
#if defined(UNW_LOCAL_ONLY) && !defined(__linux)
  return -UNW_EINVAL;
#else
  unw_word_t sc_addr;
  int ret;

  if ((ret = ia64_get (c, IA64_LOC_ADDR (c->sp + 0x10
                                         + LINUX_SIGFRAME_ARG2_OFF, 0),
                       &sc_addr)) < 0)
    return ret;

  c->sigcontext_addr = sc_addr;

  if (!IA64_IS_REG_LOC (c->loc[IA64_REG_IP])
      && IA64_GET_ADDR (c->loc[IA64_REG_IP]) == sc_addr + LINUX_SC_BR_OFF + 8)
    {
      /* Linux kernels before 2.4.19 and 2.5.10 had buggy
         unwind info for sigtramp.  Fix it up here.  */
      c->loc[IA64_REG_IP]  = IA64_LOC_ADDR (sc_addr + LINUX_SC_IP_OFF, 0);
      c->cfm_loc = IA64_LOC_ADDR (sc_addr + LINUX_SC_CFM_OFF, 0);
    }

  /* do what can't be described by unwind directives: */
  c->loc[IA64_REG_PFS] = IA64_LOC_ADDR (sc_addr + LINUX_SC_AR_PFS_OFF, 0);
  c->ec_loc = prev_cfm_loc;
  *num_regsp = c->cfm & 0x7f;           /* size of frame */
  return 0;
#endif
}

static inline int
linux_interrupt (struct cursor *c, ia64_loc_t prev_cfm_loc,
                 unw_word_t *num_regsp, int marker)
{
#if defined(UNW_LOCAL_ONLY) && !(defined(__linux) && defined(__KERNEL__))
  return -UNW_EINVAL;
#else
  unw_word_t sc_addr, num_regs;
  ia64_loc_t pfs_loc;

  sc_addr = c->sigcontext_addr = c->sp + 0x10;

  if ((c->pr & (1UL << LINUX_PT_P_NONSYS)) != 0)
    num_regs = c->cfm & 0x7f;
  else
    num_regs = 0;

  /* do what can't be described by unwind directives: */
  if (marker == ABI_MARKER_OLD_LINUX_INTERRUPT)
          pfs_loc = IA64_LOC_ADDR (sc_addr + LINUX_OLD_PT_PFS_OFF, 0);
  else
          pfs_loc = IA64_LOC_ADDR (sc_addr + LINUX_PT_PFS_OFF, 0);
  c->loc[IA64_REG_PFS] = pfs_loc;
  c->ec_loc = prev_cfm_loc;
  *num_regsp = num_regs;                /* size of frame */
  return 0;
#endif
}

static inline int
hpux_sigtramp (struct cursor *c, ia64_loc_t prev_cfm_loc,
               unw_word_t *num_regsp)
{
#if defined(UNW_LOCAL_ONLY) && !defined(__hpux)
  return -UNW_EINVAL;
#else
  unw_word_t sc_addr, bsp, bspstore;
  ia64_loc_t sc_loc;
  int ret, i;

  /* HP-UX passes the address of ucontext_t in r32: */
  if ((ret = ia64_get_stacked (c, 32, &sc_loc, NULL)) < 0)
    return ret;
  if ((ret = ia64_get (c, sc_loc, &sc_addr)) < 0)
    return ret;

  c->sigcontext_addr = sc_addr;

  /* Now mark all (preserved) registers as coming from the
     signal context: */
  c->cfm_loc = IA64_LOC_UC_REG (UNW_IA64_CFM, sc_addr);
  c->loc[IA64_REG_PRI_UNAT_MEM] = IA64_NULL_LOC;
  c->loc[IA64_REG_PSP] = IA64_LOC_UC_REG (UNW_IA64_GR + 12, sc_addr);
  c->loc[IA64_REG_BSP] = IA64_LOC_UC_REG (UNW_IA64_AR_BSP, sc_addr);
  c->loc[IA64_REG_BSPSTORE] = IA64_LOC_UC_REG (UNW_IA64_AR_BSPSTORE, sc_addr);
  c->loc[IA64_REG_PFS] = IA64_LOC_UC_REG (UNW_IA64_AR_PFS, sc_addr);
  c->loc[IA64_REG_RNAT] = IA64_LOC_UC_REG (UNW_IA64_AR_RNAT, sc_addr);
  c->loc[IA64_REG_IP] = IA64_LOC_UC_REG (UNW_IA64_IP, sc_addr);
  c->loc[IA64_REG_R4] = IA64_LOC_UC_REG (UNW_IA64_GR + 4, sc_addr);
  c->loc[IA64_REG_R5] = IA64_LOC_UC_REG (UNW_IA64_GR + 5, sc_addr);
  c->loc[IA64_REG_R6] = IA64_LOC_UC_REG (UNW_IA64_GR + 6, sc_addr);
  c->loc[IA64_REG_R7] = IA64_LOC_UC_REG (UNW_IA64_GR + 7, sc_addr);
  c->loc[IA64_REG_NAT4] = IA64_LOC_UC_REG (UNW_IA64_NAT + 4, sc_addr);
  c->loc[IA64_REG_NAT5] = IA64_LOC_UC_REG (UNW_IA64_NAT + 5, sc_addr);
  c->loc[IA64_REG_NAT6] = IA64_LOC_UC_REG (UNW_IA64_NAT + 6, sc_addr);
  c->loc[IA64_REG_NAT7] = IA64_LOC_UC_REG (UNW_IA64_NAT + 7, sc_addr);
  c->loc[IA64_REG_UNAT] = IA64_LOC_UC_REG (UNW_IA64_AR_UNAT, sc_addr);
  c->loc[IA64_REG_PR] = IA64_LOC_UC_REG (UNW_IA64_PR, sc_addr);
  c->loc[IA64_REG_LC] = IA64_LOC_UC_REG (UNW_IA64_AR_LC, sc_addr);
  c->loc[IA64_REG_FPSR] = IA64_LOC_UC_REG (UNW_IA64_AR_FPSR, sc_addr);
  c->loc[IA64_REG_B1] = IA64_LOC_UC_REG (UNW_IA64_BR + 1, sc_addr);
  c->loc[IA64_REG_B2] = IA64_LOC_UC_REG (UNW_IA64_BR + 2, sc_addr);
  c->loc[IA64_REG_B3] = IA64_LOC_UC_REG (UNW_IA64_BR + 3, sc_addr);
  c->loc[IA64_REG_B4] = IA64_LOC_UC_REG (UNW_IA64_BR + 4, sc_addr);
  c->loc[IA64_REG_B5] = IA64_LOC_UC_REG (UNW_IA64_BR + 5, sc_addr);
  c->loc[IA64_REG_F2] = IA64_LOC_UC_REG (UNW_IA64_FR + 2, sc_addr);
  c->loc[IA64_REG_F3] = IA64_LOC_UC_REG (UNW_IA64_FR + 3, sc_addr);
  c->loc[IA64_REG_F4] = IA64_LOC_UC_REG (UNW_IA64_FR + 4, sc_addr);
  c->loc[IA64_REG_F5] = IA64_LOC_UC_REG (UNW_IA64_FR + 5, sc_addr);
  for (i = 0; i < 16; ++i)
    c->loc[IA64_REG_F16 + i] = IA64_LOC_UC_REG (UNW_IA64_FR + 16 + i, sc_addr);

  c->pi.flags |= UNW_PI_FLAG_IA64_RBS_SWITCH;

  /* update the CFM cache: */
  if ((ret = ia64_get (c, c->cfm_loc, &c->cfm)) < 0)
    return ret;
  /* update the PSP cache: */
  if ((ret = ia64_get (c, c->loc[IA64_REG_PSP], &c->psp)) < 0)
    return ret;

  if ((ret = ia64_get (c, c->loc[IA64_REG_BSP], &bsp)) < 0
      || (ret = ia64_get (c, c->loc[IA64_REG_BSPSTORE], &bspstore)) < 0)
    return ret;
  if (bspstore < bsp)
    /* Dirty partition got spilled into the ucontext_t structure
       itself.  We'll need to access it via uc_access(3).  */
    rbs_switch (c, bsp, bspstore, IA64_LOC_UC_ADDR (bsp | 0x1f8, 0));

  c->ec_loc = prev_cfm_loc;

  *num_regsp = 0;
  return 0;
#endif
}


static inline int
check_rbs_switch (struct cursor *c)
{
  unw_word_t saved_bsp, saved_bspstore, loadrs, ndirty;
  int ret = 0;

  saved_bsp = c->bsp;
  if (c->pi.flags & UNW_PI_FLAG_IA64_RBS_SWITCH)
    {
      /* Got ourselves a frame that has saved ar.bspstore, ar.bsp,
         and ar.rnat, so we're all set for rbs-switching:  */
      if ((ret = ia64_get (c, c->loc[IA64_REG_BSP], &saved_bsp)) < 0
          || (ret = ia64_get (c, c->loc[IA64_REG_BSPSTORE], &saved_bspstore)))
        return ret;
    }
  else if ((c->abi_marker == ABI_MARKER_LINUX_SIGTRAMP
            || c->abi_marker == ABI_MARKER_OLD_LINUX_SIGTRAMP)
           && !IA64_IS_REG_LOC (c->loc[IA64_REG_BSP])
           && (IA64_GET_ADDR (c->loc[IA64_REG_BSP])
               == c->sigcontext_addr + LINUX_SC_AR_BSP_OFF))
    {
      /* When Linux delivers a signal on an alternate stack, it
         does things a bit differently from what the unwind
         conventions allow us to describe: instead of saving
         ar.rnat, ar.bsp, and ar.bspstore, it saves the former two
         plus the "loadrs" value.  Because of this, we need to
         detect & record a potential rbs-area switch
         manually... */

      /* If ar.bsp has been saved already AND the current bsp is
         not equal to the saved value, then we know for sure that
         we're past the point where the backing store has been
         switched (and before the point where it's restored).  */
      if ((ret = ia64_get (c, IA64_LOC_ADDR (c->sigcontext_addr
                                             + LINUX_SC_AR_BSP_OFF, 0),
                           &saved_bsp) < 0)
          || (ret = ia64_get (c, IA64_LOC_ADDR (c->sigcontext_addr
                                                + LINUX_SC_LOADRS_OFF, 0),
                              &loadrs) < 0))
        return ret;
      loadrs >>= 16;
      ndirty = rse_num_regs (c->bsp - loadrs, c->bsp);
      saved_bspstore = rse_skip_regs (saved_bsp, -ndirty);
    }

  if (saved_bsp == c->bsp)
    return 0;

  return rbs_switch (c, saved_bsp, saved_bspstore, c->loc[IA64_REG_RNAT]);
}

static inline int
update_frame_state (struct cursor *c)
{
  unw_word_t prev_ip, prev_sp, prev_bsp, ip, num_regs;
  ia64_loc_t prev_cfm_loc;
  int ret;

  prev_cfm_loc = c->cfm_loc;
  prev_ip = c->ip;
  prev_sp = c->sp;
  prev_bsp = c->bsp;

  /* Update the IP cache (do this first: if we reach the end of the
     frame-chain, the rest of the info may not be valid/useful
     anymore. */
  ret = ia64_get (c, c->loc[IA64_REG_IP], &ip);
  if (ret < 0)
    return ret;
  c->ip = ip;

  if ((ip & 0xc) != 0)
    {
      /* don't let obviously bad addresses pollute the cache */
      Debug (1, "rejecting bad ip=0x%lx\n", (long) c->ip);
      return -UNW_EINVALIDIP;
    }

  c->cfm_loc = c->loc[IA64_REG_PFS];
  /* update the CFM cache: */
  ret = ia64_get (c, c->cfm_loc, &c->cfm);
  if (ret < 0)
    return ret;

  /* Normally, AR.EC is stored in the CFM save-location.  That
     save-location contains the full function-state as defined by
     AR.PFS.  However, interruptions only save the frame-marker, not
     any other info in CFM.  Instead, AR.EC gets saved on the first
     call by the interruption-handler.  Thus, interruption-related
     frames need to track the _previous_ CFM save-location since
     that's were AR.EC is saved.  We support this by setting ec_loc to
     cfm_loc by default and giving frames marked with an ABI-marker
     the chance to override this value with prev_cfm_loc.  */
  c->ec_loc = c->cfm_loc;

  num_regs = 0;
  if (unlikely (c->abi_marker))
    {
      c->last_abi_marker = c->abi_marker;
      switch (ia64_get_abi_marker (c))
        {
        case ABI_MARKER_LINUX_SIGTRAMP:
        case ABI_MARKER_OLD_LINUX_SIGTRAMP:
          ia64_set_abi (c, ABI_LINUX);
          if ((ret = linux_sigtramp (c, prev_cfm_loc, &num_regs)) < 0)
            return ret;
          break;

        case ABI_MARKER_OLD_LINUX_INTERRUPT:
        case ABI_MARKER_LINUX_INTERRUPT:
          ia64_set_abi (c, ABI_LINUX);
          if ((ret = linux_interrupt (c, prev_cfm_loc, &num_regs,
                                      c->abi_marker)) < 0)
            return ret;
          break;

        case ABI_MARKER_HP_UX_SIGTRAMP:
          ia64_set_abi (c, ABI_HPUX);
          if ((ret = hpux_sigtramp (c, prev_cfm_loc, &num_regs)) < 0)
            return ret;
          break;

        default:
          Debug (1, "unknown ABI marker: ABI=%u, context=%u\n",
                 c->abi_marker >> 8, c->abi_marker & 0xff);
          return -UNW_EINVAL;
        }
      Debug (12, "sigcontext_addr=%lx (ret=%d)\n",
             (unsigned long) c->sigcontext_addr, ret);

      c->sigcontext_off = c->sigcontext_addr - c->sp;

      /* update the IP cache: */
      if ((ret = ia64_get (c, c->loc[IA64_REG_IP], &ip)) < 0)
        return ret;
      c->ip = ip;
      if (ip == 0)
        /* end of frame-chain reached */
        return 0;
    }
  else
    num_regs = (c->cfm >> 7) & 0x7f;    /* size of locals */

  if (!IA64_IS_NULL_LOC (c->loc[IA64_REG_BSP]))
    {
      ret = check_rbs_switch (c);
      if (ret < 0)
        return ret;
    }

  c->bsp = rse_skip_regs (c->bsp, -num_regs);

  c->sp = c->psp;
  c->abi_marker = 0;

  if (c->ip == prev_ip && c->sp == prev_sp && c->bsp == prev_bsp)
    {
      Dprintf ("%s: ip, sp, and bsp unchanged; stopping here (ip=0x%lx)\n",
               __FUNCTION__, (long) ip);
      return -UNW_EBADFRAME;
    }

  /* as we unwind, the saved ar.unat becomes the primary unat: */
  c->loc[IA64_REG_PRI_UNAT_MEM] = c->loc[IA64_REG_UNAT];

  /* restore the predicates: */
  ret = ia64_get (c, c->loc[IA64_REG_PR], &c->pr);
  if (ret < 0)
    return ret;

  c->pi_valid = 0;
  return 0;
}


PROTECTED int
unw_step (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  Debug (1, "(cursor=%p, ip=0x%016lx)\n", c, (unsigned long) c->ip);

  if ((ret = ia64_find_save_locs (c)) >= 0
      && (ret = update_frame_state (c)) >= 0)
    ret = (c->ip == 0) ? 0 : 1;

  Debug (2, "returning %d (ip=0x%016lx)\n", ret, (unsigned long) c->ip);
  return ret;
}
