/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2005 Hewlett-Packard Co
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

static ALWAYS_INLINE int
common_init (struct cursor *c, unw_word_t sp, unw_word_t bsp)
{
  unw_word_t bspstore, rbs_base;
  int ret;

  if (c->as->caching_policy != UNW_CACHE_NONE)
    /* ensure cache doesn't have any stale contents: */
    ia64_validate_cache (c->as, c->as_arg);

  c->cfm_loc =                  IA64_REG_LOC (c, UNW_IA64_CFM);
  c->loc[IA64_REG_BSP] =        IA64_NULL_LOC;
  c->loc[IA64_REG_BSPSTORE] =   IA64_REG_LOC (c, UNW_IA64_AR_BSPSTORE);
  c->loc[IA64_REG_PFS] =        IA64_REG_LOC (c, UNW_IA64_AR_PFS);
  c->loc[IA64_REG_RNAT] =       IA64_REG_LOC (c, UNW_IA64_AR_RNAT);
  c->loc[IA64_REG_IP] =         IA64_REG_LOC (c, UNW_IA64_IP);
  c->loc[IA64_REG_PRI_UNAT_MEM] = IA64_NULL_LOC; /* no primary UNaT location */
  c->loc[IA64_REG_UNAT] =       IA64_REG_LOC (c, UNW_IA64_AR_UNAT);
  c->loc[IA64_REG_PR] =         IA64_REG_LOC (c, UNW_IA64_PR);
  c->loc[IA64_REG_LC] =         IA64_REG_LOC (c, UNW_IA64_AR_LC);
  c->loc[IA64_REG_FPSR] =       IA64_REG_LOC (c, UNW_IA64_AR_FPSR);

  c->loc[IA64_REG_R4] = IA64_REG_LOC (c, UNW_IA64_GR + 4);
  c->loc[IA64_REG_R5] = IA64_REG_LOC (c, UNW_IA64_GR + 5);
  c->loc[IA64_REG_R6] = IA64_REG_LOC (c, UNW_IA64_GR + 6);
  c->loc[IA64_REG_R7] = IA64_REG_LOC (c, UNW_IA64_GR + 7);

  c->loc[IA64_REG_NAT4] = IA64_REG_NAT_LOC (c, UNW_IA64_NAT + 4, &c->nat_bitnr[0]);
  c->loc[IA64_REG_NAT5] = IA64_REG_NAT_LOC (c, UNW_IA64_NAT + 5, &c->nat_bitnr[1]);
  c->loc[IA64_REG_NAT6] = IA64_REG_NAT_LOC (c, UNW_IA64_NAT + 6, &c->nat_bitnr[2]);
  c->loc[IA64_REG_NAT7] = IA64_REG_NAT_LOC (c, UNW_IA64_NAT + 7, &c->nat_bitnr[3]);

  c->loc[IA64_REG_B1] = IA64_REG_LOC (c, UNW_IA64_BR + 1);
  c->loc[IA64_REG_B2] = IA64_REG_LOC (c, UNW_IA64_BR + 2);
  c->loc[IA64_REG_B3] = IA64_REG_LOC (c, UNW_IA64_BR + 3);
  c->loc[IA64_REG_B4] = IA64_REG_LOC (c, UNW_IA64_BR + 4);
  c->loc[IA64_REG_B5] = IA64_REG_LOC (c, UNW_IA64_BR + 5);

  c->loc[IA64_REG_F2] = IA64_FPREG_LOC (c, UNW_IA64_FR + 2);
  c->loc[IA64_REG_F3] = IA64_FPREG_LOC (c, UNW_IA64_FR + 3);
  c->loc[IA64_REG_F4] = IA64_FPREG_LOC (c, UNW_IA64_FR + 4);
  c->loc[IA64_REG_F5] = IA64_FPREG_LOC (c, UNW_IA64_FR + 5);
  c->loc[IA64_REG_F16] = IA64_FPREG_LOC (c, UNW_IA64_FR + 16);
  c->loc[IA64_REG_F17] = IA64_FPREG_LOC (c, UNW_IA64_FR + 17);
  c->loc[IA64_REG_F18] = IA64_FPREG_LOC (c, UNW_IA64_FR + 18);
  c->loc[IA64_REG_F19] = IA64_FPREG_LOC (c, UNW_IA64_FR + 19);
  c->loc[IA64_REG_F20] = IA64_FPREG_LOC (c, UNW_IA64_FR + 20);
  c->loc[IA64_REG_F21] = IA64_FPREG_LOC (c, UNW_IA64_FR + 21);
  c->loc[IA64_REG_F22] = IA64_FPREG_LOC (c, UNW_IA64_FR + 22);
  c->loc[IA64_REG_F23] = IA64_FPREG_LOC (c, UNW_IA64_FR + 23);
  c->loc[IA64_REG_F24] = IA64_FPREG_LOC (c, UNW_IA64_FR + 24);
  c->loc[IA64_REG_F25] = IA64_FPREG_LOC (c, UNW_IA64_FR + 25);
  c->loc[IA64_REG_F26] = IA64_FPREG_LOC (c, UNW_IA64_FR + 26);
  c->loc[IA64_REG_F27] = IA64_FPREG_LOC (c, UNW_IA64_FR + 27);
  c->loc[IA64_REG_F28] = IA64_FPREG_LOC (c, UNW_IA64_FR + 28);
  c->loc[IA64_REG_F29] = IA64_FPREG_LOC (c, UNW_IA64_FR + 29);
  c->loc[IA64_REG_F30] = IA64_FPREG_LOC (c, UNW_IA64_FR + 30);
  c->loc[IA64_REG_F31] = IA64_FPREG_LOC (c, UNW_IA64_FR + 31);

  ret = ia64_get (c, c->loc[IA64_REG_IP], &c->ip);
  if (ret < 0)
    return ret;

  ret = ia64_get (c, c->cfm_loc, &c->cfm);
  if (ret < 0)
    return ret;

  ret = ia64_get (c, c->loc[IA64_REG_PR], &c->pr);
  if (ret < 0)
    return ret;

  c->sp = c->psp = sp;
  c->bsp = bsp;

  ret = ia64_get (c, c->loc[IA64_REG_BSPSTORE], &bspstore);
  if (ret < 0)
    return ret;

  c->rbs_curr = c->rbs_left_edge = 0;

  /* Try to find a base of the register backing-store.  We may default
     to a reasonable value (e.g., half the address-space down from
     bspstore).  If the BSPSTORE looks corrupt, we fail. */
  if ((ret = rbs_get_base (c, bspstore, &rbs_base)) < 0)
    return ret;

  c->rbs_area[0].end = bspstore;
  c->rbs_area[0].size = bspstore - rbs_base;
  c->rbs_area[0].rnat_loc = IA64_REG_LOC (c, UNW_IA64_AR_RNAT);
  Debug (10, "initial rbs-area: [0x%llx-0x%llx), rnat@%s\n",
         (long long) rbs_base, (long long) c->rbs_area[0].end,
         ia64_strloc (c->rbs_area[0].rnat_loc));

  c->pi.flags = 0;

  c->sigcontext_addr = 0;
  c->abi_marker = 0;
  c->last_abi_marker = 0;

  c->hint = 0;
  c->prev_script = 0;
  c->eh_valid_mask = 0;
  c->pi_valid = 0;
  return 0;
}
