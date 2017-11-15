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

#include <assert.h>

#include "unwind_i.h"

HIDDEN struct ia64_global_unwind_state unw =
  {
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .save_order = {
      IA64_REG_IP, IA64_REG_PFS, IA64_REG_PSP, IA64_REG_PR,
      IA64_REG_UNAT, IA64_REG_LC, IA64_REG_FPSR, IA64_REG_PRI_UNAT_GR
    },
#if UNW_DEBUG
    .preg_name = {
      "pri_unat_gr", "pri_unat_mem", "psp", "bsp", "bspstore",
      "ar.pfs", "ar.rnat", "rp",
      "r4", "r5", "r6", "r7",
      "nat4", "nat5", "nat6", "nat7",
      "ar.unat", "pr", "ar.lc", "ar.fpsr",
      "b1", "b2", "b3", "b4", "b5",
      "f2", "f3", "f4", "f5",
      "f16", "f17", "f18", "f19", "f20", "f21", "f22", "f23",
      "f24", "f25", "f26", "f27", "f28", "f29", "f30", "f31"
    }
#endif
};

HIDDEN void
tdep_init (void)
{
  const uint8_t f1_bytes[16] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff,
    0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  const uint8_t nat_val_bytes[16] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xfe,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  const uint8_t int_val_bytes[16] = {
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x3e,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  intrmask_t saved_mask;
  uint8_t *lep, *bep;
  long i;

  sigfillset (&unwi_full_mask);

  lock_acquire (&unw.lock, saved_mask);
  {
    if (tdep_init_done)
      /* another thread else beat us to it... */
      goto out;

    mi_init ();

    mempool_init (&unw.reg_state_pool, sizeof (struct ia64_reg_state), 0);
    mempool_init (&unw.labeled_state_pool,
                  sizeof (struct ia64_labeled_state), 0);

    unw.read_only.r0 = 0;
    unw.read_only.f0.raw.bits[0] = 0;
    unw.read_only.f0.raw.bits[1] = 0;

    lep = (uint8_t *) &unw.read_only.f1_le + 16;
    bep = (uint8_t *) &unw.read_only.f1_be;
    for (i = 0; i < 16; ++i)
      {
        *--lep = f1_bytes[i];
        *bep++ = f1_bytes[i];
      }

    lep = (uint8_t *) &unw.nat_val_le + 16;
    bep = (uint8_t *) &unw.nat_val_be;
    for (i = 0; i < 16; ++i)
      {
        *--lep = nat_val_bytes[i];
        *bep++ = nat_val_bytes[i];
      }

    lep = (uint8_t *) &unw.int_val_le + 16;
    bep = (uint8_t *) &unw.int_val_be;
    for (i = 0; i < 16; ++i)
      {
        *--lep = int_val_bytes[i];
        *bep++ = int_val_bytes[i];
      }

    assert (8*sizeof(unw_hash_index_t) >= IA64_LOG_UNW_HASH_SIZE);

#ifndef UNW_REMOTE_ONLY
    ia64_local_addr_space_init ();
#endif
    tdep_init_done = 1; /* signal that we're initialized... */
  }
 out:
  lock_release (&unw.lock, saved_mask);
}
