/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

   Modified for x86_64 by Max Asbock <masbock@us.ibm.com>

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
#include <signal.h>

/* Recognise PLT entries such as:
     3bdf0: ff 25 e2 49 13 00 jmpq   *0x1349e2(%rip)
     3bdf6: 68 ae 03 00 00    pushq  $0x3ae
     3bdfb: e9 00 c5 ff ff    jmpq   38300 <_init+0x18> */
static int
is_plt_entry (struct dwarf_cursor *c)
{
  unw_word_t w0, w1;
  unw_accessors_t *a;
  int ret;

  a = unw_get_accessors (c->as);
  if ((ret = (*a->access_mem) (c->as, c->ip, &w0, 0, c->as_arg)) < 0
      || (ret = (*a->access_mem) (c->as, c->ip + 8, &w1, 0, c->as_arg)) < 0)
    return 0;

  ret = (((w0 & 0xffff) == 0x25ff)
         && (((w0 >> 48) & 0xff) == 0x68)
         && (((w1 >> 24) & 0xff) == 0xe9));

  Debug (14, "ip=0x%lx => 0x%016lx 0x%016lx, ret = %d\n", c->ip, w0, w1, ret);
  return ret;
}

PROTECTED int
unw_step (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret, i;

#if CONSERVATIVE_CHECKS
  int val = c->validate;
  c->validate = 1;
#endif

  Debug (1, "(cursor=%p, ip=0x%016lx, cfa=0x%016lx)\n",
         c, c->dwarf.ip, c->dwarf.cfa);

  /* Try DWARF-based unwinding... */
  c->sigcontext_format = X86_64_SCF_NONE;
  ret = dwarf_step (&c->dwarf);

#if CONSERVATIVE_CHECKS
  c->validate = val;
#endif

  if (ret < 0 && ret != -UNW_ENOINFO)
    {
      Debug (2, "returning %d\n", ret);
      return ret;
    }

  if (likely (ret >= 0))
    {
      /* x86_64 ABI specifies that end of call-chain is marked with a
         NULL RBP or undefined return address  */
      if (DWARF_IS_NULL_LOC (c->dwarf.loc[RBP]))
          {
            c->dwarf.ip = 0;
            ret = 0;
          }
    }
  else
    {
      /* DWARF failed.  There isn't much of a usable frame-chain on x86-64,
         but we do need to handle two special-cases:

          (i) signal trampoline: Old kernels and older libcs don't
              export the vDSO needed to get proper unwind info for the
              trampoline.  Recognize that case by looking at the code
              and filling in things by hand.

          (ii) PLT (shared-library) call-stubs: PLT stubs are invoked
              via CALLQ.  Try this for all non-signal trampoline
              code.  */

      unw_word_t prev_ip = c->dwarf.ip, prev_cfa = c->dwarf.cfa;
      struct dwarf_loc rbp_loc, rsp_loc, rip_loc;

      /* We could get here because of missing/bad unwind information.
         Validate all addresses before dereferencing. */
      c->validate = 1;

      Debug (13, "dwarf_step() failed (ret=%d), trying frame-chain\n", ret);

      if (unw_is_signal_frame (cursor) > 0)
        {
          ret = unw_handle_signal_frame(cursor);
          if (ret < 0)
            {
              Debug (2, "returning 0\n");
              return 0;
            }
        }
      else if (is_plt_entry (&c->dwarf))
        {
          /* Like regular frame, CFA = RSP+8, RA = [CFA-8], no regs saved. */
          Debug (2, "found plt entry\n");
          c->frame_info.cfa_reg_offset = 8;
          c->frame_info.cfa_reg_rsp = -1;
          c->frame_info.frame_type = UNW_X86_64_FRAME_STANDARD;
          c->dwarf.loc[RIP] = DWARF_LOC (c->dwarf.cfa, 0);
          c->dwarf.cfa += 8;
        }
      else if (DWARF_IS_NULL_LOC (c->dwarf.loc[RBP]))
        {
          for (i = 0; i < DWARF_NUM_PRESERVED_REGS; ++i)
            c->dwarf.loc[i] = DWARF_NULL_LOC;
        }
      else
        {
          unw_word_t rbp;

          ret = dwarf_get (&c->dwarf, c->dwarf.loc[RBP], &rbp);
          if (ret < 0)
            {
              Debug (2, "returning %d [RBP=0x%lx]\n", ret,
                     DWARF_GET_LOC (c->dwarf.loc[RBP]));
              return ret;
            }

          if (!rbp)
            {
              /* Looks like we may have reached the end of the call-chain.  */
              rbp_loc = DWARF_NULL_LOC;
              rsp_loc = DWARF_NULL_LOC;
              rip_loc = DWARF_NULL_LOC;
            }
          else
            {
              unw_word_t rbp1 = 0;
              rbp_loc = DWARF_LOC(rbp, 0);
              rsp_loc = DWARF_NULL_LOC;
              rip_loc = DWARF_LOC (rbp + 8, 0);
              ret = dwarf_get (&c->dwarf, rbp_loc, &rbp1);
              Debug (1, "[RBP=0x%lx] = 0x%lx (cfa = 0x%lx) -> 0x%lx\n",
                     (unsigned long) DWARF_GET_LOC (c->dwarf.loc[RBP]),
                     rbp, c->dwarf.cfa, rbp1);

              /* Heuristic to determine incorrect guess.  For RBP to be a
                 valid frame it needs to be above current CFA, but don't
                 let it go more than a little.  Note that we can't deduce
                 anything about new RBP (rbp1) since it may not be a frame
                 pointer in the frame above.  Just check we get the value. */
              if (ret < 0
                  || rbp < c->dwarf.cfa
                  || (rbp - c->dwarf.cfa) > 0x4000)
                {
                  rip_loc = DWARF_NULL_LOC;
                  rbp_loc = DWARF_NULL_LOC;
                }

              c->frame_info.frame_type = UNW_X86_64_FRAME_GUESSED;
              c->frame_info.cfa_reg_rsp = 0;
              c->frame_info.cfa_reg_offset = 16;
              c->frame_info.rbp_cfa_offset = -16;
              c->dwarf.cfa += 16;
            }

          /* Mark all registers unsaved */
          for (i = 0; i < DWARF_NUM_PRESERVED_REGS; ++i)
            c->dwarf.loc[i] = DWARF_NULL_LOC;

          c->dwarf.loc[RBP] = rbp_loc;
          c->dwarf.loc[RSP] = rsp_loc;
          c->dwarf.loc[RIP] = rip_loc;
          c->dwarf.use_prev_instr = 1;
        }

      if (DWARF_IS_NULL_LOC (c->dwarf.loc[RBP]))
        {
          ret = 0;
          Debug (2, "NULL %%rbp loc, returning %d\n", ret);
          return ret;
        }
      if (!DWARF_IS_NULL_LOC (c->dwarf.loc[RIP]))
        {
          ret = dwarf_get (&c->dwarf, c->dwarf.loc[RIP], &c->dwarf.ip);
          Debug (1, "Frame Chain [RIP=0x%Lx] = 0x%Lx\n",
                     (unsigned long long) DWARF_GET_LOC (c->dwarf.loc[RIP]),
                     (unsigned long long) c->dwarf.ip);
          if (ret < 0)
            {
              Debug (2, "returning %d\n", ret);
              return ret;
            }
          ret = 1;
        }
      else
        c->dwarf.ip = 0;

      if (c->dwarf.ip == prev_ip && c->dwarf.cfa == prev_cfa)
        return -UNW_EBADFRAME;
    }
  Debug (2, "returning %d\n", ret);
  return ret;
}
