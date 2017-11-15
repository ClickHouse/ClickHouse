/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright 2011 Linaro Limited
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>

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
#include "offsets.h"

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;
  unw_word_t sc_addr, sp, sp_addr = c->dwarf.cfa;
  struct dwarf_loc sp_loc = DWARF_LOC (sp_addr, 0);

  if ((ret = dwarf_get (&c->dwarf, sp_loc, &sp)) < 0)
    return -UNW_EUNSPEC;

  ret = unw_is_signal_frame (cursor);
  Debug(1, "unw_is_signal_frame()=%d\n", ret);

  /* Save the SP and PC to be able to return execution at this point
     later in time (unw_resume).  */
  c->sigcontext_sp = c->dwarf.cfa;
  c->sigcontext_pc = c->dwarf.ip;

  if (ret == 1)
    {
      /* Handle non-RT signal frame. */
      c->sigcontext_format = SH_SCF_LINUX_SIGFRAME;
      sc_addr = sp_addr;
    }
  else if (ret == 2)
    {
      /* Handle RT signal frame. */
      c->sigcontext_format = SH_SCF_LINUX_RT_SIGFRAME;
      sc_addr = sp_addr + sizeof (siginfo_t) + LINUX_UC_MCONTEXT_OFF;
    }
  else
    return -UNW_EUNSPEC;

  c->sigcontext_addr = sc_addr;

  /* Update the dwarf cursor.
     Set the location of the registers to the corresponding addresses of the
     uc_mcontext / sigcontext structure contents.  */
  c->dwarf.loc[UNW_SH_R0]  = DWARF_LOC (sc_addr + LINUX_SC_R0_OFF, 0);
  c->dwarf.loc[UNW_SH_R1]  = DWARF_LOC (sc_addr + LINUX_SC_R1_OFF, 0);
  c->dwarf.loc[UNW_SH_R2]  = DWARF_LOC (sc_addr + LINUX_SC_R2_OFF, 0);
  c->dwarf.loc[UNW_SH_R3]  = DWARF_LOC (sc_addr + LINUX_SC_R3_OFF, 0);
  c->dwarf.loc[UNW_SH_R4]  = DWARF_LOC (sc_addr + LINUX_SC_R4_OFF, 0);
  c->dwarf.loc[UNW_SH_R5]  = DWARF_LOC (sc_addr + LINUX_SC_R5_OFF, 0);
  c->dwarf.loc[UNW_SH_R6]  = DWARF_LOC (sc_addr + LINUX_SC_R6_OFF, 0);
  c->dwarf.loc[UNW_SH_R7]  = DWARF_LOC (sc_addr + LINUX_SC_R7_OFF, 0);
  c->dwarf.loc[UNW_SH_R8]  = DWARF_LOC (sc_addr + LINUX_SC_R8_OFF, 0);
  c->dwarf.loc[UNW_SH_R9]  = DWARF_LOC (sc_addr + LINUX_SC_R9_OFF, 0);
  c->dwarf.loc[UNW_SH_R10] = DWARF_LOC (sc_addr + LINUX_SC_R10_OFF, 0);
  c->dwarf.loc[UNW_SH_R11] = DWARF_LOC (sc_addr + LINUX_SC_R11_OFF, 0);
  c->dwarf.loc[UNW_SH_R12] = DWARF_LOC (sc_addr + LINUX_SC_R12_OFF, 0);
  c->dwarf.loc[UNW_SH_R13] = DWARF_LOC (sc_addr + LINUX_SC_R13_OFF, 0);
  c->dwarf.loc[UNW_SH_R14] = DWARF_LOC (sc_addr + LINUX_SC_R14_OFF, 0);
  c->dwarf.loc[UNW_SH_R15] = DWARF_LOC (sc_addr + LINUX_SC_R15_OFF, 0);
  c->dwarf.loc[UNW_SH_PR]  = DWARF_LOC (sc_addr + LINUX_SC_PR_OFF, 0);
  c->dwarf.loc[UNW_SH_PC]  = DWARF_LOC (sc_addr + LINUX_SC_PC_OFF, 0);

  /* Set SP/CFA and PC/IP.  */
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_SH_R15], &c->dwarf.cfa);
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_SH_PC], &c->dwarf.ip);

  c->dwarf.pi_valid = 0;

  return 1;
}

PROTECTED int
unw_step (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  Debug (1, "(cursor=%p)\n", c);

  if (unw_is_signal_frame (cursor) > 0)
    return unw_handle_signal_frame (cursor);

  ret = dwarf_step (&c->dwarf);

  if (unlikely (ret == -UNW_ESTOPUNWIND))
    return ret;

  if (unlikely (ret < 0))
    return 0;

  return (c->dwarf.ip == 0) ? 0 : 1;
}
