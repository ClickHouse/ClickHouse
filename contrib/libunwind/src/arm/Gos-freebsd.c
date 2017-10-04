/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright 2011 Linaro Limited
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>
   Copyright 2015 The FreeBSD Foundation

   Portions of this software were developed by Konstantin Belousov
   under sponsorship from the FreeBSD Foundation.

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

#include <stdio.h>
#include <signal.h>
#include "unwind_i.h"
#include "offsets.h"
#include "ex_tables.h"

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret, fmt;
  unw_word_t sc_addr, sp, sp_addr = c->dwarf.cfa;
  struct dwarf_loc sp_loc = DWARF_LOC (sp_addr, 0);

  if ((ret = dwarf_get (&c->dwarf, sp_loc, &sp)) < 0)
    return -UNW_EUNSPEC;
  fmt = unw_is_signal_frame(cursor);

  c->dwarf.pi_valid = 0;

  if (fmt == UNW_ARM_FRAME_SYSCALL)
   {
    c->sigcontext_format = ARM_SCF_FREEBSD_SYSCALL;
    c->frame_info.frame_type = UNW_ARM_FRAME_SYSCALL;
    c->frame_info.cfa_reg_offset = 0;
    c->dwarf.loc[UNW_ARM_R7] = c->dwarf.loc[UNW_ARM_R12];
    dwarf_get (&c->dwarf, c->dwarf.loc[UNW_ARM_R14], &c->dwarf.ip);
    return 1;
   }

  c->sigcontext_format = ARM_SCF_FREEBSD_SIGFRAME;
  sc_addr = sp_addr;

  /* Save the SP and PC to be able to return execution at this point
     later in time (unw_resume).  */
  c->sigcontext_sp = c->dwarf.cfa;
  c->sigcontext_pc = c->dwarf.ip;

  c->sigcontext_addr = sc_addr;
  c->frame_info.frame_type = UNW_ARM_FRAME_SIGRETURN;
  c->frame_info.cfa_reg_offset = sc_addr - sp_addr;

  /* Update the dwarf cursor.
     Set the location of the registers to the corresponding addresses of the
     uc_mcontext / sigcontext structure contents.  */
#define ROFF(n)	(FREEBSD_SC_UCONTEXT_OFF + FREEBSD_UC_MCONTEXT_OFF + \
  FREEBSD_MC_R0_OFF + (n) * 4)
#define SL(n) \
  c->dwarf.loc[UNW_ARM_R ## n] = DWARF_LOC (sc_addr + ROFF(n), 0);
  SL(0); SL(1); SL(2); SL(3); SL(4); SL(5); SL(6); SL(7);
  SL(8); SL(9); SL(10); SL(11); SL(12); SL(13); SL(14); SL(15);
#undef SL
#undef ROFF

  /* Set SP/CFA and PC/IP.  */
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_ARM_R13], &c->dwarf.cfa);
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_ARM_R15], &c->dwarf.ip);

  return 1;
}

/* Returns 1 in case of a non-RT signal frame and 2 in case of a RT signal
   frame. */
PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, w2, w3, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  ip = c->dwarf.ip;

  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0)
    return ret;
  if ((ret = (*a->access_mem) (as, ip + 4, &w1, 0, arg)) < 0)
    return ret;
  if ((ret = (*a->access_mem) (as, ip + 8, &w2, 0, arg)) < 0)
    return ret;
  if ((ret = (*a->access_mem) (as, ip + 12, &w3, 0, arg)) < 0)
    return ret;

  if (w0 == 0xe1a0000d && w1 == 0xe2800040 && w2 == 0xe59f700c &&
    w3 == 0xef0001a1)
     return UNW_ARM_FRAME_SIGRETURN;

  if ((ret = (*a->access_mem) (as, ip - 4, &w0, 0, arg)) < 0)
    return ret;
  if (w0 == 0xef000000)
    return UNW_ARM_FRAME_SYSCALL;

  return 0;
}
