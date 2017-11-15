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

#include "unwind_i.h"
#include <stdio.h>
#include "offsets.h"

#ifdef __linux__
#include <sys/syscall.h>
#include <arch/abi.h>
#else
# error "Only support Linux!"
#endif

#define  MOVELI_R10_RT_SIGRETURN                         \
  ( 0x000007e051483000ULL    |                           \
    ((unsigned long)__NR_rt_sigreturn << 43) |           \
    ((unsigned long)TREG_SYSCALL_NR << 31) )
#define  SWINT1      0x286b180051485000ULL

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor*) cursor;
  unw_word_t w0, w1, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  ip = c->dwarf.ip;

  if (!ip || !a->access_mem || (ip & (sizeof(unw_word_t) - 1)))
    return 0;

  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0)
    return ret;

  if ((ret = (*a->access_mem) (as, ip + 8, &w1, 0, arg)) < 0)
    return ret;

  /* Return 1 if the IP points to a RT sigreturn sequence. */
  if (w0 == MOVELI_R10_RT_SIGRETURN &&
      w1 ==  SWINT1)
    {
      return 1;
    }
  return 0;
}


PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  int i;
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t sc_addr, sp, sp_addr = c->dwarf.cfa;
  struct dwarf_loc sp_loc = DWARF_LOC (sp_addr, 0);
  int ret;

  if ((ret = dwarf_get (&c->dwarf, sp_loc, &sp)) < 0)
    return -UNW_EUNSPEC;

  /* Save the SP and PC to be able to return execution at this point
     later in time (unw_resume).  */
  c->sigcontext_sp = c->dwarf.cfa;
  c->sigcontext_pc = c->dwarf.ip;

  c->sigcontext_addr = sp_addr + sizeof (siginfo_t) +
    C_ABI_SAVE_AREA_SIZE;
  sc_addr = c->sigcontext_addr + LINUX_UC_MCONTEXT_OFF;

  /* Update the dwarf cursor.
     Set the location of the registers to the corresponding addresses of the
     uc_mcontext / sigcontext structure contents.  */

#define  SC_REG_OFFSET(X)   (8 * X)

  for (i = UNW_TILEGX_R0; i <= UNW_TILEGX_R55; i++)
    {
      c->dwarf.loc[i] = DWARF_LOC (sc_addr + SC_REG_OFFSET(i), 0);
    }

  /* Set SP/CFA and PC/IP.  */
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_TILEGX_R54], &c->dwarf.cfa);
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_TILEGX_R55], &c->dwarf.ip);

  return 1;
}
