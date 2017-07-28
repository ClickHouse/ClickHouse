/* libunwind - a platform-independent unwind library
   Copyright (C) 2006-2007 IBM
   Contributed by
     Corey Ashford <cjashfor@us.ibm.com>
     Jose Flavio Aguilar Paulino <jflavio@br.ibm.com> <joseflavio@gmail.com>

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

/* Here is the "common" init, for remote and local debuging" */

static inline int
common_init_ppc32 (struct cursor *c, unsigned use_prev_instr)
{
  int ret;
  int i;

  for (i = UNW_PPC32_R0; i <= UNW_PPC32_R31; i++) {
    c->dwarf.loc[i] = DWARF_REG_LOC (&c->dwarf, i);
  }
  for (i = UNW_PPC32_F0; i <= UNW_PPC32_F31; i++) {
    c->dwarf.loc[i] = DWARF_FPREG_LOC (&c->dwarf, i);
  }

  c->dwarf.loc[UNW_PPC32_CTR] = DWARF_REG_LOC (&c->dwarf, UNW_PPC32_CTR);
  c->dwarf.loc[UNW_PPC32_XER] = DWARF_REG_LOC (&c->dwarf, UNW_PPC32_XER);
  c->dwarf.loc[UNW_PPC32_CCR] = DWARF_REG_LOC (&c->dwarf, UNW_PPC32_CCR);
  c->dwarf.loc[UNW_PPC32_LR] = DWARF_REG_LOC (&c->dwarf, UNW_PPC32_LR);
  c->dwarf.loc[UNW_PPC32_FPSCR] = DWARF_REG_LOC (&c->dwarf, UNW_PPC32_FPSCR);

  ret = dwarf_get (&c->dwarf, c->dwarf.loc[UNW_PPC32_LR], &c->dwarf.ip);
  if (ret < 0)
    return ret;

  ret = dwarf_get (&c->dwarf, DWARF_REG_LOC (&c->dwarf, UNW_PPC32_R1),
                   &c->dwarf.cfa);
  if (ret < 0)
    return ret;

  c->sigcontext_format = PPC_SCF_NONE;
  c->sigcontext_addr = 0;

  c->dwarf.args_size = 0;
  c->dwarf.stash_frames = 0;
  c->dwarf.use_prev_instr = use_prev_instr;
  c->dwarf.pi_valid = 0;
  c->dwarf.pi_is_dynamic = 0;
  c->dwarf.hint = 0;
  c->dwarf.prev_rs = 0;

  return 0;
}
