/* libunwind - a platform-independent unwind library
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>
   Copyright (C) 2013 Linaro Limited

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

static inline int
common_init (struct cursor *c, unsigned use_prev_instr)
{
  int ret, i;

  c->dwarf.loc[UNW_AARCH64_X0]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X0);
  c->dwarf.loc[UNW_AARCH64_X1]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X1);
  c->dwarf.loc[UNW_AARCH64_X2]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X2);
  c->dwarf.loc[UNW_AARCH64_X3]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X3);
  c->dwarf.loc[UNW_AARCH64_X4]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X4);
  c->dwarf.loc[UNW_AARCH64_X5]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X5);
  c->dwarf.loc[UNW_AARCH64_X6]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X6);
  c->dwarf.loc[UNW_AARCH64_X7]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X7);
  c->dwarf.loc[UNW_AARCH64_X8]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X8);
  c->dwarf.loc[UNW_AARCH64_X9]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X9);
  c->dwarf.loc[UNW_AARCH64_X10] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X10);
  c->dwarf.loc[UNW_AARCH64_X11] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X11);
  c->dwarf.loc[UNW_AARCH64_X12] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X12);
  c->dwarf.loc[UNW_AARCH64_X13] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X13);
  c->dwarf.loc[UNW_AARCH64_X14] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X14);
  c->dwarf.loc[UNW_AARCH64_X15] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X15);
  c->dwarf.loc[UNW_AARCH64_X16] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X16);
  c->dwarf.loc[UNW_AARCH64_X17] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X17);
  c->dwarf.loc[UNW_AARCH64_X18] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X18);
  c->dwarf.loc[UNW_AARCH64_X19] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X19);
  c->dwarf.loc[UNW_AARCH64_X20] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X20);
  c->dwarf.loc[UNW_AARCH64_X21] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X21);
  c->dwarf.loc[UNW_AARCH64_X22] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X22);
  c->dwarf.loc[UNW_AARCH64_X23] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X23);
  c->dwarf.loc[UNW_AARCH64_X24] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X24);
  c->dwarf.loc[UNW_AARCH64_X25] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X25);
  c->dwarf.loc[UNW_AARCH64_X26] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X26);
  c->dwarf.loc[UNW_AARCH64_X27] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X27);
  c->dwarf.loc[UNW_AARCH64_X28] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X28);
  c->dwarf.loc[UNW_AARCH64_X29] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X29);
  c->dwarf.loc[UNW_AARCH64_X30] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_X30);
  c->dwarf.loc[UNW_AARCH64_SP]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_SP);
  c->dwarf.loc[UNW_AARCH64_PC]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_PC);
  c->dwarf.loc[UNW_AARCH64_PSTATE] = DWARF_REG_LOC (&c->dwarf,
                                                    UNW_AARCH64_PSTATE);
  c->dwarf.loc[UNW_AARCH64_V0]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V0);
  c->dwarf.loc[UNW_AARCH64_V1]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V1);
  c->dwarf.loc[UNW_AARCH64_V2]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V2);
  c->dwarf.loc[UNW_AARCH64_V3]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V3);
  c->dwarf.loc[UNW_AARCH64_V4]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V4);
  c->dwarf.loc[UNW_AARCH64_V5]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V5);
  c->dwarf.loc[UNW_AARCH64_V6]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V6);
  c->dwarf.loc[UNW_AARCH64_V7]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V7);
  c->dwarf.loc[UNW_AARCH64_V8]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V8);
  c->dwarf.loc[UNW_AARCH64_V9]  = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V9);
  c->dwarf.loc[UNW_AARCH64_V10] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V10);
  c->dwarf.loc[UNW_AARCH64_V11] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V11);
  c->dwarf.loc[UNW_AARCH64_V12] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V12);
  c->dwarf.loc[UNW_AARCH64_V13] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V13);
  c->dwarf.loc[UNW_AARCH64_V14] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V14);
  c->dwarf.loc[UNW_AARCH64_V15] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V15);
  c->dwarf.loc[UNW_AARCH64_V16] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V16);
  c->dwarf.loc[UNW_AARCH64_V17] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V17);
  c->dwarf.loc[UNW_AARCH64_V18] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V18);
  c->dwarf.loc[UNW_AARCH64_V19] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V19);
  c->dwarf.loc[UNW_AARCH64_V20] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V20);
  c->dwarf.loc[UNW_AARCH64_V21] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V21);
  c->dwarf.loc[UNW_AARCH64_V22] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V22);
  c->dwarf.loc[UNW_AARCH64_V23] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V23);
  c->dwarf.loc[UNW_AARCH64_V24] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V24);
  c->dwarf.loc[UNW_AARCH64_V25] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V25);
  c->dwarf.loc[UNW_AARCH64_V26] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V26);
  c->dwarf.loc[UNW_AARCH64_V27] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V27);
  c->dwarf.loc[UNW_AARCH64_V28] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V28);
  c->dwarf.loc[UNW_AARCH64_V29] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V29);
  c->dwarf.loc[UNW_AARCH64_V30] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V30);
  c->dwarf.loc[UNW_AARCH64_V31] = DWARF_REG_LOC (&c->dwarf, UNW_AARCH64_V31);

  for (i = UNW_AARCH64_PSTATE + 1; i < UNW_AARCH64_V0; ++i)
    c->dwarf.loc[i] = DWARF_NULL_LOC;

  ret = dwarf_get (&c->dwarf, c->dwarf.loc[UNW_AARCH64_PC], &c->dwarf.ip);
  if (ret < 0)
    return ret;

  ret = dwarf_get (&c->dwarf, c->dwarf.loc[UNW_AARCH64_SP], &c->dwarf.cfa);
  if (ret < 0)
    return ret;

  c->sigcontext_format = AARCH64_SCF_NONE;
  c->sigcontext_addr = 0;
  c->sigcontext_sp = 0;
  c->sigcontext_pc = 0;

  c->dwarf.args_size = 0;
  c->dwarf.stash_frames = 0;
  c->dwarf.use_prev_instr = use_prev_instr;
  c->dwarf.pi_valid = 0;
  c->dwarf.pi_is_dynamic = 0;
  c->dwarf.hint = 0;
  c->dwarf.prev_rs = 0;

  return 0;
}
