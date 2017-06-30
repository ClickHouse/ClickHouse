/* libunwind - a platform-independent unwind library
   Copyright (C) 2002 Hewlett-Packard Co
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

static inline int
common_init (struct cursor *c, unsigned use_prev_instr)
{
  int ret, i;

  c->dwarf.loc[EAX] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EAX);
  c->dwarf.loc[ECX] = DWARF_REG_LOC (&c->dwarf, UNW_X86_ECX);
  c->dwarf.loc[EDX] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EDX);
  c->dwarf.loc[EBX] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EBX);
  c->dwarf.loc[ESP] = DWARF_REG_LOC (&c->dwarf, UNW_X86_ESP);
  c->dwarf.loc[EBP] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EBP);
  c->dwarf.loc[ESI] = DWARF_REG_LOC (&c->dwarf, UNW_X86_ESI);
  c->dwarf.loc[EDI] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EDI);
  c->dwarf.loc[EIP] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EIP);
  c->dwarf.loc[EFLAGS] = DWARF_REG_LOC (&c->dwarf, UNW_X86_EFLAGS);
  c->dwarf.loc[TRAPNO] = DWARF_REG_LOC (&c->dwarf, UNW_X86_TRAPNO);
  c->dwarf.loc[ST0] = DWARF_REG_LOC (&c->dwarf, UNW_X86_ST0);
  for (i = ST0 + 1; i < DWARF_NUM_PRESERVED_REGS; ++i)
    c->dwarf.loc[i] = DWARF_NULL_LOC;

  ret = dwarf_get (&c->dwarf, c->dwarf.loc[EIP], &c->dwarf.ip);
  if (ret < 0)
    return ret;

  ret = dwarf_get (&c->dwarf, DWARF_REG_LOC (&c->dwarf, UNW_X86_ESP),
                   &c->dwarf.cfa);
  if (ret < 0)
    return ret;

  c->sigcontext_format = X86_SCF_NONE;
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
