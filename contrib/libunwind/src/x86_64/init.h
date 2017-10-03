/* libunwind - a platform-independent unwind library
   Copyright (C) 2002 Hewlett-Packard Co
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

/* Avoid a trip to x86_64_r_uc_addr() for purely local initialisation. */
#if defined UNW_LOCAL_ONLY && defined __linux
# define REG_INIT_LOC(c, rlc, ruc) \
    DWARF_LOC ((unw_word_t) &c->uc->uc_mcontext.gregs[REG_ ## ruc], 0)

#elif defined UNW_LOCAL_ONLY && defined __FreeBSD__
# define REG_INIT_LOC(c, rlc, ruc) \
    DWARF_LOC ((unw_word_t) &c->uc->uc_mcontext.mc_ ## rlc, 0)

#else
# define REG_INIT_LOC(c, rlc, ruc) \
    DWARF_REG_LOC (&c->dwarf, UNW_X86_64_ ## ruc)
#endif

static inline int
common_init (struct cursor *c, unsigned use_prev_instr)
{
  int ret;

  c->dwarf.loc[RAX] = REG_INIT_LOC(c, rax, RAX);
  c->dwarf.loc[RDX] = REG_INIT_LOC(c, rdx, RDX);
  c->dwarf.loc[RCX] = REG_INIT_LOC(c, rcx, RCX);
  c->dwarf.loc[RBX] = REG_INIT_LOC(c, rbx, RBX);
  c->dwarf.loc[RSI] = REG_INIT_LOC(c, rsi, RSI);
  c->dwarf.loc[RDI] = REG_INIT_LOC(c, rdi, RDI);
  c->dwarf.loc[RBP] = REG_INIT_LOC(c, rbp, RBP);
  c->dwarf.loc[RSP] = REG_INIT_LOC(c, rsp, RSP);
  c->dwarf.loc[R8]  = REG_INIT_LOC(c, r8,  R8);
  c->dwarf.loc[R9]  = REG_INIT_LOC(c, r9,  R9);
  c->dwarf.loc[R10] = REG_INIT_LOC(c, r10, R10);
  c->dwarf.loc[R11] = REG_INIT_LOC(c, r11, R11);
  c->dwarf.loc[R12] = REG_INIT_LOC(c, r12, R12);
  c->dwarf.loc[R13] = REG_INIT_LOC(c, r13, R13);
  c->dwarf.loc[R14] = REG_INIT_LOC(c, r14, R14);
  c->dwarf.loc[R15] = REG_INIT_LOC(c, r15, R15);
  c->dwarf.loc[RIP] = REG_INIT_LOC(c, rip, RIP);

  ret = dwarf_get (&c->dwarf, c->dwarf.loc[RIP], &c->dwarf.ip);
  if (ret < 0)
    return ret;

  ret = dwarf_get (&c->dwarf, DWARF_REG_LOC (&c->dwarf, UNW_X86_64_RSP),
                   &c->dwarf.cfa);
  if (ret < 0)
    return ret;

  c->sigcontext_format = X86_64_SCF_NONE;
  c->sigcontext_addr = 0;

  c->dwarf.args_size = 0;
  c->dwarf.stash_frames = 0;
  c->dwarf.use_prev_instr = use_prev_instr;
  c->dwarf.pi_valid = 0;
  c->dwarf.pi_is_dynamic = 0;
  c->dwarf.hint = 0;
  c->dwarf.prev_rs = 0;
  c->dwarf.eh_valid_mask = 0;

  return 0;
}
