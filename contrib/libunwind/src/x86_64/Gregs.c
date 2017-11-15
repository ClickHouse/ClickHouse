/* libunwind - a platform-independent unwind library
   Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
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

#if 0
static inline dwarf_loc_t
linux_scratch_loc (struct cursor *c, unw_regnum_t reg)
{
  unw_word_t addr = c->sigcontext_addr;

  switch (c->sigcontext_format)
    {
    case X86_64_SCF_NONE:
      return DWARF_REG_LOC (&c->dwarf, reg);

    case X86_64_SCF_LINUX_RT_SIGFRAME:
      addr += LINUX_UC_MCONTEXT_OFF;
      break;

    case X86_64_SCF_FREEBSD_SIGFRAME:
      addr += FREEBSD_UC_MCONTEXT_OFF;
      break;
    }

  return DWARF_REG_LOC (&c->dwarf, reg);

}

HIDDEN dwarf_loc_t
x86_64_scratch_loc (struct cursor *c, unw_regnum_t reg)
{
  if (c->sigcontext_addr)
    return linux_scratch_loc (c, reg);
  else
    return DWARF_REG_LOC (&c->dwarf, reg);
}
#endif

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  dwarf_loc_t loc = DWARF_NULL_LOC;
  unsigned int mask;
  int arg_num;

  switch (reg)
    {

    case UNW_X86_64_RIP:
      if (write)
        c->dwarf.ip = *valp;            /* also update the RIP cache */
      loc = c->dwarf.loc[RIP];
      break;

    case UNW_X86_64_CFA:
    case UNW_X86_64_RSP:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = c->dwarf.cfa;
      return 0;

    case UNW_X86_64_RAX:
    case UNW_X86_64_RDX:
      arg_num = reg - UNW_X86_64_RAX;
      mask = (1 << arg_num);
      if (write)
        {
          c->dwarf.eh_args[arg_num] = *valp;
          c->dwarf.eh_valid_mask |= mask;
          return 0;
        }
      else if ((c->dwarf.eh_valid_mask & mask) != 0)
        {
          *valp = c->dwarf.eh_args[arg_num];
          return 0;
        }
      else
        loc = c->dwarf.loc[(reg == UNW_X86_64_RAX) ? RAX : RDX];
      break;

    case UNW_X86_64_RCX: loc = c->dwarf.loc[RCX]; break;
    case UNW_X86_64_RBX: loc = c->dwarf.loc[RBX]; break;

    case UNW_X86_64_RBP: loc = c->dwarf.loc[RBP]; break;
    case UNW_X86_64_RSI: loc = c->dwarf.loc[RSI]; break;
    case UNW_X86_64_RDI: loc = c->dwarf.loc[RDI]; break;
    case UNW_X86_64_R8: loc = c->dwarf.loc[R8]; break;
    case UNW_X86_64_R9: loc = c->dwarf.loc[R9]; break;
    case UNW_X86_64_R10: loc = c->dwarf.loc[R10]; break;
    case UNW_X86_64_R11: loc = c->dwarf.loc[R11]; break;
    case UNW_X86_64_R12: loc = c->dwarf.loc[R12]; break;
    case UNW_X86_64_R13: loc = c->dwarf.loc[R13]; break;
    case UNW_X86_64_R14: loc = c->dwarf.loc[R14]; break;
    case UNW_X86_64_R15: loc = c->dwarf.loc[R15]; break;

    default:
      Debug (1, "bad register number %u\n", reg);
      return -UNW_EBADREG;
    }

  if (write)
    return dwarf_put (&c->dwarf, loc, *valp);
  else
    return dwarf_get (&c->dwarf, loc, valp);
}

HIDDEN int
tdep_access_fpreg (struct cursor *c, unw_regnum_t reg, unw_fpreg_t *valp,
                   int write)
{
      return -UNW_EBADREG;
}
