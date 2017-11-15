/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery

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

/* FIXME: The following is probably unfinished and/or at least partly bogus.  */

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  dwarf_loc_t loc = DWARF_NULL_LOC;
  
  switch (reg)
    {
    case UNW_MIPS_R0:
    case UNW_MIPS_R1:
    case UNW_MIPS_R2:
    case UNW_MIPS_R3:
    case UNW_MIPS_R4:
    case UNW_MIPS_R5:
    case UNW_MIPS_R6:
    case UNW_MIPS_R7:
    case UNW_MIPS_R8:
    case UNW_MIPS_R9:
    case UNW_MIPS_R10:
    case UNW_MIPS_R11:
    case UNW_MIPS_R12:
    case UNW_MIPS_R13:
    case UNW_MIPS_R14:
    case UNW_MIPS_R15:
    case UNW_MIPS_R16:
    case UNW_MIPS_R17:
    case UNW_MIPS_R18:
    case UNW_MIPS_R19:
    case UNW_MIPS_R20:
    case UNW_MIPS_R21:
    case UNW_MIPS_R22:
    case UNW_MIPS_R23:
    case UNW_MIPS_R24:
    case UNW_MIPS_R25:
    case UNW_MIPS_R26:
    case UNW_MIPS_R27:
    case UNW_MIPS_R28:
    case UNW_MIPS_R29:
    case UNW_MIPS_R30:
    case UNW_MIPS_R31:
      loc = c->dwarf.loc[reg - UNW_MIPS_R0];
      break;

    case UNW_MIPS_PC:
      if (write)
	c->dwarf.ip = *valp;            /* update the IP cache */
      loc = c->dwarf.loc[reg];
      break;

    case UNW_MIPS_CFA:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = c->dwarf.cfa;
      return 0;

    /* FIXME: IP?  Copro & shadow registers?  */

    default:
      Debug (1, "bad register number %u\n", reg);
      return -UNW_EBADREG;
    }

  if (write)
    return dwarf_put (&c->dwarf, loc, *valp);
  else
    return dwarf_get (&c->dwarf, loc, valp);
}

/* FIXME for MIPS.  */

HIDDEN int
tdep_access_fpreg (struct cursor *c, unw_regnum_t reg, unw_fpreg_t *valp,
                   int write)
{
  Debug (1, "bad register number %u\n", reg);
  return -UNW_EBADREG;
}
