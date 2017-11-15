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

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  struct dwarf_loc loc;

  switch (reg)
    {
    case UNW_PPC64_R0:
    case UNW_PPC64_R2:
    case UNW_PPC64_R3:
    case UNW_PPC64_R4:
    case UNW_PPC64_R5:
    case UNW_PPC64_R6:
    case UNW_PPC64_R7:
    case UNW_PPC64_R8:
    case UNW_PPC64_R9:
    case UNW_PPC64_R10:
    case UNW_PPC64_R11:
    case UNW_PPC64_R12:
    case UNW_PPC64_R13:
    case UNW_PPC64_R14:
    case UNW_PPC64_R15:
    case UNW_PPC64_R16:
    case UNW_PPC64_R17:
    case UNW_PPC64_R18:
    case UNW_PPC64_R19:
    case UNW_PPC64_R20:
    case UNW_PPC64_R21:
    case UNW_PPC64_R22:
    case UNW_PPC64_R23:
    case UNW_PPC64_R24:
    case UNW_PPC64_R25:
    case UNW_PPC64_R26:
    case UNW_PPC64_R27:
    case UNW_PPC64_R28:
    case UNW_PPC64_R29:
    case UNW_PPC64_R30:
    case UNW_PPC64_R31:
    case UNW_PPC64_LR:
    case UNW_PPC64_CTR:
    case UNW_PPC64_CR0:
    case UNW_PPC64_CR1:
    case UNW_PPC64_CR2:
    case UNW_PPC64_CR3:
    case UNW_PPC64_CR4:
    case UNW_PPC64_CR5:
    case UNW_PPC64_CR6:
    case UNW_PPC64_CR7:
    case UNW_PPC64_VRSAVE:
    case UNW_PPC64_VSCR:
    case UNW_PPC64_SPE_ACC:
    case UNW_PPC64_SPEFSCR:
      loc = c->dwarf.loc[reg];
      break;

    case UNW_TDEP_IP:
      if (write)
        {
          c->dwarf.ip = *valp;  /* update the IP cache */
          if (c->dwarf.pi_valid && (*valp < c->dwarf.pi.start_ip
                                    || *valp >= c->dwarf.pi.end_ip))
            c->dwarf.pi_valid = 0;      /* new IP outside of current proc */
        }
      else
        *valp = c->dwarf.ip;
      return 0;

    case UNW_TDEP_SP:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = c->dwarf.cfa;
      return 0;

    default:
      return -UNW_EBADREG;
      break;
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
  struct dwarf_loc loc;

  if ((unsigned) (reg - UNW_PPC64_F0) < 32)
  {
    loc = c->dwarf.loc[reg];
    if (write)
      return dwarf_putfp (&c->dwarf, loc, *valp);
    else
      return dwarf_getfp (&c->dwarf, loc, valp);
  }
  else
  if ((unsigned) (reg - UNW_PPC64_V0) < 32)
  {
    loc = c->dwarf.loc[reg];
    if (write)
      return dwarf_putvr (&c->dwarf, loc, *valp);
    else
      return dwarf_getvr (&c->dwarf, loc, valp);
  }

  return -UNW_EBADREG;
}

