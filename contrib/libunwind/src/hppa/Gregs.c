/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
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

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  struct dwarf_loc loc;

  switch (reg)
    {
    case UNW_HPPA_IP:
      if (write)
        c->dwarf.ip = *valp;            /* update the IP cache */
      if (c->dwarf.pi_valid && (*valp < c->dwarf.pi.start_ip
                                || *valp >= c->dwarf.pi.end_ip))
        c->dwarf.pi_valid = 0;          /* new IP outside of current proc */
      break;

    case UNW_HPPA_CFA:
    case UNW_HPPA_SP:
      if (write)
        return -UNW_EREADONLYREG;
      *valp = c->dwarf.cfa;
      return 0;

      /* Do the exception-handling register remapping: */
    case UNW_HPPA_EH0: reg = UNW_HPPA_GR + 20; break;
    case UNW_HPPA_EH1: reg = UNW_HPPA_GR + 21; break;
    case UNW_HPPA_EH2: reg = UNW_HPPA_GR + 22; break;
    case UNW_HPPA_EH3: reg = UNW_HPPA_GR + 31; break;

    default:
      break;
    }

  if ((unsigned) (reg - UNW_HPPA_GR) >= 32)
    return -UNW_EBADREG;

  loc = c->dwarf.loc[reg];

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

  if ((unsigned) (reg - UNW_HPPA_FR) >= 32)
    return -UNW_EBADREG;

  loc = c->dwarf.loc[reg];

  if (write)
    return dwarf_putfp (&c->dwarf, loc, *valp);
  else
    return dwarf_getfp (&c->dwarf, loc, valp);
}
