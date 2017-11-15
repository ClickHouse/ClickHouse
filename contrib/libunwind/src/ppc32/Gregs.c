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
      break;
    }

  /* make sure it's not an FP or VR register */
  if ((((unsigned) (reg - UNW_PPC32_F0)) <= 31))
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

  if ((unsigned) (reg - UNW_PPC32_F0) < 32)
  {
    loc = c->dwarf.loc[reg];
    if (write)
      return dwarf_putfp (&c->dwarf, loc, *valp);
    else
      return dwarf_getfp (&c->dwarf, loc, valp);
  }

  return -UNW_EBADREG;
}

