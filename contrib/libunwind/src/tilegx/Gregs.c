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

HIDDEN int
tdep_access_reg (struct cursor *c, unw_regnum_t reg, unw_word_t *valp,
                 int write)
{
  dwarf_loc_t loc = DWARF_NULL_LOC;
  
  if (reg == UNW_TILEGX_R54 && !write)
    {
      reg = UNW_TILEGX_CFA;
    }
  
  if (reg <= UNW_TILEGX_R55)
    loc = c->dwarf.loc[reg - UNW_TILEGX_R0];
  else if (reg == UNW_TILEGX_CFA)
    {
      if (write)
        return -UNW_EREADONLYREG;
      *valp = c->dwarf.cfa;
      return 0;
    }
  else
    {
      Debug (1, "bad register number %u\n", reg);
      return -UNW_EBADREG;
    }
  
  if (write)
    {
      if (ci->dwarf.use_prev_instr == 0) {
	if (reg == UNW_TILEGX_PC)
	  c->dwarf.ip = *valp;            /* update the IP cache */
       }
      else {
	if (reg == UNW_TILEGX_R55)
	  c->dwarf.ip = *valp;            /* update the IP cache */
      }
      return dwarf_put (&c->dwarf, loc, *valp);
    }
  else
    return dwarf_get (&c->dwarf, loc, valp);
}

HIDDEN int
tdep_access_fpreg (struct cursor *c, unw_regnum_t reg, unw_fpreg_t *valp,
                   int write)
{
  Debug (1, "bad register number %u\n", reg);
  return -UNW_EBADREG;
}
