/* libunwind - a platform-independent unwind library
   Copyright (C) 2004 Hewlett-Packard Co
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

PROTECTED int
unw_get_save_loc (unw_cursor_t *cursor, int reg, unw_save_loc_t *sloc)
{
  struct cursor *c = (struct cursor *) cursor;
  dwarf_loc_t loc;

  loc = DWARF_NULL_LOC;         /* default to "not saved" */

  switch (reg)
    {
    case UNW_X86_64_RBX: loc = c->dwarf.loc[RBX]; break;
    case UNW_X86_64_RSP: loc = c->dwarf.loc[RSP]; break;
    case UNW_X86_64_RBP: loc = c->dwarf.loc[RBP]; break;
    case UNW_X86_64_R12: loc = c->dwarf.loc[R12]; break;
    case UNW_X86_64_R13: loc = c->dwarf.loc[R13]; break;
    case UNW_X86_64_R14: loc = c->dwarf.loc[R14]; break;
    case UNW_X86_64_R15: loc = c->dwarf.loc[R15]; break;

    default:
      break;
    }

  memset (sloc, 0, sizeof (*sloc));

  if (DWARF_IS_NULL_LOC (loc))
    {
      sloc->type = UNW_SLT_NONE;
      return 0;
    }

#if !defined(UNW_LOCAL_ONLY)
  if (DWARF_IS_REG_LOC (loc))
    {
      sloc->type = UNW_SLT_REG;
      sloc->u.regnum = DWARF_GET_LOC (loc);
    }
  else
#endif
    {
      sloc->type = UNW_SLT_MEMORY;
      sloc->u.addr = DWARF_GET_LOC (loc);
    }
  return 0;
}
