/* libunwind - a platform-independent unwind library
   Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
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

#include <stdlib.h>

#include "unwind_i.h"
#include "offsets.h"

/* This routine is responsible for copying the register values in
   cursor C and establishing them as the current machine state. */

static inline int
establish_machine_state (struct cursor *c)
{
  int (*access_reg) (unw_addr_space_t, unw_regnum_t, unw_word_t *,
                     int write, void *);
  int (*access_fpreg) (unw_addr_space_t, unw_regnum_t, unw_fpreg_t *,
                       int write, void *);
  unw_addr_space_t as = c->dwarf.as;
  void *arg = c->dwarf.as_arg;
  unw_fpreg_t fpval;
  unw_word_t val;
  int reg;

  access_reg = as->acc.access_reg;
  access_fpreg = as->acc.access_fpreg;

  Debug (8, "copying out cursor state\n");

  for (reg = 0; reg <= UNW_REG_LAST; ++reg)
    {
      Debug (16, "copying %s %d\n", unw_regname (reg), reg);
      if (unw_is_fpreg (reg))
        {
          if (tdep_access_fpreg (c, reg, &fpval, 0) >= 0)
            (*access_fpreg) (as, reg, &fpval, 1, arg);
        }
      else
        {
          if (tdep_access_reg (c, reg, &val, 0) >= 0)
            (*access_reg) (as, reg, &val, 1, arg);
        }
    }

  if (c->dwarf.args_size)
    {
      if (tdep_access_reg (c, UNW_X86_ESP, &val, 0) >= 0)
        {
          val += c->dwarf.args_size;
          (*access_reg) (as, UNW_X86_ESP, &val, 1, arg);
        }
    }
  return 0;
}

PROTECTED int
unw_resume (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  Debug (1, "(cursor=%p)\n", c);

  if ((ret = establish_machine_state (c)) < 0)
    return ret;

  return (*c->dwarf.as->acc.resume) (c->dwarf.as, (unw_cursor_t *) c,
                                     c->dwarf.as_arg);
}
