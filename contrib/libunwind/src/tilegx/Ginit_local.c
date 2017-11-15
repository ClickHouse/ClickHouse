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
#include "init.h"

#ifdef UNW_REMOTE_ONLY

PROTECTED int
unw_init_local (unw_cursor_t *cursor, ucontext_t *uc)
{
  return -UNW_EINVAL;
}

#else /* !UNW_REMOTE_ONLY */

static int
unw_init_local_common(unw_cursor_t *cursor, ucontext_t *uc, unsigned use_prev_instr)
{
  struct cursor *c = (struct cursor *) cursor;

  if (!tdep_init_done)
    tdep_init ();

  memset(c, 0, sizeof(unw_cursor_t));

  Debug (1, "(cursor=%p)\n", c);

  c->dwarf.as = unw_local_addr_space;

  c->dwarf.as_arg = uc;
  return common_init (c, use_prev_instr);
}

PROTECTED int
unw_init_local (unw_cursor_t *cursor, ucontext_t *uc)
{
  return unw_init_local_common(cursor, uc, 1);
}

PROTECTED int
unw_init_local2 (unw_cursor_t *cursor, ucontext_t *uc, int flag)
{
  if (!flag)
    {
      return unw_init_local_common(cursor, uc, 1);
    }
  else if (flag == UNW_INIT_SIGNAL_FRAME)
    {
      return unw_init_local_common(cursor, uc, 0);
    }
  else
    {
      return -UNW_EINVAL;
    }
}

#endif /* !UNW_REMOTE_ONLY */
