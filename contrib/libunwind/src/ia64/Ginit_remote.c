/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2002, 2004 Hewlett-Packard Co
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

#include "init.h"
#include "unwind_i.h"

PROTECTED int
unw_init_remote (unw_cursor_t *cursor, unw_addr_space_t as, void *as_arg)
{
#ifdef UNW_LOCAL_ONLY
  return -UNW_EINVAL;
#else /* !UNW_LOCAL_ONLY */
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t sp, bsp;
  int ret;

  if (!tdep_init_done)
    tdep_init ();

  Debug (1, "(cursor=%p)\n", c);

  if (as == unw_local_addr_space)
    /* This special-casing is unfortunate and shouldn't be needed;
       however, both Linux and HP-UX need to adjust the context a bit
       before it's usable.  Try to think of a cleaner way of doing
       this.  Not sure it's possible though, as long as we want to be
       able to use the context returned by getcontext() et al.  */
    return unw_init_local (cursor, as_arg);

  c->as = as;
  c->as_arg = as_arg;

  if ((ret = ia64_get (c, IA64_REG_LOC (c, UNW_IA64_GR + 12), &sp)) < 0
      || (ret = ia64_get (c, IA64_REG_LOC (c, UNW_IA64_AR_BSP), &bsp)) < 0)
    return ret;

  return common_init (c, sp, bsp);
#endif /* !UNW_LOCAL_ONLY */
}
