/* libunwind - a platform-independent unwind library
   Copyright (C) 2001-2002 Hewlett-Packard Co
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

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  struct ia64_state_record sr;
  int ret;

  /* Crude and slow, but we need to peek ahead into the unwind
     descriptors to find out if the current IP is inside the signal
     trampoline.  */
  ret = ia64_fetch_proc_info (c, c->ip, 1);
  if (ret < 0)
    return ret;

  ret = ia64_create_state_record (c, &sr);
  if (ret < 0)
    return ret;

  /* For now, we assume that any non-zero abi marker implies a signal frame.
     This should get us pretty far.  */
  ret = (sr.abi_marker != 0);

  ia64_free_state_record (&sr);

  Debug (1, "(cursor=%p, ip=0x%016lx) -> %d\n", c, c->ip, ret);
  return ret;
}
