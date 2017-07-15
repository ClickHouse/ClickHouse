/* libunwind - a platform-independent unwind library
   Copyright (c) 2002-2003 Hewlett-Packard Development Company, L.P.
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
unw_get_proc_info (unw_cursor_t *cursor, unw_proc_info_t *pi)
{
  struct cursor *c = (struct cursor *) cursor;

  if (dwarf_make_proc_info (&c->dwarf) < 0)
    {
      /* On x86-64, some key routines such as _start() and _dl_start()
         are missing DWARF unwind info.  We don't want to fail in that
         case, because those frames are uninteresting and just mark
         the end of the frame-chain anyhow.  */
      memset (pi, 0, sizeof (*pi));
      pi->start_ip = c->dwarf.ip;
      pi->end_ip = c->dwarf.ip + 1;
      return 0;
    }
  *pi = c->dwarf.pi;
  return 0;
}
