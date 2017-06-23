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

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
#ifdef __linux__ 
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, w2, w3, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  /* Check if IP points at sigreturn() sequence.  On Linux, this normally is:

    rt_sigreturn:
       0x34190000 ldi 0, %r25
       0x3414015a ldi __NR_rt_sigreturn,%r20
       0xe4008200 be,l 0x100(%sr2,%r0),%sr0,%r31
       0x08000240 nop

     When a signal interrupts a system call, the first word is instead:

       0x34190002 ldi 1, %r25
  */
  ip = c->dwarf.ip;
  if (!ip)
    return 0;
  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 4, &w1, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 8, &w2, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 12, &w3, 0, arg)) < 0)
    {
      Debug (1, "failed to read sigreturn code (ret=%d)\n", ret);
      return ret;
    }
  ret = ((w0 == 0x34190000 || w0 == 0x34190002)
         && w1 == 0x3414015a && w2 == 0xe4008200 && w3 == 0x08000240);
  Debug (1, "(cursor=%p, ip=0x%08lx) -> %d\n", c, (unsigned) ip, ret);
  return ret;
#else
  printf ("%s: implement me\n", __FUNCTION__);
#endif
  return -UNW_ENOINFO;
}
