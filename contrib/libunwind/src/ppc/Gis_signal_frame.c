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

#include <libunwind_i.h>

PROTECTED int
unw_is_signal_frame (unw_cursor_t * cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, i0, i1, i2, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  as->validate = 1;             /* Don't trust the ip */
  arg = c->dwarf.as_arg;

  /* Check if return address points at sigreturn sequence.
     on ppc64 Linux that is (see libc.so):
     0x38210080  addi r1, r1, 128  // pop the stack
     0x380000ac  li r0, 172        // invoke system service 172
     0x44000002  sc
   */

  ip = c->dwarf.ip;
  if (ip == 0)
    return 0;

  /* Read up two 8-byte words at the IP.  We are only looking at 3
     consecutive 32-bit words, so the second 8-byte word needs to be
     shifted right by 32 bits (think big-endian) */

  a = unw_get_accessors (as);
  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 8, &w1, 0, arg)) < 0)
    return 0;

  if (tdep_big_endian (as))
    {
      i0 = w0 >> 32;
      i1 = w0 & 0xffffffffUL;
      i2 = w1 >> 32;
    }
  else
    {
      i0 = w0 & 0xffffffffUL;
      i1 = w0 >> 32;
      i2 = w1 & 0xffffffffUL;
    }

  return (i0 == 0x38210080 && i1 == 0x380000ac && i2 == 0x44000002);
}
