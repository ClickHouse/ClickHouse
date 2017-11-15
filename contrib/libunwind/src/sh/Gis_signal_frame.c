/* libunwind - a platform-independent unwind library
   Copyright (C) 2012 Tommi Rantala <tt.rantala@gmail.com>

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

/* Disassembly of the Linux VDSO sigreturn functions:

00000000 <__kernel_sigreturn>:
   0:   05 93           mov.w   e <__kernel_sigreturn+0xe>,r3   ! 77
   2:   10 c3           trapa   #16
   4:   0b 20           or      r0,r0
   6:   0b 20           or      r0,r0
   8:   0b 20           or      r0,r0
   a:   0b 20           or      r0,r0
   c:   0b 20           or      r0,r0
   e:   77 00           .word 0x0077
  10:   09 00           nop
  12:   09 00           nop
  14:   09 00           nop
  16:   09 00           nop
  18:   09 00           nop
  1a:   09 00           nop
  1c:   09 00           nop
  1e:   09 00           nop

00000020 <__kernel_rt_sigreturn>:
  20:   05 93           mov.w   2e <__kernel_rt_sigreturn+0xe>,r3       ! ad
  22:   10 c3           trapa   #16
  24:   0b 20           or      r0,r0
  26:   0b 20           or      r0,r0
  28:   0b 20           or      r0,r0
  2a:   0b 20           or      r0,r0
  2c:   0b 20           or      r0,r0
  2e:   ad 00           mov.w   @(r0,r10),r0
  30:   09 00           nop
  32:   09 00           nop
  34:   09 00           nop
  36:   09 00           nop
  38:   09 00           nop
  3a:   09 00           nop
  3c:   09 00           nop
  3e:   09 00           nop
*/

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
#ifdef __linux__
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  ip = c->dwarf.ip;

  ret = (*a->access_mem) (as, ip, &w0, 0, arg);
  if (ret < 0)
    return ret;

  if (w0 != 0xc3109305)
    return 0;

  ret = (*a->access_mem) (as, ip+4, &w0, 0, arg);
  if (ret < 0)
    return ret;

  if (w0 != 0x200b200b)
    return 0;

  ret = (*a->access_mem) (as, ip+8, &w0, 0, arg);
  if (ret < 0)
    return ret;

  if (w0 != 0x200b200b)
    return 0;

  ret = (*a->access_mem) (as, ip+12, &w0, 0, arg);
  if (ret < 0)
    return ret;

  if (w0 == 0x0077200b)
    return 1; /* non-RT */
  else if (w0 == 0x00ad200b)
    return 2; /* RT */

  /* does not look like a signal frame */
  return 0;

#else
  return -UNW_ENOINFO;
#endif
}
