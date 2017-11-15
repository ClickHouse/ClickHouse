/* libunwind - a platform-independent unwind library
   Copyright (C) 2009 Red Hat
        Contributed by Jan Kratochvil <jan.kratochvil@redhat.com>

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

#include "unwind-internal.h"

/* gcc/unwind-dw2.c: Retrieve the return address and flag whether that IP is
   before or after first not yet fully executed instruction.  */

PROTECTED unsigned long
_Unwind_GetIPInfo (struct _Unwind_Context *context, int *ip_before_insn)
{
  unw_word_t val;

  unw_get_reg (&context->cursor, UNW_REG_IP, &val);
  *ip_before_insn = unw_is_signal_frame (&context->cursor);
  return val;
}

unsigned long __libunwind_Unwind_GetIPInfo (struct _Unwind_Context *, int *)
     ALIAS (_Unwind_GetIPInfo);
