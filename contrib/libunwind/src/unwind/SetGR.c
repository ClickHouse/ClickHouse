/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
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

#include "unwind-internal.h"
#ifdef UNW_TARGET_X86
#include "dwarf_i.h"
#endif

PROTECTED void
_Unwind_SetGR (struct _Unwind_Context *context, int index,
               unsigned long new_value)
{
#ifdef UNW_TARGET_X86
  index = dwarf_to_unw_regnum(index);
#endif
  unw_set_reg (&context->cursor, index, new_value);
#ifdef UNW_TARGET_IA64
  if (index >= UNW_IA64_GR && index <= UNW_IA64_GR + 127)
    /* Clear the NaT bit. */
    unw_set_reg (&context->cursor, UNW_IA64_NAT + (index - UNW_IA64_GR), 0);
#endif
}

void __libunwind_Unwind_SetGR (struct _Unwind_Context *, int, unsigned long)
     ALIAS (_Unwind_SetGR);
