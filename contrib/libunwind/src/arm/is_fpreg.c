/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery

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

#include "libunwind_i.h"

/* FIXME: I'm not sure if libunwind's GP/FP register distinction is very useful
   on ARM.  Count all the FP or coprocessor registers we know about for now.  */

PROTECTED int
unw_is_fpreg (int regnum)
{
  return ((regnum >= UNW_ARM_S0 && regnum <= UNW_ARM_S31)
          || (regnum >= UNW_ARM_F0 && regnum <= UNW_ARM_F7)
          || (regnum >= UNW_ARM_wCGR0 && regnum <= UNW_ARM_wCGR7)
          || (regnum >= UNW_ARM_wR0 && regnum <= UNW_ARM_wR15)
          || (regnum >= UNW_ARM_wC0 && regnum <= UNW_ARM_wC7)
          || (regnum >= UNW_ARM_D0 && regnum <= UNW_ARM_D31));
}
