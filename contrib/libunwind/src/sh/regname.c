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

static const char *const regname[] =
  {
    [UNW_SH_R0]  = "r0",
    [UNW_SH_R1]  = "r1",
    [UNW_SH_R2]  = "r2",
    [UNW_SH_R3]  = "r3",
    [UNW_SH_R4]  = "r4",
    [UNW_SH_R5]  = "r5",
    [UNW_SH_R6]  = "r6",
    [UNW_SH_R7]  = "r7",
    [UNW_SH_R8]  = "r8",
    [UNW_SH_R9]  = "r9",
    [UNW_SH_R10] = "r10",
    [UNW_SH_R11] = "r11",
    [UNW_SH_R12] = "r12",
    [UNW_SH_R13] = "r13",
    [UNW_SH_R14] = "r14",
    [UNW_SH_R15] = "r15",
    [UNW_SH_PC]  = "pc",
    [UNW_SH_PR]  = "pr",
  };

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname) && regname[reg] != NULL)
    return regname[reg];
  else
    return "???";
}
