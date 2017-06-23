/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright (C) 2014 Tilera Corp.

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

static const char *regname[] =
  {
    /* 0.  */
    "r0",  "r1",  "r2",  "r3",  "r4",  "r5",  "r6",  "r7",
    /* 8.  */
    "r8",  "r9",  "r10", "r11",  "r12",  "r13",  "r14",  "r15",
    /* 16.  */
    "r16",  "r17",  "r18", "r19",  "r20",  "r21",  "r22",  "r23",
    /* 24.  */
    "r24",  "r25",  "r26", "r27",  "r28",  "r29",  "r30",  "r31",
    /* 32.  */
    "r32",  "r33",  "r34", "r35",  "r36",  "r37",  "r38",  "r39",
    /* 40.  */
    "r40",  "r41",  "r42", "r43",  "r44",  "r45",  "r46",  "r47",
    /* 48.  */
    "r48",  "r49",  "r50", "r51",  "r52",  "r53",  "r54",  "r55",
    /* pc, cfa */
    "pc",  "cfa"
  };

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname))
    return regname[reg];
  else
    return "???";
}
