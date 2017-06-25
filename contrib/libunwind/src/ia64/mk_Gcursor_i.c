/* libunwind - a platform-independent unwind library
   Copyright (C) 2003 Hewlett-Packard Co
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

/* Utility to generate cursor_i.h.  */

#include "libunwind_i.h"

#ifdef offsetof
# undef offsetof
#endif

#define offsetof(type,field)    ((char *) &((type *) 0)->field - (char *) 0)

#define OFFSET(sym, offset) \
        asm volatile("\n->" #sym " %0" : : "i" (offset))

int
main (void)
{
  OFFSET("IP_OFF",       offsetof (struct cursor, ip));
  OFFSET("PR_OFF",       offsetof (struct cursor, pr));
  OFFSET("BSP_OFF",      offsetof (struct cursor, bsp));
  OFFSET("PSP_OFF",      offsetof (struct cursor, psp));
  OFFSET("PFS_LOC_OFF",  offsetof (struct cursor, loc[IA64_REG_PFS]));
  OFFSET("RNAT_LOC_OFF", offsetof (struct cursor, loc[IA64_REG_RNAT]));
  OFFSET("UNAT_LOC_OFF", offsetof (struct cursor, loc[IA64_REG_UNAT]));
  OFFSET("LC_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_LC]));
  OFFSET("FPSR_LOC_OFF", offsetof (struct cursor, loc[IA64_REG_FPSR]));
  OFFSET("B1_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_B1]));
  OFFSET("B2_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_B2]));
  OFFSET("B3_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_B3]));
  OFFSET("B4_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_B4]));
  OFFSET("B5_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_B5]));
  OFFSET("F2_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_F2]));
  OFFSET("F3_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_F3]));
  OFFSET("F4_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_F4]));
  OFFSET("F5_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_F5]));
  OFFSET("FR_LOC_OFF",   offsetof (struct cursor, loc[IA64_REG_F16]));
  OFFSET("LOC_SIZE",
      (offsetof (struct cursor, loc[1]) - offsetof (struct cursor, loc[0])));
  OFFSET("SIGCONTEXT_ADDR_OFF", offsetof (struct cursor, sigcontext_addr));
  return 0;
}
