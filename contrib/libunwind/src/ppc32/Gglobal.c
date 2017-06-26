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

#include "unwind_i.h"
#include "dwarf_i.h"

HIDDEN define_lock (ppc32_lock);
HIDDEN int tdep_init_done;

/* The API register numbers are exactly the same as the .eh_frame
   registers, for now at least.  */
HIDDEN const uint8_t dwarf_to_unw_regnum_map[DWARF_REGNUM_MAP_LENGTH] =
  {
        [UNW_PPC32_R0]=UNW_PPC32_R0,
        [UNW_PPC32_R1]=UNW_PPC32_R1,
        [UNW_PPC32_R2]=UNW_PPC32_R2,
        [UNW_PPC32_R3]=UNW_PPC32_R3,
        [UNW_PPC32_R4]=UNW_PPC32_R4,
        [UNW_PPC32_R5]=UNW_PPC32_R5,
        [UNW_PPC32_R6]=UNW_PPC32_R6,
        [UNW_PPC32_R7]=UNW_PPC32_R7,
        [UNW_PPC32_R8]=UNW_PPC32_R8,
        [UNW_PPC32_R9]=UNW_PPC32_R9,
        [UNW_PPC32_R10]=UNW_PPC32_R10,
        [UNW_PPC32_R11]=UNW_PPC32_R11,
        [UNW_PPC32_R12]=UNW_PPC32_R12,
        [UNW_PPC32_R13]=UNW_PPC32_R13,
        [UNW_PPC32_R14]=UNW_PPC32_R14,
        [UNW_PPC32_R15]=UNW_PPC32_R15,
        [UNW_PPC32_R16]=UNW_PPC32_R16,
        [UNW_PPC32_R17]=UNW_PPC32_R17,
        [UNW_PPC32_R18]=UNW_PPC32_R18,
        [UNW_PPC32_R19]=UNW_PPC32_R19,
        [UNW_PPC32_R20]=UNW_PPC32_R20,
        [UNW_PPC32_R21]=UNW_PPC32_R21,
        [UNW_PPC32_R22]=UNW_PPC32_R22,
        [UNW_PPC32_R23]=UNW_PPC32_R23,
        [UNW_PPC32_R24]=UNW_PPC32_R24,
        [UNW_PPC32_R25]=UNW_PPC32_R25,
        [UNW_PPC32_R26]=UNW_PPC32_R26,
        [UNW_PPC32_R27]=UNW_PPC32_R27,
        [UNW_PPC32_R28]=UNW_PPC32_R28,
        [UNW_PPC32_R29]=UNW_PPC32_R29,
        [UNW_PPC32_R30]=UNW_PPC32_R30,
        [UNW_PPC32_R31]=UNW_PPC32_R31,

        [UNW_PPC32_CTR]=UNW_PPC32_CTR,
        [UNW_PPC32_XER]=UNW_PPC32_XER,
        [UNW_PPC32_CCR]=UNW_PPC32_CCR,
        [UNW_PPC32_LR]=UNW_PPC32_LR,
        [UNW_PPC32_FPSCR]=UNW_PPC32_FPSCR,

        [UNW_PPC32_F0]=UNW_PPC32_F0,
        [UNW_PPC32_F1]=UNW_PPC32_F1,
        [UNW_PPC32_F2]=UNW_PPC32_F2,
        [UNW_PPC32_F3]=UNW_PPC32_F3,
        [UNW_PPC32_F4]=UNW_PPC32_F4,
        [UNW_PPC32_F5]=UNW_PPC32_F5,
        [UNW_PPC32_F6]=UNW_PPC32_F6,
        [UNW_PPC32_F7]=UNW_PPC32_F7,
        [UNW_PPC32_F8]=UNW_PPC32_F8,
        [UNW_PPC32_F9]=UNW_PPC32_F9,
        [UNW_PPC32_F10]=UNW_PPC32_F10,
        [UNW_PPC32_F11]=UNW_PPC32_F11,
        [UNW_PPC32_F12]=UNW_PPC32_F12,
        [UNW_PPC32_F13]=UNW_PPC32_F13,
        [UNW_PPC32_F14]=UNW_PPC32_F14,
        [UNW_PPC32_F15]=UNW_PPC32_F15,
        [UNW_PPC32_F16]=UNW_PPC32_F16,
        [UNW_PPC32_F17]=UNW_PPC32_F17,
        [UNW_PPC32_F18]=UNW_PPC32_F18,
        [UNW_PPC32_F19]=UNW_PPC32_F19,
        [UNW_PPC32_F20]=UNW_PPC32_F20,
        [UNW_PPC32_F21]=UNW_PPC32_F21,
        [UNW_PPC32_F22]=UNW_PPC32_F22,
        [UNW_PPC32_F23]=UNW_PPC32_F23,
        [UNW_PPC32_F24]=UNW_PPC32_F24,
        [UNW_PPC32_F25]=UNW_PPC32_F25,
        [UNW_PPC32_F26]=UNW_PPC32_F26,
        [UNW_PPC32_F27]=UNW_PPC32_F27,
        [UNW_PPC32_F28]=UNW_PPC32_F28,
        [UNW_PPC32_F29]=UNW_PPC32_F29,
        [UNW_PPC32_F30]=UNW_PPC32_F30,
        [UNW_PPC32_F31]=UNW_PPC32_F31,
};

HIDDEN void
tdep_init (void)
{
  intrmask_t saved_mask;

  sigfillset (&unwi_full_mask);

  lock_acquire (&ppc32_lock, saved_mask);
  {
    if (tdep_init_done)
      /* another thread else beat us to it... */
      goto out;

    mi_init ();

    dwarf_init ();

#ifndef UNW_REMOTE_ONLY
    ppc32_local_addr_space_init ();
#endif
    tdep_init_done = 1; /* signal that we're initialized... */
  }
 out:
  lock_release (&ppc32_lock, saved_mask);
}
