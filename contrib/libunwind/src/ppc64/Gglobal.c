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

HIDDEN define_lock (ppc64_lock);
HIDDEN int tdep_init_done;

/* The API register numbers are exactly the same as the .eh_frame
   registers, for now at least.  */
HIDDEN const uint8_t dwarf_to_unw_regnum_map[DWARF_REGNUM_MAP_LENGTH] =
  {
        [UNW_PPC64_R0]=UNW_PPC64_R0,
        [UNW_PPC64_R1]=UNW_PPC64_R1,
        [UNW_PPC64_R2]=UNW_PPC64_R2,
        [UNW_PPC64_R3]=UNW_PPC64_R3,
        [UNW_PPC64_R4]=UNW_PPC64_R4,
        [UNW_PPC64_R5]=UNW_PPC64_R5,
        [UNW_PPC64_R6]=UNW_PPC64_R6,
        [UNW_PPC64_R7]=UNW_PPC64_R7,
        [UNW_PPC64_R8]=UNW_PPC64_R8,
        [UNW_PPC64_R9]=UNW_PPC64_R9,
        [UNW_PPC64_R10]=UNW_PPC64_R10,
        [UNW_PPC64_R11]=UNW_PPC64_R11,
        [UNW_PPC64_R12]=UNW_PPC64_R12,
        [UNW_PPC64_R13]=UNW_PPC64_R13,
        [UNW_PPC64_R14]=UNW_PPC64_R14,
        [UNW_PPC64_R15]=UNW_PPC64_R15,
        [UNW_PPC64_R16]=UNW_PPC64_R16,
        [UNW_PPC64_R17]=UNW_PPC64_R17,
        [UNW_PPC64_R18]=UNW_PPC64_R18,
        [UNW_PPC64_R19]=UNW_PPC64_R19,
        [UNW_PPC64_R20]=UNW_PPC64_R20,
        [UNW_PPC64_R21]=UNW_PPC64_R21,
        [UNW_PPC64_R22]=UNW_PPC64_R22,
        [UNW_PPC64_R23]=UNW_PPC64_R23,
        [UNW_PPC64_R24]=UNW_PPC64_R24,
        [UNW_PPC64_R25]=UNW_PPC64_R25,
        [UNW_PPC64_R26]=UNW_PPC64_R26,
        [UNW_PPC64_R27]=UNW_PPC64_R27,
        [UNW_PPC64_R28]=UNW_PPC64_R28,
        [UNW_PPC64_R29]=UNW_PPC64_R29,
        [UNW_PPC64_R30]=UNW_PPC64_R30,
        [UNW_PPC64_R31]=UNW_PPC64_R31,

        [UNW_PPC64_F0]=UNW_PPC64_F0,
        [UNW_PPC64_F1]=UNW_PPC64_F1,
        [UNW_PPC64_F2]=UNW_PPC64_F2,
        [UNW_PPC64_F3]=UNW_PPC64_F3,
        [UNW_PPC64_F4]=UNW_PPC64_F4,
        [UNW_PPC64_F5]=UNW_PPC64_F5,
        [UNW_PPC64_F6]=UNW_PPC64_F6,
        [UNW_PPC64_F7]=UNW_PPC64_F7,
        [UNW_PPC64_F8]=UNW_PPC64_F8,
        [UNW_PPC64_F9]=UNW_PPC64_F9,
        [UNW_PPC64_F10]=UNW_PPC64_F10,
        [UNW_PPC64_F11]=UNW_PPC64_F11,
        [UNW_PPC64_F12]=UNW_PPC64_F12,
        [UNW_PPC64_F13]=UNW_PPC64_F13,
        [UNW_PPC64_F14]=UNW_PPC64_F14,
        [UNW_PPC64_F15]=UNW_PPC64_F15,
        [UNW_PPC64_F16]=UNW_PPC64_F16,
        [UNW_PPC64_F17]=UNW_PPC64_F17,
        [UNW_PPC64_F18]=UNW_PPC64_F18,
        [UNW_PPC64_F19]=UNW_PPC64_F19,
        [UNW_PPC64_F20]=UNW_PPC64_F20,
        [UNW_PPC64_F21]=UNW_PPC64_F21,
        [UNW_PPC64_F22]=UNW_PPC64_F22,
        [UNW_PPC64_F23]=UNW_PPC64_F23,
        [UNW_PPC64_F24]=UNW_PPC64_F24,
        [UNW_PPC64_F25]=UNW_PPC64_F25,
        [UNW_PPC64_F26]=UNW_PPC64_F26,
        [UNW_PPC64_F27]=UNW_PPC64_F27,
        [UNW_PPC64_F28]=UNW_PPC64_F28,
        [UNW_PPC64_F29]=UNW_PPC64_F29,
        [UNW_PPC64_F30]=UNW_PPC64_F30,
        [UNW_PPC64_F31]=UNW_PPC64_F31,

        [UNW_PPC64_LR]=UNW_PPC64_LR,
        [UNW_PPC64_CTR]=UNW_PPC64_CTR,
        [UNW_PPC64_ARG_POINTER]=UNW_PPC64_ARG_POINTER,

        [UNW_PPC64_CR0]=UNW_PPC64_CR0,
        [UNW_PPC64_CR1]=UNW_PPC64_CR1,
        [UNW_PPC64_CR2]=UNW_PPC64_CR2,
        [UNW_PPC64_CR3]=UNW_PPC64_CR3,
        [UNW_PPC64_CR4]=UNW_PPC64_CR4,
        [UNW_PPC64_CR5]=UNW_PPC64_CR5,
        [UNW_PPC64_CR6]=UNW_PPC64_CR6,
        [UNW_PPC64_CR7]=UNW_PPC64_CR7,

        [UNW_PPC64_XER]=UNW_PPC64_XER,

        [UNW_PPC64_V0]=UNW_PPC64_V0,
        [UNW_PPC64_V1]=UNW_PPC64_V1,
        [UNW_PPC64_V2]=UNW_PPC64_V2,
        [UNW_PPC64_V3]=UNW_PPC64_V3,
        [UNW_PPC64_V4]=UNW_PPC64_V4,
        [UNW_PPC64_V5]=UNW_PPC64_V5,
        [UNW_PPC64_V6]=UNW_PPC64_V6,
        [UNW_PPC64_V7]=UNW_PPC64_V7,
        [UNW_PPC64_V8]=UNW_PPC64_V8,
        [UNW_PPC64_V9]=UNW_PPC64_V9,
        [UNW_PPC64_V10]=UNW_PPC64_V10,
        [UNW_PPC64_V11]=UNW_PPC64_V11,
        [UNW_PPC64_V12]=UNW_PPC64_V12,
        [UNW_PPC64_V13]=UNW_PPC64_V13,
        [UNW_PPC64_V14]=UNW_PPC64_V14,
        [UNW_PPC64_V15]=UNW_PPC64_V15,
        [UNW_PPC64_V16]=UNW_PPC64_V16,
        [UNW_PPC64_V17]=UNW_PPC64_V17,
        [UNW_PPC64_V18]=UNW_PPC64_V18,
        [UNW_PPC64_V19]=UNW_PPC64_V19,
        [UNW_PPC64_V20]=UNW_PPC64_V20,
        [UNW_PPC64_V21]=UNW_PPC64_V21,
        [UNW_PPC64_V22]=UNW_PPC64_V22,
        [UNW_PPC64_V23]=UNW_PPC64_V23,
        [UNW_PPC64_V24]=UNW_PPC64_V24,
        [UNW_PPC64_V25]=UNW_PPC64_V25,
        [UNW_PPC64_V26]=UNW_PPC64_V26,
        [UNW_PPC64_V27]=UNW_PPC64_V27,
        [UNW_PPC64_V28]=UNW_PPC64_V28,
        [UNW_PPC64_V29]=UNW_PPC64_V29,
        [UNW_PPC64_V30]=UNW_PPC64_V30,
        [UNW_PPC64_V31]=UNW_PPC64_V31,

        [UNW_PPC64_VRSAVE]=UNW_PPC64_VRSAVE,
        [UNW_PPC64_VSCR]=UNW_PPC64_VSCR,
        [UNW_PPC64_SPE_ACC]=UNW_PPC64_SPE_ACC,
        [UNW_PPC64_SPEFSCR]=UNW_PPC64_SPEFSCR,
  };

HIDDEN void
tdep_init (void)
{
  intrmask_t saved_mask;

  sigfillset (&unwi_full_mask);

  lock_acquire (&ppc64_lock, saved_mask);
  {
    if (tdep_init_done)
      /* another thread else beat us to it... */
      goto out;

    mi_init ();

    dwarf_init ();

#ifndef UNW_REMOTE_ONLY
    ppc64_local_addr_space_init ();
#endif
    tdep_init_done = 1; /* signal that we're initialized... */
  }
 out:
  lock_release (&ppc64_lock, saved_mask);
}
