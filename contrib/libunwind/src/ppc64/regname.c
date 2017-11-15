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

static const char *regname[] =
  {
    [UNW_PPC64_R0]="GPR0",
    [UNW_PPC64_R1]="GPR1",
    [UNW_PPC64_R2]="GPR2",
    [UNW_PPC64_R3]="GPR3",
    [UNW_PPC64_R4]="GPR4",
    [UNW_PPC64_R5]="GPR5",
    [UNW_PPC64_R6]="GPR6",
    [UNW_PPC64_R7]="GPR7",
    [UNW_PPC64_R8]="GPR8",
    [UNW_PPC64_R9]="GPR9",
    [UNW_PPC64_R10]="GPR10",
    [UNW_PPC64_R11]="GPR11",
    [UNW_PPC64_R12]="GPR12",
    [UNW_PPC64_R13]="GPR13",
    [UNW_PPC64_R14]="GPR14",
    [UNW_PPC64_R15]="GPR15",
    [UNW_PPC64_R16]="GPR16",
    [UNW_PPC64_R17]="GPR17",
    [UNW_PPC64_R18]="GPR18",
    [UNW_PPC64_R19]="GPR19",
    [UNW_PPC64_R20]="GPR20",
    [UNW_PPC64_R21]="GPR21",
    [UNW_PPC64_R22]="GPR22",
    [UNW_PPC64_R23]="GPR23",
    [UNW_PPC64_R24]="GPR24",
    [UNW_PPC64_R25]="GPR25",
    [UNW_PPC64_R26]="GPR26",
    [UNW_PPC64_R27]="GPR27",
    [UNW_PPC64_R28]="GPR28",
    [UNW_PPC64_R29]="GPR29",
    [UNW_PPC64_R30]="GPR30",
    [UNW_PPC64_R31]="GPR31",

    [UNW_PPC64_F0]="FPR0",
    [UNW_PPC64_F1]="FPR1",
    [UNW_PPC64_F2]="FPR2",
    [UNW_PPC64_F3]="FPR3",
    [UNW_PPC64_F4]="FPR4",
    [UNW_PPC64_F5]="FPR5",
    [UNW_PPC64_F6]="FPR6",
    [UNW_PPC64_F7]="FPR7",
    [UNW_PPC64_F8]="FPR8",
    [UNW_PPC64_F9]="FPR9",
    [UNW_PPC64_F10]="FPR10",
    [UNW_PPC64_F11]="FPR11",
    [UNW_PPC64_F12]="FPR12",
    [UNW_PPC64_F13]="FPR13",
    [UNW_PPC64_F14]="FPR14",
    [UNW_PPC64_F15]="FPR15",
    [UNW_PPC64_F16]="FPR16",
    [UNW_PPC64_F17]="FPR17",
    [UNW_PPC64_F18]="FPR18",
    [UNW_PPC64_F19]="FPR19",
    [UNW_PPC64_F20]="FPR20",
    [UNW_PPC64_F21]="FPR21",
    [UNW_PPC64_F22]="FPR22",
    [UNW_PPC64_F23]="FPR23",
    [UNW_PPC64_F24]="FPR24",
    [UNW_PPC64_F25]="FPR25",
    [UNW_PPC64_F26]="FPR26",
    [UNW_PPC64_F27]="FPR27",
    [UNW_PPC64_F28]="FPR28",
    [UNW_PPC64_F29]="FPR29",
    [UNW_PPC64_F30]="FPR30",
    [UNW_PPC64_F31]="FPR31",

    [UNW_PPC64_LR]="LR",
    [UNW_PPC64_CTR]="CTR",
    [UNW_PPC64_ARG_POINTER]="ARG_POINTER",

    [UNW_PPC64_CR0]="CR0",
    [UNW_PPC64_CR1]="CR1",
    [UNW_PPC64_CR2]="CR2",
    [UNW_PPC64_CR3]="CR3",
    [UNW_PPC64_CR4]="CR4",
    [UNW_PPC64_CR5]="CR5",
    [UNW_PPC64_CR6]="CR6",
    [UNW_PPC64_CR7]="CR7",

    [UNW_PPC64_XER]="XER",

    [UNW_PPC64_V0]="VR0",
    [UNW_PPC64_V1]="VR1",
    [UNW_PPC64_V2]="VR2",
    [UNW_PPC64_V3]="VR3",
    [UNW_PPC64_V4]="VR4",
    [UNW_PPC64_V5]="VR5",
    [UNW_PPC64_V6]="VR6",
    [UNW_PPC64_V7]="VR7",
    [UNW_PPC64_V8]="VR8",
    [UNW_PPC64_V9]="VR9",
    [UNW_PPC64_V10]="VR10",
    [UNW_PPC64_V11]="VR11",
    [UNW_PPC64_V12]="VR12",
    [UNW_PPC64_V13]="VR13",
    [UNW_PPC64_V14]="VR14",
    [UNW_PPC64_V15]="VR15",
    [UNW_PPC64_V16]="VR16",
    [UNW_PPC64_V17]="VR17",
    [UNW_PPC64_V18]="VR18",
    [UNW_PPC64_V19]="VR19",
    [UNW_PPC64_V20]="VR20",
    [UNW_PPC64_V21]="VR21",
    [UNW_PPC64_V22]="VR22",
    [UNW_PPC64_V23]="VR23",
    [UNW_PPC64_V24]="VR24",
    [UNW_PPC64_V25]="VR25",
    [UNW_PPC64_V26]="VR26",
    [UNW_PPC64_V27]="VR27",
    [UNW_PPC64_V28]="VR28",
    [UNW_PPC64_V29]="VR29",
    [UNW_PPC64_V30]="VR30",
    [UNW_PPC64_V31]="VR31",

    [UNW_PPC64_VSCR]="VSCR",

    [UNW_PPC64_VRSAVE]="VRSAVE",
    [UNW_PPC64_SPE_ACC]="SPE_ACC",
    [UNW_PPC64_SPEFSCR]="SPEFSCR",

    [UNW_PPC64_FRAME_POINTER]="FRAME_POINTER",
    [UNW_PPC64_NIP]="NIP",

   };

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname))
    return regname[reg];
  else
    return "???";
}
