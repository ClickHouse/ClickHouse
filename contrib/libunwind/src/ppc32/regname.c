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
    [UNW_PPC32_R0]="GPR0",
    [UNW_PPC32_R1]="GPR1",
    [UNW_PPC32_R2]="GPR2",
    [UNW_PPC32_R3]="GPR3",
    [UNW_PPC32_R4]="GPR4",
    [UNW_PPC32_R5]="GPR5",
    [UNW_PPC32_R6]="GPR6",
    [UNW_PPC32_R7]="GPR7",
    [UNW_PPC32_R8]="GPR8",
    [UNW_PPC32_R9]="GPR9",
    [UNW_PPC32_R10]="GPR10",
    [UNW_PPC32_R11]="GPR11",
    [UNW_PPC32_R12]="GPR12",
    [UNW_PPC32_R13]="GPR13",
    [UNW_PPC32_R14]="GPR14",
    [UNW_PPC32_R15]="GPR15",
    [UNW_PPC32_R16]="GPR16",
    [UNW_PPC32_R17]="GPR17",
    [UNW_PPC32_R18]="GPR18",
    [UNW_PPC32_R19]="GPR19",
    [UNW_PPC32_R20]="GPR20",
    [UNW_PPC32_R21]="GPR21",
    [UNW_PPC32_R22]="GPR22",
    [UNW_PPC32_R23]="GPR23",
    [UNW_PPC32_R24]="GPR24",
    [UNW_PPC32_R25]="GPR25",
    [UNW_PPC32_R26]="GPR26",
    [UNW_PPC32_R27]="GPR27",
    [UNW_PPC32_R28]="GPR28",
    [UNW_PPC32_R29]="GPR29",
    [UNW_PPC32_R30]="GPR30",
    [UNW_PPC32_R31]="GPR31",

    [UNW_PPC32_CTR]="CTR",
    [UNW_PPC32_XER]="XER",
    [UNW_PPC32_CCR]="CCR",
    [UNW_PPC32_LR]="LR",
    [UNW_PPC32_FPSCR]="FPSCR",

    [UNW_PPC32_F0]="FPR0",
    [UNW_PPC32_F1]="FPR1",
    [UNW_PPC32_F2]="FPR2",
    [UNW_PPC32_F3]="FPR3",
    [UNW_PPC32_F4]="FPR4",
    [UNW_PPC32_F5]="FPR5",
    [UNW_PPC32_F6]="FPR6",
    [UNW_PPC32_F7]="FPR7",
    [UNW_PPC32_F8]="FPR8",
    [UNW_PPC32_F9]="FPR9",
    [UNW_PPC32_F10]="FPR10",
    [UNW_PPC32_F11]="FPR11",
    [UNW_PPC32_F12]="FPR12",
    [UNW_PPC32_F13]="FPR13",
    [UNW_PPC32_F14]="FPR14",
    [UNW_PPC32_F15]="FPR15",
    [UNW_PPC32_F16]="FPR16",
    [UNW_PPC32_F17]="FPR17",
    [UNW_PPC32_F18]="FPR18",
    [UNW_PPC32_F19]="FPR19",
    [UNW_PPC32_F20]="FPR20",
    [UNW_PPC32_F21]="FPR21",
    [UNW_PPC32_F22]="FPR22",
    [UNW_PPC32_F23]="FPR23",
    [UNW_PPC32_F24]="FPR24",
    [UNW_PPC32_F25]="FPR25",
    [UNW_PPC32_F26]="FPR26",
    [UNW_PPC32_F27]="FPR27",
    [UNW_PPC32_F28]="FPR28",
    [UNW_PPC32_F29]="FPR29",
    [UNW_PPC32_F30]="FPR30",
    [UNW_PPC32_F31]="FPR31"
};

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname))
    return regname[reg];
  else
    return "???";
}
