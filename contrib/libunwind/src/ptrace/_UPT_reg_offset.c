/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>
   Copyright (C) 2013 Linaro Limited

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

#include "_UPT_internal.h"

#include <stddef.h>

#ifdef HAVE_ASM_PTRACE_OFFSETS_H
# include <asm/ptrace_offsets.h>
#endif

const int _UPT_reg_offset[UNW_REG_LAST + 1] =
  {
#ifdef HAVE_ASM_PTRACE_OFFSETS_H
# ifndef PT_AR_CSD
#  define PT_AR_CSD     -1      /* this was introduced with rev 2.1 of ia64 */
# endif

    [UNW_IA64_GR +  0]  = -1,           [UNW_IA64_GR +  1]      = PT_R1,
    [UNW_IA64_GR +  2]  = PT_R2,        [UNW_IA64_GR +  3]      = PT_R3,
    [UNW_IA64_GR +  4]  = PT_R4,        [UNW_IA64_GR +  5]      = PT_R5,
    [UNW_IA64_GR +  6]  = PT_R6,        [UNW_IA64_GR +  7]      = PT_R7,
    [UNW_IA64_GR +  8]  = PT_R8,        [UNW_IA64_GR +  9]      = PT_R9,
    [UNW_IA64_GR + 10]  = PT_R10,       [UNW_IA64_GR + 11]      = PT_R11,
    [UNW_IA64_GR + 12]  = PT_R12,       [UNW_IA64_GR + 13]      = PT_R13,
    [UNW_IA64_GR + 14]  = PT_R14,       [UNW_IA64_GR + 15]      = PT_R15,
    [UNW_IA64_GR + 16]  = PT_R16,       [UNW_IA64_GR + 17]      = PT_R17,
    [UNW_IA64_GR + 18]  = PT_R18,       [UNW_IA64_GR + 19]      = PT_R19,
    [UNW_IA64_GR + 20]  = PT_R20,       [UNW_IA64_GR + 21]      = PT_R21,
    [UNW_IA64_GR + 22]  = PT_R22,       [UNW_IA64_GR + 23]      = PT_R23,
    [UNW_IA64_GR + 24]  = PT_R24,       [UNW_IA64_GR + 25]      = PT_R25,
    [UNW_IA64_GR + 26]  = PT_R26,       [UNW_IA64_GR + 27]      = PT_R27,
    [UNW_IA64_GR + 28]  = PT_R28,       [UNW_IA64_GR + 29]      = PT_R29,
    [UNW_IA64_GR + 30]  = PT_R30,       [UNW_IA64_GR + 31]      = PT_R31,

    [UNW_IA64_NAT+  0]  = -1,           [UNW_IA64_NAT+  1]      = PT_NAT_BITS,
    [UNW_IA64_NAT+  2]  = PT_NAT_BITS,  [UNW_IA64_NAT+  3]      = PT_NAT_BITS,
    [UNW_IA64_NAT+  4]  = PT_NAT_BITS,  [UNW_IA64_NAT+  5]      = PT_NAT_BITS,
    [UNW_IA64_NAT+  6]  = PT_NAT_BITS,  [UNW_IA64_NAT+  7]      = PT_NAT_BITS,
    [UNW_IA64_NAT+  8]  = PT_NAT_BITS,  [UNW_IA64_NAT+  9]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 10]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 11]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 12]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 13]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 14]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 15]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 16]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 17]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 18]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 19]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 20]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 21]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 22]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 23]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 24]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 25]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 26]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 27]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 28]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 29]      = PT_NAT_BITS,
    [UNW_IA64_NAT+ 30]  = PT_NAT_BITS,  [UNW_IA64_NAT+ 31]      = PT_NAT_BITS,

    [UNW_IA64_FR +  0]  = -1,           [UNW_IA64_FR +  1]      = -1,
    [UNW_IA64_FR +  2]  = PT_F2,        [UNW_IA64_FR +  3]      = PT_F3,
    [UNW_IA64_FR +  4]  = PT_F4,        [UNW_IA64_FR +  5]      = PT_F5,
    [UNW_IA64_FR +  6]  = PT_F6,        [UNW_IA64_FR +  7]      = PT_F7,
    [UNW_IA64_FR +  8]  = PT_F8,        [UNW_IA64_FR +  9]      = PT_F9,
    [UNW_IA64_FR + 10]  = PT_F10,       [UNW_IA64_FR + 11]      = PT_F11,
    [UNW_IA64_FR + 12]  = PT_F12,       [UNW_IA64_FR + 13]      = PT_F13,
    [UNW_IA64_FR + 14]  = PT_F14,       [UNW_IA64_FR + 15]      = PT_F15,
    [UNW_IA64_FR + 16]  = PT_F16,       [UNW_IA64_FR + 17]      = PT_F17,
    [UNW_IA64_FR + 18]  = PT_F18,       [UNW_IA64_FR + 19]      = PT_F19,
    [UNW_IA64_FR + 20]  = PT_F20,       [UNW_IA64_FR + 21]      = PT_F21,
    [UNW_IA64_FR + 22]  = PT_F22,       [UNW_IA64_FR + 23]      = PT_F23,
    [UNW_IA64_FR + 24]  = PT_F24,       [UNW_IA64_FR + 25]      = PT_F25,
    [UNW_IA64_FR + 26]  = PT_F26,       [UNW_IA64_FR + 27]      = PT_F27,
    [UNW_IA64_FR + 28]  = PT_F28,       [UNW_IA64_FR + 29]      = PT_F29,
    [UNW_IA64_FR + 30]  = PT_F30,       [UNW_IA64_FR + 31]      = PT_F31,
    [UNW_IA64_FR + 32]  = PT_F32,       [UNW_IA64_FR + 33]      = PT_F33,
    [UNW_IA64_FR + 34]  = PT_F34,       [UNW_IA64_FR + 35]      = PT_F35,
    [UNW_IA64_FR + 36]  = PT_F36,       [UNW_IA64_FR + 37]      = PT_F37,
    [UNW_IA64_FR + 38]  = PT_F38,       [UNW_IA64_FR + 39]      = PT_F39,
    [UNW_IA64_FR + 40]  = PT_F40,       [UNW_IA64_FR + 41]      = PT_F41,
    [UNW_IA64_FR + 42]  = PT_F42,       [UNW_IA64_FR + 43]      = PT_F43,
    [UNW_IA64_FR + 44]  = PT_F44,       [UNW_IA64_FR + 45]      = PT_F45,
    [UNW_IA64_FR + 46]  = PT_F46,       [UNW_IA64_FR + 47]      = PT_F47,
    [UNW_IA64_FR + 48]  = PT_F48,       [UNW_IA64_FR + 49]      = PT_F49,
    [UNW_IA64_FR + 50]  = PT_F50,       [UNW_IA64_FR + 51]      = PT_F51,
    [UNW_IA64_FR + 52]  = PT_F52,       [UNW_IA64_FR + 53]      = PT_F53,
    [UNW_IA64_FR + 54]  = PT_F54,       [UNW_IA64_FR + 55]      = PT_F55,
    [UNW_IA64_FR + 56]  = PT_F56,       [UNW_IA64_FR + 57]      = PT_F57,
    [UNW_IA64_FR + 58]  = PT_F58,       [UNW_IA64_FR + 59]      = PT_F59,
    [UNW_IA64_FR + 60]  = PT_F60,       [UNW_IA64_FR + 61]      = PT_F61,
    [UNW_IA64_FR + 62]  = PT_F62,       [UNW_IA64_FR + 63]      = PT_F63,
    [UNW_IA64_FR + 64]  = PT_F64,       [UNW_IA64_FR + 65]      = PT_F65,
    [UNW_IA64_FR + 66]  = PT_F66,       [UNW_IA64_FR + 67]      = PT_F67,
    [UNW_IA64_FR + 68]  = PT_F68,       [UNW_IA64_FR + 69]      = PT_F69,
    [UNW_IA64_FR + 70]  = PT_F70,       [UNW_IA64_FR + 71]      = PT_F71,
    [UNW_IA64_FR + 72]  = PT_F72,       [UNW_IA64_FR + 73]      = PT_F73,
    [UNW_IA64_FR + 74]  = PT_F74,       [UNW_IA64_FR + 75]      = PT_F75,
    [UNW_IA64_FR + 76]  = PT_F76,       [UNW_IA64_FR + 77]      = PT_F77,
    [UNW_IA64_FR + 78]  = PT_F78,       [UNW_IA64_FR + 79]      = PT_F79,
    [UNW_IA64_FR + 80]  = PT_F80,       [UNW_IA64_FR + 81]      = PT_F81,
    [UNW_IA64_FR + 82]  = PT_F82,       [UNW_IA64_FR + 83]      = PT_F83,
    [UNW_IA64_FR + 84]  = PT_F84,       [UNW_IA64_FR + 85]      = PT_F85,
    [UNW_IA64_FR + 86]  = PT_F86,       [UNW_IA64_FR + 87]      = PT_F87,
    [UNW_IA64_FR + 88]  = PT_F88,       [UNW_IA64_FR + 89]      = PT_F89,
    [UNW_IA64_FR + 90]  = PT_F90,       [UNW_IA64_FR + 91]      = PT_F91,
    [UNW_IA64_FR + 92]  = PT_F92,       [UNW_IA64_FR + 93]      = PT_F93,
    [UNW_IA64_FR + 94]  = PT_F94,       [UNW_IA64_FR + 95]      = PT_F95,
    [UNW_IA64_FR + 96]  = PT_F96,       [UNW_IA64_FR + 97]      = PT_F97,
    [UNW_IA64_FR + 98]  = PT_F98,       [UNW_IA64_FR + 99]      = PT_F99,
    [UNW_IA64_FR +100]  = PT_F100,      [UNW_IA64_FR +101]      = PT_F101,
    [UNW_IA64_FR +102]  = PT_F102,      [UNW_IA64_FR +103]      = PT_F103,
    [UNW_IA64_FR +104]  = PT_F104,      [UNW_IA64_FR +105]      = PT_F105,
    [UNW_IA64_FR +106]  = PT_F106,      [UNW_IA64_FR +107]      = PT_F107,
    [UNW_IA64_FR +108]  = PT_F108,      [UNW_IA64_FR +109]      = PT_F109,
    [UNW_IA64_FR +110]  = PT_F110,      [UNW_IA64_FR +111]      = PT_F111,
    [UNW_IA64_FR +112]  = PT_F112,      [UNW_IA64_FR +113]      = PT_F113,
    [UNW_IA64_FR +114]  = PT_F114,      [UNW_IA64_FR +115]      = PT_F115,
    [UNW_IA64_FR +116]  = PT_F116,      [UNW_IA64_FR +117]      = PT_F117,
    [UNW_IA64_FR +118]  = PT_F118,      [UNW_IA64_FR +119]      = PT_F119,
    [UNW_IA64_FR +120]  = PT_F120,      [UNW_IA64_FR +121]      = PT_F121,
    [UNW_IA64_FR +122]  = PT_F122,      [UNW_IA64_FR +123]      = PT_F123,
    [UNW_IA64_FR +124]  = PT_F124,      [UNW_IA64_FR +125]      = PT_F125,
    [UNW_IA64_FR +126]  = PT_F126,      [UNW_IA64_FR +127]      = PT_F127,

    [UNW_IA64_AR +  0]  = -1,           [UNW_IA64_AR +  1]      = -1,
    [UNW_IA64_AR +  2]  = -1,           [UNW_IA64_AR +  3]      = -1,
    [UNW_IA64_AR +  4]  = -1,           [UNW_IA64_AR +  5]      = -1,
    [UNW_IA64_AR +  6]  = -1,           [UNW_IA64_AR +  7]      = -1,
    [UNW_IA64_AR +  8]  = -1,           [UNW_IA64_AR +  9]      = -1,
    [UNW_IA64_AR + 10]  = -1,           [UNW_IA64_AR + 11]      = -1,
    [UNW_IA64_AR + 12]  = -1,           [UNW_IA64_AR + 13]      = -1,
    [UNW_IA64_AR + 14]  = -1,           [UNW_IA64_AR + 15]      = -1,
    [UNW_IA64_AR + 16]  = PT_AR_RSC,    [UNW_IA64_AR + 17]      = PT_AR_BSP,
    [UNW_IA64_AR + 18]  = PT_AR_BSPSTORE,[UNW_IA64_AR + 19]     = PT_AR_RNAT,
    [UNW_IA64_AR + 20]  = -1,           [UNW_IA64_AR + 21]      = -1,
    [UNW_IA64_AR + 22]  = -1,           [UNW_IA64_AR + 23]      = -1,
    [UNW_IA64_AR + 24]  = -1,           [UNW_IA64_AR + 25]      = PT_AR_CSD,
    [UNW_IA64_AR + 26]  = -1,           [UNW_IA64_AR + 27]      = -1,
    [UNW_IA64_AR + 28]  = -1,           [UNW_IA64_AR + 29]      = -1,
    [UNW_IA64_AR + 30]  = -1,           [UNW_IA64_AR + 31]      = -1,
    [UNW_IA64_AR + 32]  = PT_AR_CCV,    [UNW_IA64_AR + 33]      = -1,
    [UNW_IA64_AR + 34]  = -1,           [UNW_IA64_AR + 35]      = -1,
    [UNW_IA64_AR + 36]  = PT_AR_UNAT,   [UNW_IA64_AR + 37]      = -1,
    [UNW_IA64_AR + 38]  = -1,           [UNW_IA64_AR + 39]      = -1,
    [UNW_IA64_AR + 40]  = PT_AR_FPSR,   [UNW_IA64_AR + 41]      = -1,
    [UNW_IA64_AR + 42]  = -1,           [UNW_IA64_AR + 43]      = -1,
    [UNW_IA64_AR + 44]  = -1,           [UNW_IA64_AR + 45]      = -1,
    [UNW_IA64_AR + 46]  = -1,           [UNW_IA64_AR + 47]      = -1,
    [UNW_IA64_AR + 48]  = -1,           [UNW_IA64_AR + 49]      = -1,
    [UNW_IA64_AR + 50]  = -1,           [UNW_IA64_AR + 51]      = -1,
    [UNW_IA64_AR + 52]  = -1,           [UNW_IA64_AR + 53]      = -1,
    [UNW_IA64_AR + 54]  = -1,           [UNW_IA64_AR + 55]      = -1,
    [UNW_IA64_AR + 56]  = -1,           [UNW_IA64_AR + 57]      = -1,
    [UNW_IA64_AR + 58]  = -1,           [UNW_IA64_AR + 59]      = -1,
    [UNW_IA64_AR + 60]  = -1,           [UNW_IA64_AR + 61]      = -1,
    [UNW_IA64_AR + 62]  = -1,           [UNW_IA64_AR + 63]      = -1,
    [UNW_IA64_AR + 64]  = PT_AR_PFS,    [UNW_IA64_AR + 65]      = PT_AR_LC,
    [UNW_IA64_AR + 66]  = PT_AR_EC,     [UNW_IA64_AR + 67]      = -1,
    [UNW_IA64_AR + 68]  = -1,           [UNW_IA64_AR + 69]      = -1,
    [UNW_IA64_AR + 70]  = -1,           [UNW_IA64_AR + 71]      = -1,
    [UNW_IA64_AR + 72]  = -1,           [UNW_IA64_AR + 73]      = -1,
    [UNW_IA64_AR + 74]  = -1,           [UNW_IA64_AR + 75]      = -1,
    [UNW_IA64_AR + 76]  = -1,           [UNW_IA64_AR + 77]      = -1,
    [UNW_IA64_AR + 78]  = -1,           [UNW_IA64_AR + 79]      = -1,
    [UNW_IA64_AR + 80]  = -1,           [UNW_IA64_AR + 81]      = -1,
    [UNW_IA64_AR + 82]  = -1,           [UNW_IA64_AR + 83]      = -1,
    [UNW_IA64_AR + 84]  = -1,           [UNW_IA64_AR + 85]      = -1,
    [UNW_IA64_AR + 86]  = -1,           [UNW_IA64_AR + 87]      = -1,
    [UNW_IA64_AR + 88]  = -1,           [UNW_IA64_AR + 89]      = -1,
    [UNW_IA64_AR + 90]  = -1,           [UNW_IA64_AR + 91]      = -1,
    [UNW_IA64_AR + 92]  = -1,           [UNW_IA64_AR + 93]      = -1,
    [UNW_IA64_AR + 94]  = -1,           [UNW_IA64_AR + 95]      = -1,
    [UNW_IA64_AR + 96]  = -1,           [UNW_IA64_AR + 97]      = -1,
    [UNW_IA64_AR + 98]  = -1,           [UNW_IA64_AR + 99]      = -1,
    [UNW_IA64_AR +100]  = -1,           [UNW_IA64_AR +101]      = -1,
    [UNW_IA64_AR +102]  = -1,           [UNW_IA64_AR +103]      = -1,
    [UNW_IA64_AR +104]  = -1,           [UNW_IA64_AR +105]      = -1,
    [UNW_IA64_AR +106]  = -1,           [UNW_IA64_AR +107]      = -1,
    [UNW_IA64_AR +108]  = -1,           [UNW_IA64_AR +109]      = -1,
    [UNW_IA64_AR +110]  = -1,           [UNW_IA64_AR +111]      = -1,
    [UNW_IA64_AR +112]  = -1,           [UNW_IA64_AR +113]      = -1,
    [UNW_IA64_AR +114]  = -1,           [UNW_IA64_AR +115]      = -1,
    [UNW_IA64_AR +116]  = -1,           [UNW_IA64_AR +117]      = -1,
    [UNW_IA64_AR +118]  = -1,           [UNW_IA64_AR +119]      = -1,
    [UNW_IA64_AR +120]  = -1,           [UNW_IA64_AR +121]      = -1,
    [UNW_IA64_AR +122]  = -1,           [UNW_IA64_AR +123]      = -1,
    [UNW_IA64_AR +124]  = -1,           [UNW_IA64_AR +125]      = -1,
    [UNW_IA64_AR +126]  = -1,           [UNW_IA64_AR +127]      = -1,

    [UNW_IA64_BR +  0]  = PT_B0,        [UNW_IA64_BR +  1]      = PT_B1,
    [UNW_IA64_BR +  2]  = PT_B2,        [UNW_IA64_BR +  3]      = PT_B3,
    [UNW_IA64_BR +  4]  = PT_B4,        [UNW_IA64_BR +  5]      = PT_B5,
    [UNW_IA64_BR +  6]  = PT_B6,        [UNW_IA64_BR +  7]      = PT_B7,

    [UNW_IA64_PR]       = PT_PR,
    [UNW_IA64_CFM]      = PT_CFM,
    [UNW_IA64_IP]       = PT_CR_IIP
#elif defined(HAVE_TTRACE)
# warning No support for ttrace() yet.
#elif defined(UNW_TARGET_HPPA)
    [UNW_HPPA_GR +  0]  = 0x000,        [UNW_HPPA_GR +  1]      = 0x004,
    [UNW_HPPA_GR +  2]  = 0x008,        [UNW_HPPA_GR +  3]      = 0x00c,
    [UNW_HPPA_GR +  4]  = 0x010,        [UNW_HPPA_GR +  5]      = 0x014,
    [UNW_HPPA_GR +  6]  = 0x018,        [UNW_HPPA_GR +  7]      = 0x01c,
    [UNW_HPPA_GR +  8]  = 0x020,        [UNW_HPPA_GR +  9]      = 0x024,
    [UNW_HPPA_GR + 10]  = 0x028,        [UNW_HPPA_GR + 11]      = 0x02c,
    [UNW_HPPA_GR + 12]  = 0x030,        [UNW_HPPA_GR + 13]      = 0x034,
    [UNW_HPPA_GR + 14]  = 0x038,        [UNW_HPPA_GR + 15]      = 0x03c,
    [UNW_HPPA_GR + 16]  = 0x040,        [UNW_HPPA_GR + 17]      = 0x044,
    [UNW_HPPA_GR + 18]  = 0x048,        [UNW_HPPA_GR + 19]      = 0x04c,
    [UNW_HPPA_GR + 20]  = 0x050,        [UNW_HPPA_GR + 21]      = 0x054,
    [UNW_HPPA_GR + 22]  = 0x058,        [UNW_HPPA_GR + 23]      = 0x05c,
    [UNW_HPPA_GR + 24]  = 0x060,        [UNW_HPPA_GR + 25]      = 0x064,
    [UNW_HPPA_GR + 26]  = 0x068,        [UNW_HPPA_GR + 27]      = 0x06c,
    [UNW_HPPA_GR + 28]  = 0x070,        [UNW_HPPA_GR + 29]      = 0x074,
    [UNW_HPPA_GR + 30]  = 0x078,        [UNW_HPPA_GR + 31]      = 0x07c,

    [UNW_HPPA_FR +  0]  = 0x080,        [UNW_HPPA_FR +  1]      = 0x088,
    [UNW_HPPA_FR +  2]  = 0x090,        [UNW_HPPA_FR +  3]      = 0x098,
    [UNW_HPPA_FR +  4]  = 0x0a0,        [UNW_HPPA_FR +  5]      = 0x0a8,
    [UNW_HPPA_FR +  6]  = 0x0b0,        [UNW_HPPA_FR +  7]      = 0x0b8,
    [UNW_HPPA_FR +  8]  = 0x0c0,        [UNW_HPPA_FR +  9]      = 0x0c8,
    [UNW_HPPA_FR + 10]  = 0x0d0,        [UNW_HPPA_FR + 11]      = 0x0d8,
    [UNW_HPPA_FR + 12]  = 0x0e0,        [UNW_HPPA_FR + 13]      = 0x0e8,
    [UNW_HPPA_FR + 14]  = 0x0f0,        [UNW_HPPA_FR + 15]      = 0x0f8,
    [UNW_HPPA_FR + 16]  = 0x100,        [UNW_HPPA_FR + 17]      = 0x108,
    [UNW_HPPA_FR + 18]  = 0x110,        [UNW_HPPA_FR + 19]      = 0x118,
    [UNW_HPPA_FR + 20]  = 0x120,        [UNW_HPPA_FR + 21]      = 0x128,
    [UNW_HPPA_FR + 22]  = 0x130,        [UNW_HPPA_FR + 23]      = 0x138,
    [UNW_HPPA_FR + 24]  = 0x140,        [UNW_HPPA_FR + 25]      = 0x148,
    [UNW_HPPA_FR + 26]  = 0x150,        [UNW_HPPA_FR + 27]      = 0x158,
    [UNW_HPPA_FR + 28]  = 0x160,        [UNW_HPPA_FR + 29]      = 0x168,
    [UNW_HPPA_FR + 30]  = 0x170,        [UNW_HPPA_FR + 31]      = 0x178,

    [UNW_HPPA_IP]       = 0x1a8         /* IAOQ[0] */
#elif defined(UNW_TARGET_X86)
#if defined __FreeBSD__
#define UNW_R_OFF(R, r) \
    [UNW_X86_##R]       = offsetof(gregset_t, r_##r),
    UNW_R_OFF(EAX, eax)
    UNW_R_OFF(EDX, edx)
    UNW_R_OFF(ECX, ecx)
    UNW_R_OFF(EBX, ebx)
    UNW_R_OFF(ESI, esi)
    UNW_R_OFF(EDI, edi)
    UNW_R_OFF(EBP, ebp)
    UNW_R_OFF(ESP, esp)
    UNW_R_OFF(EIP, eip)
//  UNW_R_OFF(CS, cs)
//  UNW_R_OFF(EFLAGS, eflags)
//  UNW_R_OFF(SS, ss)
#elif defined __linux__
    [UNW_X86_EAX]       = 0x18,
    [UNW_X86_EBX]       = 0x00,
    [UNW_X86_ECX]       = 0x04,
    [UNW_X86_EDX]       = 0x08,
    [UNW_X86_ESI]       = 0x0c,
    [UNW_X86_EDI]       = 0x10,
    [UNW_X86_EBP]       = 0x14,
    [UNW_X86_EIP]       = 0x30,
    [UNW_X86_ESP]       = 0x3c
/*  CS                  = 0x34, */
/*  DS                  = 0x1c, */
/*  ES                  = 0x20, */
/*  FS                  = 0x24, */
/*  GS                  = 0x28, */
/*  ORIG_EAX            = 0x2c, */
/*  EFLAGS              = 0x38, */
/*  SS                  = 0x40 */
#else
#error Port me
#endif
#elif defined(UNW_TARGET_X86_64)
#if defined __FreeBSD__
#define UNW_R_OFF(R, r) \
    [UNW_X86_64_##R]    = offsetof(gregset_t, r_##r),
    UNW_R_OFF(RAX, rax)
    UNW_R_OFF(RDX, rdx)
    UNW_R_OFF(RCX, rcx)
    UNW_R_OFF(RBX, rbx)
    UNW_R_OFF(RSI, rsi)
    UNW_R_OFF(RDI, rdi)
    UNW_R_OFF(RBP, rbp)
    UNW_R_OFF(RSP, rsp)
    UNW_R_OFF(R8, r8)
    UNW_R_OFF(R9, r9)
    UNW_R_OFF(R10, r10)
    UNW_R_OFF(R11, r11)
    UNW_R_OFF(R12, r12)
    UNW_R_OFF(R13, r13)
    UNW_R_OFF(R14, r14)
    UNW_R_OFF(R15, r15)
    UNW_R_OFF(RIP, rip)
//  UNW_R_OFF(CS, cs)
//  UNW_R_OFF(EFLAGS, rflags)
//  UNW_R_OFF(SS, ss)
#undef UNW_R_OFF
#elif defined __linux__
    [UNW_X86_64_RAX]    = 0x50,
    [UNW_X86_64_RDX]    = 0x60,
    [UNW_X86_64_RCX]    = 0x58,
    [UNW_X86_64_RBX]    = 0x28,
    [UNW_X86_64_RSI]    = 0x68,
    [UNW_X86_64_RDI]    = 0x70,
    [UNW_X86_64_RBP]    = 0x20,
    [UNW_X86_64_RSP]    = 0x98,
    [UNW_X86_64_R8]     = 0x48,
    [UNW_X86_64_R9]     = 0x40,
    [UNW_X86_64_R10]    = 0x38,
    [UNW_X86_64_R11]    = 0x30,
    [UNW_X86_64_R12]    = 0x18,
    [UNW_X86_64_R13]    = 0x10,
    [UNW_X86_64_R14]    = 0x08,
    [UNW_X86_64_R15]    = 0x00,
    [UNW_X86_64_RIP]    = 0x80
//  [UNW_X86_64_CS]     = 0x88,
//  [UNW_X86_64_EFLAGS] = 0x90,
//  [UNW_X86_64_RSP]    = 0x98,
//  [UNW_X86_64_SS]     = 0xa0
#else
#error Port me
#endif
#elif defined(UNW_TARGET_PPC32) || defined(UNW_TARGET_PPC64)

#define UNW_REG_SLOT_SIZE sizeof(unsigned long)
#define UNW_PPC_R(v) ((v) * UNW_REG_SLOT_SIZE)
#define UNW_PPC_PT(p) UNW_PPC_R(PT_##p)

#define UNW_FP_OFF(b, i)    \
    [UNW_PPC##b##_F##i] = UNW_PPC_R(PT_FPR0 + i * 8/UNW_REG_SLOT_SIZE)

#define UNW_R_OFF(b, i) \
    [UNW_PPC##b##_R##i] = UNW_PPC_R(PT_R##i)

#define UNW_PPC_REGS(b) \
    UNW_R_OFF(b, 0),    \
    UNW_R_OFF(b, 1),    \
    UNW_R_OFF(b, 2),    \
    UNW_R_OFF(b, 3),    \
    UNW_R_OFF(b, 4),    \
    UNW_R_OFF(b, 5),    \
    UNW_R_OFF(b, 6),    \
    UNW_R_OFF(b, 7),    \
    UNW_R_OFF(b, 8),    \
    UNW_R_OFF(b, 9),    \
    UNW_R_OFF(b, 10),   \
    UNW_R_OFF(b, 11),   \
    UNW_R_OFF(b, 12),   \
    UNW_R_OFF(b, 13),   \
    UNW_R_OFF(b, 14),   \
    UNW_R_OFF(b, 15),   \
    UNW_R_OFF(b, 16),   \
    UNW_R_OFF(b, 17),   \
    UNW_R_OFF(b, 18),   \
    UNW_R_OFF(b, 19),   \
    UNW_R_OFF(b, 20),   \
    UNW_R_OFF(b, 21),   \
    UNW_R_OFF(b, 22),   \
    UNW_R_OFF(b, 23),   \
    UNW_R_OFF(b, 24),   \
    UNW_R_OFF(b, 25),   \
    UNW_R_OFF(b, 26),   \
    UNW_R_OFF(b, 27),   \
    UNW_R_OFF(b, 28),   \
    UNW_R_OFF(b, 29),   \
    UNW_R_OFF(b, 30),   \
    UNW_R_OFF(b, 31),   \
                               \
    [UNW_PPC##b##_CTR] = UNW_PPC_PT(CTR), \
    [UNW_PPC##b##_XER] = UNW_PPC_PT(XER), \
    [UNW_PPC##b##_LR]  = UNW_PPC_PT(LNK), \
                               \
    UNW_FP_OFF(b, 0), \
    UNW_FP_OFF(b, 1), \
    UNW_FP_OFF(b, 2), \
    UNW_FP_OFF(b, 3), \
    UNW_FP_OFF(b, 4), \
    UNW_FP_OFF(b, 5), \
    UNW_FP_OFF(b, 6), \
    UNW_FP_OFF(b, 7), \
    UNW_FP_OFF(b, 8), \
    UNW_FP_OFF(b, 9), \
    UNW_FP_OFF(b, 10), \
    UNW_FP_OFF(b, 11), \
    UNW_FP_OFF(b, 12), \
    UNW_FP_OFF(b, 13), \
    UNW_FP_OFF(b, 14), \
    UNW_FP_OFF(b, 15), \
    UNW_FP_OFF(b, 16), \
    UNW_FP_OFF(b, 17), \
    UNW_FP_OFF(b, 18), \
    UNW_FP_OFF(b, 19), \
    UNW_FP_OFF(b, 20), \
    UNW_FP_OFF(b, 21), \
    UNW_FP_OFF(b, 22), \
    UNW_FP_OFF(b, 23), \
    UNW_FP_OFF(b, 24), \
    UNW_FP_OFF(b, 25), \
    UNW_FP_OFF(b, 26), \
    UNW_FP_OFF(b, 27), \
    UNW_FP_OFF(b, 28), \
    UNW_FP_OFF(b, 29), \
    UNW_FP_OFF(b, 30), \
    UNW_FP_OFF(b, 31)

#define UNW_PPC32_REGS \
    [UNW_PPC32_FPSCR] = UNW_PPC_PT(FPSCR), \
    [UNW_PPC32_CCR] = UNW_PPC_PT(CCR)

#define UNW_VR_OFF(i)   \
    [UNW_PPC64_V##i] = UNW_PPC_R(PT_VR0 + i * 2)

#define UNW_PPC64_REGS \
    [UNW_PPC64_NIP] = UNW_PPC_PT(NIP), \
    [UNW_PPC64_FRAME_POINTER] = -1, \
    [UNW_PPC64_ARG_POINTER] = -1,   \
    [UNW_PPC64_CR0] = -1,           \
    [UNW_PPC64_CR1] = -1,           \
    [UNW_PPC64_CR2] = -1,           \
    [UNW_PPC64_CR3] = -1,           \
    [UNW_PPC64_CR4] = -1,           \
    [UNW_PPC64_CR5] = -1,           \
    [UNW_PPC64_CR6] = -1,           \
    [UNW_PPC64_CR7] = -1,           \
    [UNW_PPC64_VRSAVE] = UNW_PPC_PT(VRSAVE), \
    [UNW_PPC64_VSCR] = UNW_PPC_PT(VSCR),     \
    [UNW_PPC64_SPE_ACC] = -1,       \
    [UNW_PPC64_SPEFSCR] = -1,       \
    UNW_VR_OFF(0), \
    UNW_VR_OFF(1), \
    UNW_VR_OFF(2), \
    UNW_VR_OFF(3), \
    UNW_VR_OFF(4), \
    UNW_VR_OFF(5), \
    UNW_VR_OFF(6), \
    UNW_VR_OFF(7), \
    UNW_VR_OFF(8), \
    UNW_VR_OFF(9), \
    UNW_VR_OFF(10), \
    UNW_VR_OFF(11), \
    UNW_VR_OFF(12), \
    UNW_VR_OFF(13), \
    UNW_VR_OFF(14), \
    UNW_VR_OFF(15), \
    UNW_VR_OFF(16), \
    UNW_VR_OFF(17), \
    UNW_VR_OFF(18), \
    UNW_VR_OFF(19), \
    UNW_VR_OFF(20), \
    UNW_VR_OFF(21), \
    UNW_VR_OFF(22), \
    UNW_VR_OFF(23), \
    UNW_VR_OFF(24), \
    UNW_VR_OFF(25), \
    UNW_VR_OFF(26), \
    UNW_VR_OFF(27), \
    UNW_VR_OFF(28), \
    UNW_VR_OFF(29), \
    UNW_VR_OFF(30), \
    UNW_VR_OFF(31)

#if defined(UNW_TARGET_PPC32)
    UNW_PPC_REGS(32),
    UNW_PPC32_REGS,
#else
    UNW_PPC_REGS(64),
    UNW_PPC64_REGS,
#endif

#elif defined(UNW_TARGET_ARM)
#if defined(__linux__) || defined(__FreeBSD__)
    [UNW_ARM_R0]       = 0x00,
    [UNW_ARM_R1]       = 0x04,
    [UNW_ARM_R2]       = 0x08,
    [UNW_ARM_R3]       = 0x0c,
    [UNW_ARM_R4]       = 0x10,
    [UNW_ARM_R5]       = 0x14,
    [UNW_ARM_R6]       = 0x18,
    [UNW_ARM_R7]       = 0x1c,
    [UNW_ARM_R8]       = 0x20,
    [UNW_ARM_R9]       = 0x24,
    [UNW_ARM_R10]      = 0x28,
    [UNW_ARM_R11]      = 0x2c,
    [UNW_ARM_R12]      = 0x30,
    [UNW_ARM_R13]      = 0x34,
    [UNW_ARM_R14]      = 0x38,
    [UNW_ARM_R15]      = 0x3c,
#else
#error Fix me
#endif
#elif defined(UNW_TARGET_MIPS)
    [UNW_MIPS_R0]  =  0,
    [UNW_MIPS_R1]  =  1,
    [UNW_MIPS_R2]  =  2,
    [UNW_MIPS_R3]  =  3,
    [UNW_MIPS_R4]  =  4,
    [UNW_MIPS_R5]  =  5,
    [UNW_MIPS_R6]  =  6,
    [UNW_MIPS_R7]  =  7,
    [UNW_MIPS_R8]  =  8,
    [UNW_MIPS_R9]  =  9,
    [UNW_MIPS_R10] = 10,
    [UNW_MIPS_R11] = 11,
    [UNW_MIPS_R12] = 12,
    [UNW_MIPS_R13] = 13,
    [UNW_MIPS_R14] = 14,
    [UNW_MIPS_R15] = 15,
    [UNW_MIPS_R16] = 16,
    [UNW_MIPS_R17] = 17,
    [UNW_MIPS_R18] = 18,
    [UNW_MIPS_R19] = 19,
    [UNW_MIPS_R20] = 20,
    [UNW_MIPS_R21] = 21,
    [UNW_MIPS_R22] = 22,
    [UNW_MIPS_R23] = 23,
    [UNW_MIPS_R24] = 24,
    [UNW_MIPS_R25] = 25,
    [UNW_MIPS_R26] = 26,
    [UNW_MIPS_R27] = 27,
    [UNW_MIPS_R28] = 28,
    [UNW_MIPS_R29] = 29,
    [UNW_MIPS_R30] = 30,
    [UNW_MIPS_R31] = 31,
    [UNW_MIPS_PC]  = 64,
#elif defined(UNW_TARGET_SH)
#elif defined(UNW_TARGET_AARCH64)
    [UNW_AARCH64_X0]       = 0x00,
    [UNW_AARCH64_X1]       = 0x08,
    [UNW_AARCH64_X2]       = 0x10,
    [UNW_AARCH64_X3]       = 0x18,
    [UNW_AARCH64_X4]       = 0x20,
    [UNW_AARCH64_X5]       = 0x28,
    [UNW_AARCH64_X6]       = 0x30,
    [UNW_AARCH64_X7]       = 0x38,
    [UNW_AARCH64_X8]       = 0x40,
    [UNW_AARCH64_X9]       = 0x48,
    [UNW_AARCH64_X10]      = 0x50,
    [UNW_AARCH64_X11]      = 0x58,
    [UNW_AARCH64_X12]      = 0x60,
    [UNW_AARCH64_X13]      = 0x68,
    [UNW_AARCH64_X14]      = 0x70,
    [UNW_AARCH64_X15]      = 0x78,
    [UNW_AARCH64_X16]      = 0x80,
    [UNW_AARCH64_X17]      = 0x88,
    [UNW_AARCH64_X18]      = 0x90,
    [UNW_AARCH64_X19]      = 0x98,
    [UNW_AARCH64_X20]      = 0xa0,
    [UNW_AARCH64_X21]      = 0xa8,
    [UNW_AARCH64_X22]      = 0xb0,
    [UNW_AARCH64_X23]      = 0xb8,
    [UNW_AARCH64_X24]      = 0xc0,
    [UNW_AARCH64_X25]      = 0xc8,
    [UNW_AARCH64_X26]      = 0xd0,
    [UNW_AARCH64_X27]      = 0xd8,
    [UNW_AARCH64_X28]      = 0xe0,
    [UNW_AARCH64_X29]      = 0xe8,
    [UNW_AARCH64_X30]      = 0xf0,
    [UNW_AARCH64_SP]       = 0xf8,
    [UNW_AARCH64_PC]       = 0x100,
    [UNW_AARCH64_PSTATE]   = 0x108
#elif defined(UNW_TARGET_TILEGX)
    [UNW_TILEGX_R0]    = 0x00,
    [UNW_TILEGX_R1]    = 0x08,
    [UNW_TILEGX_R2]    = 0x10,
    [UNW_TILEGX_R3]    = 0x08,
    [UNW_TILEGX_R4]    = 0x20,
    [UNW_TILEGX_R5]    = 0x28,
    [UNW_TILEGX_R6]    = 0x30,
    [UNW_TILEGX_R7]    = 0x38,
    [UNW_TILEGX_R8]    = 0x40,
    [UNW_TILEGX_R9]    = 0x48,
    [UNW_TILEGX_R10]    = 0x50,
    [UNW_TILEGX_R11]    = 0x58,
    [UNW_TILEGX_R12]    = 0x60,
    [UNW_TILEGX_R13]    = 0x68,
    [UNW_TILEGX_R14]    = 0x70,
    [UNW_TILEGX_R15]    = 0x78,
    [UNW_TILEGX_R16]    = 0x80,
    [UNW_TILEGX_R17]    = 0x88,
    [UNW_TILEGX_R18]    = 0x90,
    [UNW_TILEGX_R19]    = 0x98,
    [UNW_TILEGX_R20]    = 0xa0,
    [UNW_TILEGX_R21]    = 0xa8,
    [UNW_TILEGX_R22]    = 0xb0,
    [UNW_TILEGX_R23]    = 0xb8,
    [UNW_TILEGX_R24]    = 0xc0,
    [UNW_TILEGX_R25]    = 0xc8,
    [UNW_TILEGX_R26]    = 0xd0,
    [UNW_TILEGX_R27]    = 0xd8,
    [UNW_TILEGX_R28]    = 0xe0,
    [UNW_TILEGX_R29]    = 0xe8,
    [UNW_TILEGX_R30]    = 0xf0,
    [UNW_TILEGX_R31]    = 0xf8,
    [UNW_TILEGX_R32]    = 0x100,
    [UNW_TILEGX_R33]    = 0x108,
    [UNW_TILEGX_R34]    = 0x110,
    [UNW_TILEGX_R35]    = 0x118,
    [UNW_TILEGX_R36]    = 0x120,
    [UNW_TILEGX_R37]    = 0x128,
    [UNW_TILEGX_R38]    = 0x130,
    [UNW_TILEGX_R39]    = 0x138,
    [UNW_TILEGX_R40]    = 0x140,
    [UNW_TILEGX_R41]    = 0x148,
    [UNW_TILEGX_R42]    = 0x150,
    [UNW_TILEGX_R43]    = 0x158,
    [UNW_TILEGX_R44]    = 0x160,
    [UNW_TILEGX_R45]    = 0x168,
    [UNW_TILEGX_R46]    = 0x170,
    [UNW_TILEGX_R47]    = 0x178,
    [UNW_TILEGX_R48]    = 0x180,
    [UNW_TILEGX_R49]    = 0x188,
    [UNW_TILEGX_R50]    = 0x190,
    [UNW_TILEGX_R51]    = 0x198,
    [UNW_TILEGX_R52]    = 0x1a0,
    [UNW_TILEGX_R53]    = 0x1a8,
    [UNW_TILEGX_R54]    = 0x1b0,
    [UNW_TILEGX_R55]    = 0x1b8,
    [UNW_TILEGX_PC]     = 0x1a0
#else
# error Fix me.
#endif
  };
