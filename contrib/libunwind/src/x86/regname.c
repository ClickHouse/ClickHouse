#include "unwind_i.h"

static const char *regname[] =
  {
    "eax", "edx", "ecx", "ebx", "esi", "edi", "ebp", "esp", "eip",
    "eflags", "trapno",
    "st0", "st1", "st2", "st3", "st4", "st5", "st6", "st7",
    "fcw", "fsw", "ftw", "fop", "fcs", "fip", "fea", "fds",
    "xmm0_lo", "xmm0_hi", "xmm1_lo", "xmm1_hi",
    "xmm2_lo", "xmm2_hi", "xmm3_lo", "xmm3_hi",
    "xmm4_lo", "xmm4_hi", "xmm5_lo", "xmm5_hi",
    "xmm6_lo", "xmm6_hi", "xmm7_lo", "xmm7_hi",
    "mxcsr",
    "gs", "fs", "es", "ds", "ss", "cs",
    "tss", "ldt",
    "cfi",
    "xmm0", "xmm1", "xmm2", "xmm3", "xmm4", "xmm5", "xmm6", "xmm7",
  };

PROTECTED const char *
unw_regname (unw_regnum_t reg)
{
  if (reg < (unw_regnum_t) ARRAY_SIZE (regname))
    return regname[reg];
  else
    return "???";
}
