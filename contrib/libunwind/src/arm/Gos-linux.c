/* libunwind - a platform-independent unwind library
   Copyright (C) 2008 CodeSourcery
   Copyright 2011 Linaro Limited
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

#include <stdio.h>
#include <signal.h>
#include "unwind_i.h"
#include "offsets.h"

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;
  unw_word_t sc_addr, sp, sp_addr = c->dwarf.cfa;
  struct dwarf_loc sp_loc = DWARF_LOC (sp_addr, 0);

  if ((ret = dwarf_get (&c->dwarf, sp_loc, &sp)) < 0)
    return -UNW_EUNSPEC;

  /* Obtain signal frame type (non-RT or RT). */
  ret = unw_is_signal_frame (cursor);

  /* Save the SP and PC to be able to return execution at this point
     later in time (unw_resume).  */
  c->sigcontext_sp = c->dwarf.cfa;
  c->sigcontext_pc = c->dwarf.ip;

  /* Since kernel version 2.6.18 the non-RT signal frame starts with a
     ucontext while the RT signal frame starts with a siginfo, followed
     by a sigframe whose first element is an ucontext.
     Prior 2.6.18 the non-RT signal frame starts with a sigcontext while
     the RT signal frame starts with two pointers followed by a siginfo
     and an ucontext. The first pointer points to the start of the siginfo
     structure and the second one to the ucontext structure.  */

  if (ret == 1)
    {
      /* Handle non-RT signal frames. Check if the first word on the stack
         is the magic number.  */
      if (sp == 0x5ac3c35a)
        {
          c->sigcontext_format = ARM_SCF_LINUX_SIGFRAME;
          sc_addr = sp_addr + LINUX_UC_MCONTEXT_OFF;
        }
      else
        {
          c->sigcontext_format = ARM_SCF_LINUX_OLD_SIGFRAME;
          sc_addr = sp_addr;
        }
    }
  else if (ret == 2)
    {
      /* Handle RT signal frames. Check if the first word on the stack is a
         pointer to the siginfo structure.  */
      if (sp == sp_addr + 8)
        {
          c->sigcontext_format = ARM_SCF_LINUX_OLD_RT_SIGFRAME;
          sc_addr = sp_addr + 8 + sizeof (siginfo_t) + LINUX_UC_MCONTEXT_OFF;
        }
      else
        {
          c->sigcontext_format = ARM_SCF_LINUX_RT_SIGFRAME;
          sc_addr = sp_addr + sizeof (siginfo_t) + LINUX_UC_MCONTEXT_OFF;
        }
    }
  else
    return -UNW_EUNSPEC;

  c->sigcontext_addr = sc_addr;
  c->frame_info.frame_type = UNW_ARM_FRAME_SIGRETURN;
  c->frame_info.cfa_reg_offset = sc_addr - sp_addr;

  /* Update the dwarf cursor.
     Set the location of the registers to the corresponding addresses of the
     uc_mcontext / sigcontext structure contents.  */
  c->dwarf.loc[UNW_ARM_R0] = DWARF_LOC (sc_addr + LINUX_SC_R0_OFF, 0);
  c->dwarf.loc[UNW_ARM_R1] = DWARF_LOC (sc_addr + LINUX_SC_R1_OFF, 0);
  c->dwarf.loc[UNW_ARM_R2] = DWARF_LOC (sc_addr + LINUX_SC_R2_OFF, 0);
  c->dwarf.loc[UNW_ARM_R3] = DWARF_LOC (sc_addr + LINUX_SC_R3_OFF, 0);
  c->dwarf.loc[UNW_ARM_R4] = DWARF_LOC (sc_addr + LINUX_SC_R4_OFF, 0);
  c->dwarf.loc[UNW_ARM_R5] = DWARF_LOC (sc_addr + LINUX_SC_R5_OFF, 0);
  c->dwarf.loc[UNW_ARM_R6] = DWARF_LOC (sc_addr + LINUX_SC_R6_OFF, 0);
  c->dwarf.loc[UNW_ARM_R7] = DWARF_LOC (sc_addr + LINUX_SC_R7_OFF, 0);
  c->dwarf.loc[UNW_ARM_R8] = DWARF_LOC (sc_addr + LINUX_SC_R8_OFF, 0);
  c->dwarf.loc[UNW_ARM_R9] = DWARF_LOC (sc_addr + LINUX_SC_R9_OFF, 0);
  c->dwarf.loc[UNW_ARM_R10] = DWARF_LOC (sc_addr + LINUX_SC_R10_OFF, 0);
  c->dwarf.loc[UNW_ARM_R11] = DWARF_LOC (sc_addr + LINUX_SC_FP_OFF, 0);
  c->dwarf.loc[UNW_ARM_R12] = DWARF_LOC (sc_addr + LINUX_SC_IP_OFF, 0);
  c->dwarf.loc[UNW_ARM_R13] = DWARF_LOC (sc_addr + LINUX_SC_SP_OFF, 0);
  c->dwarf.loc[UNW_ARM_R14] = DWARF_LOC (sc_addr + LINUX_SC_LR_OFF, 0);
  c->dwarf.loc[UNW_ARM_R15] = DWARF_LOC (sc_addr + LINUX_SC_PC_OFF, 0);

  /* Set SP/CFA and PC/IP.  */
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_ARM_R13], &c->dwarf.cfa);
  dwarf_get (&c->dwarf, c->dwarf.loc[UNW_ARM_R15], &c->dwarf.ip);

  c->dwarf.pi_valid = 0;

  return 1;
}

#define ARM_NR_sigreturn 119
#define ARM_NR_rt_sigreturn 173
#define ARM_NR_OABI_SYSCALL_BASE 0x900000

/* ARM EABI sigreturn (the syscall number is loaded into r7) */
#define MOV_R7_SIGRETURN (0xe3a07000UL | ARM_NR_sigreturn)
#define MOV_R7_RT_SIGRETURN (0xe3a07000UL | ARM_NR_rt_sigreturn)

/* ARM OABI sigreturn (using SWI) */
#define ARM_SIGRETURN \
  (0xef000000UL | ARM_NR_sigreturn | ARM_NR_OABI_SYSCALL_BASE)
#define ARM_RT_SIGRETURN \
  (0xef000000UL | ARM_NR_rt_sigreturn | ARM_NR_OABI_SYSCALL_BASE)

/* Thumb sigreturn (two insns, syscall number is loaded into r7) */
#define THUMB_SIGRETURN (0xdf00UL << 16 | 0x2700 | ARM_NR_sigreturn)
#define THUMB_RT_SIGRETURN (0xdf00UL << 16 | 0x2700 | ARM_NR_rt_sigreturn)

/* Thumb2 sigreturn (mov.w r7, $SYS_ify(rt_sigreturn/sigreturn)) */
#define THUMB2_SIGRETURN (((0x0700 | ARM_NR_sigreturn) << 16) | \
                                       0xf04f)
#define THUMB2_RT_SIGRETURN (((0x0700 | ARM_NR_rt_sigreturn) << 16) | \
                                       0xf04f)
/* TODO: with different toolchains, there are a lot more possibilities */

/* Returns 1 in case of a non-RT signal frame and 2 in case of a RT signal
   frame. */
PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  /* The least bit denotes thumb/arm mode. Do not read there. */
  ip = c->dwarf.ip & ~0x1;

  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0)
    return ret;

  /* Return 1 if the IP points to a non-RT sigreturn sequence.  */
  if (w0 == MOV_R7_SIGRETURN || w0 == ARM_SIGRETURN || w0 == THUMB_SIGRETURN
           || w0 == THUMB2_SIGRETURN)
    return 1;
  /* Return 2 if the IP points to a RT sigreturn sequence.  */
  else if (w0 == MOV_R7_RT_SIGRETURN || w0 == ARM_RT_SIGRETURN
           || w0 == THUMB_RT_SIGRETURN || w0 == THUMB2_RT_SIGRETURN)
    return 2;

  return 0;
}
