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
#include "ucontext_i.h"
#include <signal.h>

/* This definition originates in /usr/include/asm-ppc64/ptrace.h, but is
   defined there only when __KERNEL__ is defined.  We reproduce it here for
   our use at the user level in order to locate the ucontext record, which
   appears to be at this offset relative to the stack pointer when in the
   context of the signal handler return trampoline code -
   __kernel_sigtramp_rt64.  */
#define __SIGNAL_FRAMESIZE 128

/* This definition comes from the document "64-bit PowerPC ELF Application
   Binary Interface Supplement 1.9", section 3.2.2.
   http://www.linux-foundation.org/spec/ELF/ppc64/PPC-elf64abi-1.9.html#STACK */

typedef struct
{
  long unsigned back_chain;
  long unsigned lr_save;
  /* many more fields here, but they are unused by this code */
} stack_frame_t;


PROTECTED int
unw_step (unw_cursor_t * cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  stack_frame_t dummy;
  unw_word_t back_chain_offset, lr_save_offset;
  struct dwarf_loc back_chain_loc, lr_save_loc, sp_loc, ip_loc;
  int ret;

  Debug (1, "(cursor=%p, ip=0x%016lx)\n", c, (unsigned long) c->dwarf.ip);

  if (c->dwarf.ip == 0)
    {
      /* Unless the cursor or stack is corrupt or uninitialized,
         we've most likely hit the top of the stack */
      return 0;
    }

  /* Try DWARF-based unwinding... */

  ret = dwarf_step (&c->dwarf);

  if (ret < 0 && ret != -UNW_ENOINFO)
    {
      Debug (2, "returning %d\n", ret);
      return ret;
    }

  if (unlikely (ret < 0))
    {
      if (likely (unw_is_signal_frame (cursor) <= 0))
        {
          /* DWARF unwinding failed.  As of 09/26/2006, gcc in 64-bit mode
             produces the mandatory level of traceback record in the code, but
             I get the impression that this is transitory, that eventually gcc
             will not produce any traceback records at all.  So, for now, we
             won't bother to try to find and use these records.

             We can, however, attempt to unwind the frame by using the callback
             chain.  This is very crude, however, and won't be able to unwind
             any registers besides the IP, SP, and LR . */

          back_chain_offset = ((void *) &dummy.back_chain - (void *) &dummy);
          lr_save_offset = ((void *) &dummy.lr_save - (void *) &dummy);

          back_chain_loc = DWARF_LOC (c->dwarf.cfa + back_chain_offset, 0);

          if ((ret =
               dwarf_get (&c->dwarf, back_chain_loc, &c->dwarf.cfa)) < 0)
            {
              Debug (2,
                 "Unable to retrieve CFA from back chain in stack frame - %d\n",
                 ret);
              return ret;
            }
          if (c->dwarf.cfa == 0)
            /* Unless the cursor or stack is corrupt or uninitialized we've most
               likely hit the top of the stack */
            return 0;

          lr_save_loc = DWARF_LOC (c->dwarf.cfa + lr_save_offset, 0);

          if ((ret = dwarf_get (&c->dwarf, lr_save_loc, &c->dwarf.ip)) < 0)
            {
              Debug (2,
                 "Unable to retrieve IP from lr save in stack frame - %d\n",
                 ret);
              return ret;
            }
          ret = 1;
        }
      else
        {
          /* Find the sigcontext record by taking the CFA and adjusting by
             the dummy signal frame size.

             Note that there isn't any way to determined if SA_SIGINFO was
             set in the sa_flags parameter to sigaction when the signal
             handler was established.  If it was not set, the ucontext
             record is not required to be on the stack, in which case the
             following code will likely cause a seg fault or other crash
             condition.  */

          unw_word_t ucontext = c->dwarf.cfa + __SIGNAL_FRAMESIZE;

          Debug (1, "signal frame, skip over trampoline\n");

          c->sigcontext_format = PPC_SCF_LINUX_RT_SIGFRAME;
          c->sigcontext_addr = ucontext;

          sp_loc = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R1, 0);
          ip_loc = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_LINK, 0);

          ret = dwarf_get (&c->dwarf, sp_loc, &c->dwarf.cfa);
          if (ret < 0)
            {
              Debug (2, "returning %d\n", ret);
              return ret;
            }
          ret = dwarf_get (&c->dwarf, ip_loc, &c->dwarf.ip);
          if (ret < 0)
            {
              Debug (2, "returning %d\n", ret);
              return ret;
            }

          /* Instead of just restoring the non-volatile registers, do all
             of the registers for now.  This will incur a performance hit,
             but it's rare enough not to cause too much of a problem, and
             might be useful in some cases.  */
          c->dwarf.loc[UNW_PPC32_R0] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R0, 0);
          c->dwarf.loc[UNW_PPC32_R1] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R1, 0);
          c->dwarf.loc[UNW_PPC32_R2] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R2, 0);
          c->dwarf.loc[UNW_PPC32_R3] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R3, 0);
          c->dwarf.loc[UNW_PPC32_R4] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R4, 0);
          c->dwarf.loc[UNW_PPC32_R5] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R5, 0);
          c->dwarf.loc[UNW_PPC32_R6] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R6, 0);
          c->dwarf.loc[UNW_PPC32_R7] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R7, 0);
          c->dwarf.loc[UNW_PPC32_R8] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R8, 0);
          c->dwarf.loc[UNW_PPC32_R9] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R9, 0);
          c->dwarf.loc[UNW_PPC32_R10] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R10, 0);
          c->dwarf.loc[UNW_PPC32_R11] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R11, 0);
          c->dwarf.loc[UNW_PPC32_R12] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R12, 0);
          c->dwarf.loc[UNW_PPC32_R13] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R13, 0);
          c->dwarf.loc[UNW_PPC32_R14] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R14, 0);
          c->dwarf.loc[UNW_PPC32_R15] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R15, 0);
          c->dwarf.loc[UNW_PPC32_R16] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R16, 0);
          c->dwarf.loc[UNW_PPC32_R17] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R17, 0);
          c->dwarf.loc[UNW_PPC32_R18] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R18, 0);
          c->dwarf.loc[UNW_PPC32_R19] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R19, 0);
          c->dwarf.loc[UNW_PPC32_R20] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R20, 0);
          c->dwarf.loc[UNW_PPC32_R21] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R21, 0);
          c->dwarf.loc[UNW_PPC32_R22] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R22, 0);
          c->dwarf.loc[UNW_PPC32_R23] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R23, 0);
          c->dwarf.loc[UNW_PPC32_R24] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R24, 0);
          c->dwarf.loc[UNW_PPC32_R25] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R25, 0);
          c->dwarf.loc[UNW_PPC32_R26] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R26, 0);
          c->dwarf.loc[UNW_PPC32_R27] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R27, 0);
          c->dwarf.loc[UNW_PPC32_R28] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R28, 0);
          c->dwarf.loc[UNW_PPC32_R29] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R29, 0);
          c->dwarf.loc[UNW_PPC32_R30] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R30, 0);
          c->dwarf.loc[UNW_PPC32_R31] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R31, 0);

          c->dwarf.loc[UNW_PPC32_LR] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_LINK, 0);
          c->dwarf.loc[UNW_PPC32_CTR] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_CTR, 0);

          /* This CR0 assignment is probably wrong.  There are 8 dwarf columns
             assigned to the CR registers, but only one CR register in the
             mcontext structure */
          c->dwarf.loc[UNW_PPC32_CCR] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_CCR, 0);
          c->dwarf.loc[UNW_PPC32_XER] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_XER, 0);

          c->dwarf.loc[UNW_PPC32_F0] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R0, 0);
          c->dwarf.loc[UNW_PPC32_F1] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R1, 0);
          c->dwarf.loc[UNW_PPC32_F2] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R2, 0);
          c->dwarf.loc[UNW_PPC32_F3] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R3, 0);
          c->dwarf.loc[UNW_PPC32_F4] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R4, 0);
          c->dwarf.loc[UNW_PPC32_F5] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R5, 0);
          c->dwarf.loc[UNW_PPC32_F6] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R6, 0);
          c->dwarf.loc[UNW_PPC32_F7] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R7, 0);
          c->dwarf.loc[UNW_PPC32_F8] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R8, 0);
          c->dwarf.loc[UNW_PPC32_F9] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R9, 0);
          c->dwarf.loc[UNW_PPC32_F10] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R10, 0);
          c->dwarf.loc[UNW_PPC32_F11] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R11, 0);
          c->dwarf.loc[UNW_PPC32_F12] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R12, 0);
          c->dwarf.loc[UNW_PPC32_F13] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R13, 0);
          c->dwarf.loc[UNW_PPC32_F14] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R14, 0);
          c->dwarf.loc[UNW_PPC32_F15] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R15, 0);
          c->dwarf.loc[UNW_PPC32_F16] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R16, 0);
          c->dwarf.loc[UNW_PPC32_F17] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R17, 0);
          c->dwarf.loc[UNW_PPC32_F18] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R18, 0);
          c->dwarf.loc[UNW_PPC32_F19] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R19, 0);
          c->dwarf.loc[UNW_PPC32_F20] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R20, 0);
          c->dwarf.loc[UNW_PPC32_F21] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R21, 0);
          c->dwarf.loc[UNW_PPC32_F22] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R22, 0);
          c->dwarf.loc[UNW_PPC32_F23] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R23, 0);
          c->dwarf.loc[UNW_PPC32_F24] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R24, 0);
          c->dwarf.loc[UNW_PPC32_F25] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R25, 0);
          c->dwarf.loc[UNW_PPC32_F26] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R26, 0);
          c->dwarf.loc[UNW_PPC32_F27] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R27, 0);
          c->dwarf.loc[UNW_PPC32_F28] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R28, 0);
          c->dwarf.loc[UNW_PPC32_F29] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R29, 0);
          c->dwarf.loc[UNW_PPC32_F30] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R30, 0);
          c->dwarf.loc[UNW_PPC32_F31] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R31, 0);

          ret = 1;
        }
    }
  return ret;
}
