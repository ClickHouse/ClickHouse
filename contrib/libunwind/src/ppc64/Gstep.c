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
#include "remote.h"
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
  long unsigned cr_save;
  long unsigned lr_save;
  /* many more fields here, but they are unused by this code */
} stack_frame_t;


PROTECTED int
unw_step (unw_cursor_t * cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  stack_frame_t dummy;
  unw_word_t back_chain_offset, lr_save_offset, v_regs_ptr;
  struct dwarf_loc back_chain_loc, lr_save_loc, sp_loc, ip_loc, v_regs_loc;
  int ret, i;

  Debug (1, "(cursor=%p, ip=0x%016lx)\n", c, (unsigned long) c->dwarf.ip);

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

          /* Mark all registers unsaved */
          for (i = 0; i < DWARF_NUM_PRESERVED_REGS; ++i)
            c->dwarf.loc[i] = DWARF_NULL_LOC;

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
          ip_loc = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_NIP, 0);

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
          c->dwarf.loc[UNW_PPC64_R0] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R0, 0);
          c->dwarf.loc[UNW_PPC64_R1] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R1, 0);
          c->dwarf.loc[UNW_PPC64_R2] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R2, 0);
          c->dwarf.loc[UNW_PPC64_R3] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R3, 0);
          c->dwarf.loc[UNW_PPC64_R4] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R4, 0);
          c->dwarf.loc[UNW_PPC64_R5] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R5, 0);
          c->dwarf.loc[UNW_PPC64_R6] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R6, 0);
          c->dwarf.loc[UNW_PPC64_R7] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R7, 0);
          c->dwarf.loc[UNW_PPC64_R8] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R8, 0);
          c->dwarf.loc[UNW_PPC64_R9] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R9, 0);
          c->dwarf.loc[UNW_PPC64_R10] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R10, 0);
          c->dwarf.loc[UNW_PPC64_R11] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R11, 0);
          c->dwarf.loc[UNW_PPC64_R12] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R12, 0);
          c->dwarf.loc[UNW_PPC64_R13] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R13, 0);
          c->dwarf.loc[UNW_PPC64_R14] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R14, 0);
          c->dwarf.loc[UNW_PPC64_R15] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R15, 0);
          c->dwarf.loc[UNW_PPC64_R16] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R16, 0);
          c->dwarf.loc[UNW_PPC64_R17] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R17, 0);
          c->dwarf.loc[UNW_PPC64_R18] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R18, 0);
          c->dwarf.loc[UNW_PPC64_R19] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R19, 0);
          c->dwarf.loc[UNW_PPC64_R20] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R20, 0);
          c->dwarf.loc[UNW_PPC64_R21] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R21, 0);
          c->dwarf.loc[UNW_PPC64_R22] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R22, 0);
          c->dwarf.loc[UNW_PPC64_R23] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R23, 0);
          c->dwarf.loc[UNW_PPC64_R24] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R24, 0);
          c->dwarf.loc[UNW_PPC64_R25] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R25, 0);
          c->dwarf.loc[UNW_PPC64_R26] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R26, 0);
          c->dwarf.loc[UNW_PPC64_R27] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R27, 0);
          c->dwarf.loc[UNW_PPC64_R28] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R28, 0);
          c->dwarf.loc[UNW_PPC64_R29] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R29, 0);
          c->dwarf.loc[UNW_PPC64_R30] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R30, 0);
          c->dwarf.loc[UNW_PPC64_R31] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R31, 0);

          c->dwarf.loc[UNW_PPC64_LR] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_LINK, 0);
          c->dwarf.loc[UNW_PPC64_CTR] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_CTR, 0);
          /* This CR0 assignment is probably wrong.  There are 8 dwarf columns
             assigned to the CR registers, but only one CR register in the
             mcontext structure */
          c->dwarf.loc[UNW_PPC64_CR0] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_CCR, 0);
          c->dwarf.loc[UNW_PPC64_XER] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_XER, 0);
          c->dwarf.loc[UNW_PPC64_NIP] =
            DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_NIP, 0);

          /* TODO: Is there a way of obtaining the value of the
             pseudo frame pointer (which is sp + some fixed offset, I
             assume), based on the contents of the ucontext record
             structure?  For now, set this loc to null. */
          c->dwarf.loc[UNW_PPC64_FRAME_POINTER] = DWARF_NULL_LOC;

          c->dwarf.loc[UNW_PPC64_F0] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R0, 0);
          c->dwarf.loc[UNW_PPC64_F1] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R1, 0);
          c->dwarf.loc[UNW_PPC64_F2] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R2, 0);
          c->dwarf.loc[UNW_PPC64_F3] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R3, 0);
          c->dwarf.loc[UNW_PPC64_F4] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R4, 0);
          c->dwarf.loc[UNW_PPC64_F5] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R5, 0);
          c->dwarf.loc[UNW_PPC64_F6] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R6, 0);
          c->dwarf.loc[UNW_PPC64_F7] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R7, 0);
          c->dwarf.loc[UNW_PPC64_F8] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R8, 0);
          c->dwarf.loc[UNW_PPC64_F9] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R9, 0);
          c->dwarf.loc[UNW_PPC64_F10] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R10, 0);
          c->dwarf.loc[UNW_PPC64_F11] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R11, 0);
          c->dwarf.loc[UNW_PPC64_F12] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R12, 0);
          c->dwarf.loc[UNW_PPC64_F13] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R13, 0);
          c->dwarf.loc[UNW_PPC64_F14] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R14, 0);
          c->dwarf.loc[UNW_PPC64_F15] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R15, 0);
          c->dwarf.loc[UNW_PPC64_F16] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R16, 0);
          c->dwarf.loc[UNW_PPC64_F17] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R17, 0);
          c->dwarf.loc[UNW_PPC64_F18] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R18, 0);
          c->dwarf.loc[UNW_PPC64_F19] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R19, 0);
          c->dwarf.loc[UNW_PPC64_F20] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R20, 0);
          c->dwarf.loc[UNW_PPC64_F21] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R21, 0);
          c->dwarf.loc[UNW_PPC64_F22] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R22, 0);
          c->dwarf.loc[UNW_PPC64_F23] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R23, 0);
          c->dwarf.loc[UNW_PPC64_F24] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R24, 0);
          c->dwarf.loc[UNW_PPC64_F25] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R25, 0);
          c->dwarf.loc[UNW_PPC64_F26] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R26, 0);
          c->dwarf.loc[UNW_PPC64_F27] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R27, 0);
          c->dwarf.loc[UNW_PPC64_F28] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R28, 0);
          c->dwarf.loc[UNW_PPC64_F29] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R29, 0);
          c->dwarf.loc[UNW_PPC64_F30] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R30, 0);
          c->dwarf.loc[UNW_PPC64_F31] =
            DWARF_LOC (ucontext + UC_MCONTEXT_FREGS_R31, 0);
          /* Note that there is no .eh_section register column for the
             FPSCR register.  I don't know why this is.  */

          v_regs_loc = DWARF_LOC (ucontext + UC_MCONTEXT_V_REGS, 0);
          ret = dwarf_get (&c->dwarf, v_regs_loc, &v_regs_ptr);
          if (ret < 0)
            {
              Debug (2, "returning %d\n", ret);
              return ret;
            }
          if (v_regs_ptr != 0)
            {
              /* The v_regs_ptr is not null.  Set all of the AltiVec locs */

              c->dwarf.loc[UNW_PPC64_V0] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R0, 0);
              c->dwarf.loc[UNW_PPC64_V1] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R1, 0);
              c->dwarf.loc[UNW_PPC64_V2] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R2, 0);
              c->dwarf.loc[UNW_PPC64_V3] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R3, 0);
              c->dwarf.loc[UNW_PPC64_V4] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R4, 0);
              c->dwarf.loc[UNW_PPC64_V5] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R5, 0);
              c->dwarf.loc[UNW_PPC64_V6] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R6, 0);
              c->dwarf.loc[UNW_PPC64_V7] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R7, 0);
              c->dwarf.loc[UNW_PPC64_V8] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R8, 0);
              c->dwarf.loc[UNW_PPC64_V9] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R9, 0);
              c->dwarf.loc[UNW_PPC64_V10] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R10, 0);
              c->dwarf.loc[UNW_PPC64_V11] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R11, 0);
              c->dwarf.loc[UNW_PPC64_V12] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R12, 0);
              c->dwarf.loc[UNW_PPC64_V13] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R13, 0);
              c->dwarf.loc[UNW_PPC64_V14] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R14, 0);
              c->dwarf.loc[UNW_PPC64_V15] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R15, 0);
              c->dwarf.loc[UNW_PPC64_V16] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R16, 0);
              c->dwarf.loc[UNW_PPC64_V17] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R17, 0);
              c->dwarf.loc[UNW_PPC64_V18] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R18, 0);
              c->dwarf.loc[UNW_PPC64_V19] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R19, 0);
              c->dwarf.loc[UNW_PPC64_V20] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R20, 0);
              c->dwarf.loc[UNW_PPC64_V21] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R21, 0);
              c->dwarf.loc[UNW_PPC64_V22] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R22, 0);
              c->dwarf.loc[UNW_PPC64_V23] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R23, 0);
              c->dwarf.loc[UNW_PPC64_V24] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R24, 0);
              c->dwarf.loc[UNW_PPC64_V25] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R25, 0);
              c->dwarf.loc[UNW_PPC64_V26] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R26, 0);
              c->dwarf.loc[UNW_PPC64_V27] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R27, 0);
              c->dwarf.loc[UNW_PPC64_V28] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R28, 0);
              c->dwarf.loc[UNW_PPC64_V29] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R29, 0);
              c->dwarf.loc[UNW_PPC64_V30] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R30, 0);
              c->dwarf.loc[UNW_PPC64_V31] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_R31, 0);
              c->dwarf.loc[UNW_PPC64_VRSAVE] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_VRSAVE, 0);
              c->dwarf.loc[UNW_PPC64_VSCR] =
                DWARF_LOC (v_regs_ptr + UC_MCONTEXT_VREGS_VSCR, 0);
            }
          else
            {
              c->dwarf.loc[UNW_PPC64_V0] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V1] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V2] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V3] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V4] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V5] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V6] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V7] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V8] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V9] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V10] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V11] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V12] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V13] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V14] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V15] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V16] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V17] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V18] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V19] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V20] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V21] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V22] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V23] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V24] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V25] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V26] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V27] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V28] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V29] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V30] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_V31] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_VRSAVE] = DWARF_NULL_LOC;
              c->dwarf.loc[UNW_PPC64_VSCR] = DWARF_NULL_LOC;
            }
          ret = 1;
        }
    }

  if (c->dwarf.ip == 0)
    {
      /* Unless the cursor or stack is corrupt or uninitialized,
         we've most likely hit the top of the stack */
      Debug (2, "returning 0\n");
      return 0;
    }

  // on ppc64, R2 register is used as pointer to TOC
  // section which is used for symbol lookup in PIC code
  // ppc64 linker generates "ld r2, 40(r1)" (ELFv1) or
  // "ld r2, 24(r1)" (ELFv2) instruction after each
  // @plt call. We need restore R2, but only for @plt calls
  {
    unw_word_t ip = c->dwarf.ip;
    unw_addr_space_t as = c->dwarf.as;
    unw_accessors_t *a = unw_get_accessors (as);
    void *arg = c->dwarf.as_arg;
    uint32_t toc_save = (as->abi == UNW_PPC64_ABI_ELFv2)? 24 : 40;
    int32_t inst;

    if (fetch32 (as, a, &ip, &inst, arg) >= 0
	&& (uint32_t)inst == (0xE8410000U + toc_save))
      {
	// @plt call, restoring R2 from CFA+toc_save
	c->dwarf.loc[UNW_PPC64_R2] = DWARF_LOC(c->dwarf.cfa + toc_save, 0);
      }
  }

  Debug (2, "returning %d with last return statement\n", ret);
  return ret;
}
