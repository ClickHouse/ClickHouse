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

#include "unwind_i.h"
#include "offsets.h"

#ifndef UNW_REMOTE_ONLY

HIDDEN inline int
arm_local_resume (unw_addr_space_t as, unw_cursor_t *cursor, void *arg)
{
#ifdef __linux__
  struct cursor *c = (struct cursor *) cursor;
  unw_tdep_context_t *uc = c->dwarf.as_arg;

  if (c->sigcontext_format == ARM_SCF_NONE)
    {
      /* Since there are no signals involved here we restore the non scratch
         registers only.  */
      unsigned long regs[10];
      regs[0] = uc->regs[4];
      regs[1] = uc->regs[5];
      regs[2] = uc->regs[6];
      regs[3] = uc->regs[7];
      regs[4] = uc->regs[8];
      regs[5] = uc->regs[9];
      regs[6] = uc->regs[10];
      regs[7] = uc->regs[11]; /* FP */
      regs[8] = uc->regs[13]; /* SP */
      regs[9] = uc->regs[14]; /* LR */

      struct regs_overlay {
              char x[sizeof(regs)];
      };

      asm __volatile__ (
        "ldmia %0, {r4-r12, lr}\n"
        "mov sp, r12\n"
        "bx lr\n"
        : : "r" (regs),
            "m" (*(struct regs_overlay *)regs)
      );
    }
  else
    {
      /* In case a signal frame is involved, we're using its trampoline which
         calls sigreturn.  */
      struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;
      sc->arm_r0 = uc->regs[0];
      sc->arm_r1 = uc->regs[1];
      sc->arm_r2 = uc->regs[2];
      sc->arm_r3 = uc->regs[3];
      sc->arm_r4 = uc->regs[4];
      sc->arm_r5 = uc->regs[5];
      sc->arm_r6 = uc->regs[6];
      sc->arm_r7 = uc->regs[7];
      sc->arm_r8 = uc->regs[8];
      sc->arm_r9 = uc->regs[9];
      sc->arm_r10 = uc->regs[10];
      sc->arm_fp = uc->regs[11]; /* FP */
      sc->arm_ip = uc->regs[12]; /* IP */
      sc->arm_sp = uc->regs[13]; /* SP */
      sc->arm_lr = uc->regs[14]; /* LR */
      sc->arm_pc = uc->regs[15]; /* PC */
      /* clear the ITSTATE bits.  */
      sc->arm_cpsr &= 0xf9ff03ffUL;

      /* Set the SP and the PC in order to continue execution at the modified
         trampoline which restores the signal mask and the registers.  */
      asm __volatile__ (
        "mov sp, %0\n"
        "bx %1\n"
        : : "r" (c->sigcontext_sp), "r" (c->sigcontext_pc)
      );
   }
  unreachable();
#else
  printf ("%s: implement me\n", __FUNCTION__);
#endif
  return -UNW_EINVAL;
}

#endif /* !UNW_REMOTE_ONLY */

static inline void
establish_machine_state (struct cursor *c)
{
  unw_addr_space_t as = c->dwarf.as;
  void *arg = c->dwarf.as_arg;
  unw_fpreg_t fpval;
  unw_word_t val;
  int reg;

  Debug (8, "copying out cursor state\n");

  for (reg = 0; reg <= UNW_REG_LAST; ++reg)
    {
      Debug (16, "copying %s %d\n", unw_regname (reg), reg);
      if (unw_is_fpreg (reg))
        {
          if (tdep_access_fpreg (c, reg, &fpval, 0) >= 0)
            as->acc.access_fpreg (as, reg, &fpval, 1, arg);
        }
      else
        {
          if (tdep_access_reg (c, reg, &val, 0) >= 0)
            as->acc.access_reg (as, reg, &val, 1, arg);
        }
    }
}

PROTECTED int
unw_resume (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;

  Debug (1, "(cursor=%p)\n", c);

  if (!c->dwarf.ip)
    {
      /* This can happen easily when the frame-chain gets truncated
         due to bad or missing unwind-info.  */
      Debug (1, "refusing to resume execution at address 0\n");
      return -UNW_EINVAL;
    }

  establish_machine_state (c);

  return (*c->dwarf.as->acc.resume) (c->dwarf.as, (unw_cursor_t *) c,
                                     c->dwarf.as_arg);
}
