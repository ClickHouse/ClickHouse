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
sh_local_resume (unw_addr_space_t as, unw_cursor_t *cursor, void *arg)
{
#ifdef __linux__
  struct cursor *c = (struct cursor *) cursor;
  unw_tdep_context_t *uc = c->dwarf.as_arg;

  if (c->sigcontext_format == SH_SCF_NONE)
    {
      /* Since there are no signals involved here we restore the non scratch
         registers only.  */
      unsigned long regs[8];
      regs[0] = uc->uc_mcontext.gregs[8];
      regs[1] = uc->uc_mcontext.gregs[9];
      regs[2] = uc->uc_mcontext.gregs[10];
      regs[3] = uc->uc_mcontext.gregs[11];
      regs[4] = uc->uc_mcontext.gregs[12];
      regs[5] = uc->uc_mcontext.gregs[13];
      regs[6] = uc->uc_mcontext.gregs[14];
      regs[7] = uc->uc_mcontext.gregs[15];
      unsigned long pc = uc->uc_mcontext.pr;

      struct regs_overlay {
        char x[sizeof(regs)];
      };

      asm volatile (
        "mov.l @%0+, r8\n"
        "mov.l @%0+, r9\n"
        "mov.l @%0+, r10\n"
        "mov.l @%0+, r11\n"
        "mov.l @%0+, r12\n"
        "mov.l @%0+, r13\n"
        "mov.l @%0+, r14\n"
        "mov.l @%0,  r15\n"
        "lds %1, pr\n"
        "rts\n"
        "nop\n"
        :
        : "r" (regs),
          "r" (pc),
          "m" (*(struct regs_overlay *)regs)
      );
    }
  else
    {
      /* In case a signal frame is involved, we're using its trampoline which
         calls sigreturn.  */
      struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;
      sc->sc_regs[0] = uc->uc_mcontext.gregs[0];
      sc->sc_regs[1] = uc->uc_mcontext.gregs[1];
      sc->sc_regs[2] = uc->uc_mcontext.gregs[2];
      sc->sc_regs[3] = uc->uc_mcontext.gregs[3];
      sc->sc_regs[4] = uc->uc_mcontext.gregs[4];
      sc->sc_regs[5] = uc->uc_mcontext.gregs[5];
      sc->sc_regs[6] = uc->uc_mcontext.gregs[6];
      sc->sc_regs[7] = uc->uc_mcontext.gregs[7];
      sc->sc_regs[8] = uc->uc_mcontext.gregs[8];
      sc->sc_regs[9] = uc->uc_mcontext.gregs[9];
      sc->sc_regs[10] = uc->uc_mcontext.gregs[10];
      sc->sc_regs[11] = uc->uc_mcontext.gregs[11];
      sc->sc_regs[12] = uc->uc_mcontext.gregs[12];
      sc->sc_regs[13] = uc->uc_mcontext.gregs[13];
      sc->sc_regs[14] = uc->uc_mcontext.gregs[14];
      sc->sc_regs[15] = uc->uc_mcontext.gregs[15];
      sc->sc_pc = uc->uc_mcontext.pc;
      sc->sc_pr = uc->uc_mcontext.pr;

      /* Set the SP and the PC in order to continue execution at the modified
         trampoline which restores the signal mask and the registers.  */
      asm __volatile__ (
        "mov %0, r15\n"
        "lds %1, pr\n"
        "rts\n"
        "nop\n"
        :
        : "r" (c->sigcontext_sp),
          "r" (c->sigcontext_pc)
      );
   }
  unreachable();
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
