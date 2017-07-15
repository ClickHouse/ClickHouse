/* libunwind - a platform-independent unwind library
   Copyright (c) 2004 Hewlett-Packard Development Company, L.P.
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

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

#include <stdlib.h>

#include "unwind_i.h"

#ifndef UNW_REMOTE_ONLY

#if defined(__linux)

# include <sys/syscall.h>

static NORETURN inline long
my_rt_sigreturn (void *new_sp, int in_syscall)
{
  register unsigned long r25 __asm__ ("r25") = (in_syscall != 0);
  register unsigned long r20 __asm__ ("r20") = SYS_rt_sigreturn;

  __asm__ __volatile__ ("copy %0, %%sp\n"
                        "be,l 0x100(%%sr2,%%r0),%%sr0,%%r31\n"
                        "nop"
                        :
                        : "r"(new_sp), "r"(r20), "r"(r25)
                        : "memory");
  abort ();
}

#endif /* __linux */

HIDDEN inline int
hppa_local_resume (unw_addr_space_t as, unw_cursor_t *cursor, void *arg)
{
#if defined(__linux)
  struct cursor *c = (struct cursor *) cursor;
  ucontext_t *uc = c->dwarf.as_arg;

  /* Ensure c->pi is up-to-date.  On PA-RISC, it's relatively common to be
     missing DWARF unwind info.  We don't want to fail in that case,
     because the frame-chain still would let us do a backtrace at
     least.  */
  dwarf_make_proc_info (&c->dwarf);

  if (unlikely (c->sigcontext_format != HPPA_SCF_NONE))
    {
      struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;

      Debug (8, "resuming at ip=%x via sigreturn(%p)\n", c->dwarf.ip, sc);
      my_rt_sigreturn (sc, (sc->sc_flags & PARISC_SC_FLAG_IN_SYSCALL) != 0);
    }
  else
    {
      Debug (8, "resuming at ip=%x via setcontext()\n", c->dwarf.ip);
      setcontext (uc);
    }
#else
# warning Implement me!
#endif
  return -UNW_EINVAL;
}

#endif /* !UNW_REMOTE_ONLY */

/* This routine is responsible for copying the register values in
   cursor C and establishing them as the current machine state. */

static inline int
establish_machine_state (struct cursor *c)
{
  int (*access_reg) (unw_addr_space_t, unw_regnum_t, unw_word_t *,
                     int write, void *);
  int (*access_fpreg) (unw_addr_space_t, unw_regnum_t, unw_fpreg_t *,
                       int write, void *);
  unw_addr_space_t as = c->dwarf.as;
  void *arg = c->dwarf.as_arg;
  unw_fpreg_t fpval;
  unw_word_t val;
  int reg;

  access_reg = as->acc.access_reg;
  access_fpreg = as->acc.access_fpreg;

  Debug (8, "copying out cursor state\n");

  for (reg = 0; reg <= UNW_REG_LAST; ++reg)
    {
      Debug (16, "copying %s %d\n", unw_regname (reg), reg);
      if (unw_is_fpreg (reg))
        {
          if (tdep_access_fpreg (c, reg, &fpval, 0) >= 0)
            (*access_fpreg) (as, reg, &fpval, 1, arg);
        }
      else
        {
          if (tdep_access_reg (c, reg, &val, 0) >= 0)
            (*access_reg) (as, reg, &val, 1, arg);
        }
    }
  return 0;
}

PROTECTED int
unw_resume (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  Debug (1, "(cursor=%p)\n", c);

  if (!c->dwarf.ip)
    {
      /* This can happen easily when the frame-chain gets truncated
         due to bad or missing unwind-info.  */
      Debug (1, "refusing to resume execution at address 0\n");
      return -UNW_EINVAL;
    }

  if ((ret = establish_machine_state (c)) < 0)
    return ret;

  return (*c->dwarf.as->acc.resume) (c->dwarf.as, (unw_cursor_t *) c,
                                     c->dwarf.as_arg);
}
