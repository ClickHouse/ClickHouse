/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2005 Hewlett-Packard Co
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

#if UNW_TARGET_IA64

#include "libunwind_i.h"
#include "tdep-ia64/rse.h"

static inline int
bsp_match (unw_cursor_t *c, unw_word_t *wp)
{
  unw_word_t bsp, pfs, sol;

  if (unw_get_reg (c, UNW_IA64_BSP, &bsp) < 0
      || unw_get_reg (c, UNW_IA64_AR_PFS, &pfs) < 0)
    abort ();

  /* simulate the effect of "br.call sigsetjmp" on ar.bsp: */
  sol = (pfs >> 7) & 0x7f;
  bsp = rse_skip_regs (bsp, sol);

  if (bsp != wp[JB_BSP])
    return 0;

  if (unlikely (sol == 0))
    {
      unw_word_t sp, prev_sp;
      unw_cursor_t tmp = *c;

      /* The caller of {sig,}setjmp() cannot have a NULL-frame.  If we
         see a NULL-frame, we haven't reached the right target yet.
         To have a NULL-frame, the number of locals must be zero and
         the stack-frame must also be empty.  */

      if (unw_step (&tmp) < 0)
        abort ();

      if (unw_get_reg (&tmp, UNW_REG_SP, &sp) < 0
          || unw_get_reg (&tmp, UNW_REG_SP, &prev_sp) < 0)
        abort ();

      if (sp == prev_sp)
        /* got a NULL-frame; keep looking... */
        return 0;
    }
  return 1;
}

/* On ia64 we cannot always call sigprocmask() at
   _UI_siglongjmp_cont() because the signal may have switched stacks
   and the old stack's register-backing store may have overflown,
   leaving us no space to allocate the stacked registers needed to
   call sigprocmask().  Fortunately, we can just let unw_resume() (via
   sigreturn) take care of restoring the signal-mask.  That's faster
   anyhow.  */
static inline int
resume_restores_sigmask (unw_cursor_t *c, unw_word_t *wp)
{
  unw_word_t sc_addr = ((struct cursor *) c)->sigcontext_addr;
  struct sigcontext *sc = (struct sigcontext *) sc_addr;
  sigset_t current_mask;
  void *mp;

  if (!sc_addr)
    return 0;

  /* let unw_resume() install the desired signal mask */

  if (wp[JB_MASK_SAVED])
    mp = &wp[JB_MASK];
  else
    {
      if (sigprocmask (SIG_BLOCK, NULL, &current_mask) < 0)
        abort ();
      mp = &current_mask;
    }
  memcpy (&sc->sc_mask, mp, sizeof (sc->sc_mask));
  return 1;
}

#else /* !UNW_TARGET_IA64 */

static inline int
bsp_match (unw_cursor_t *c, unw_word_t *wp)
{
  return 1;
}

static inline int
resume_restores_sigmask (unw_cursor_t *c, unw_word_t *wp)
{
  /* We may want to do this analogously as for ia64... */
  return 0;
}

#endif /* !UNW_TARGET_IA64 */
