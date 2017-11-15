/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
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

#define UNW_LOCAL_ONLY

#undef _FORTIFY_SOURCE
#include <assert.h>
#include <libunwind.h>
#include <setjmp.h>
#include <signal.h>
#include <stdlib.h>

#include "jmpbuf.h"
#include "setjmp_i.h"

#if defined(__GLIBC__)
#if __GLIBC_PREREQ(2, 4)

/* Starting with glibc-2.4, {sig,}setjmp in GLIBC obfuscates the
   register values in jmp_buf by XORing them with a "random"
   canary value.

   This makes it impossible to implement longjmp, as we
   can never match wp[JB_SP], unless we decode the canary first.

   Doing so is possible, but doesn't appear to be worth the trouble,
   so we simply defer to glibc longjmp here.  */
#define _longjmp __nonworking__longjmp
#define longjmp __nonworking_longjmp
static void _longjmp (jmp_buf env, int val);
static void longjmp (jmp_buf env, int val);
#endif
#endif /* __GLIBC__ */

void
_longjmp (jmp_buf env, int val)
{
  extern int _UI_longjmp_cont;
  unw_context_t uc;
  unw_cursor_t c;
  unw_word_t sp;
  unw_word_t *wp = (unw_word_t *) env;

  if (unw_getcontext (&uc) < 0 || unw_init_local (&c, &uc) < 0)
    abort ();

  do
    {
      if (unw_get_reg (&c, UNW_REG_SP, &sp) < 0)
        abort ();
#ifdef __FreeBSD__
      if (sp != wp[JB_SP] + sizeof(unw_word_t))
#else
      if (sp != wp[JB_SP])
#endif
        continue;

      if (!bsp_match (&c, wp))
        continue;

      /* found the right frame: */

      assert (UNW_NUM_EH_REGS >= 2);

      if (unw_set_reg (&c, UNW_REG_EH + 0, wp[JB_RP]) < 0
          || unw_set_reg (&c, UNW_REG_EH + 1, val) < 0
          || unw_set_reg (&c, UNW_REG_IP,
                          (unw_word_t) (uintptr_t) &_UI_longjmp_cont))
        abort ();

      unw_resume (&c);

      abort ();
    }
  while (unw_step (&c) > 0);

  abort ();
}

#ifdef __GNUC__
#define STRINGIFY1(x) #x
#define STRINGIFY(x) STRINGIFY1(x)
void longjmp (jmp_buf env, int val) 
  __attribute__ ((alias (STRINGIFY(_longjmp))));
#else

void
longjmp (jmp_buf env, int val)
{
  _longjmp (env, val);
}

#endif /* __GNUC__ */
