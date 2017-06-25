/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2004 Hewlett-Packard Co
        Contributed by David Mosberger

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

PROTECTED int
unw_step (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret, i;

  Debug (1, "(cursor=%p, ip=0x%08x)\n", c, (unsigned) c->dwarf.ip);

  /* Try DWARF-based unwinding... */
  ret = dwarf_step (&c->dwarf);

  if (ret < 0 && ret != -UNW_ENOINFO)
    {
      Debug (2, "returning %d\n", ret);
      return ret;
    }

  if (unlikely (ret < 0))
    {
      /* DWARF failed, let's see if we can follow the frame-chain
         or skip over the signal trampoline.  */

      Debug (13, "dwarf_step() failed (ret=%d), trying fallback\n", ret);

      if (unw_is_signal_frame (cursor))
        {
#ifdef __linux__
          /* Assume that the trampoline is at the beginning of the
             sigframe.  */
          unw_word_t ip, sc_addr = c->dwarf.ip + LINUX_RT_SIGFRAME_UC_OFF;
          dwarf_loc_t iaoq_loc = DWARF_LOC (sc_addr + LINUX_SC_IAOQ_OFF, 0);

          c->sigcontext_format = HPPA_SCF_LINUX_RT_SIGFRAME;
          c->sigcontext_addr = sc_addr;

          if ((ret = dwarf_get (&c->dwarf, iaoq_loc, &ip)) < 0)
            {
              Debug (2, "failed to read IAOQ[1] (ret=%d)\n", ret);
              return ret;
            }
          c->dwarf.ip = ip & ~0x3;      /* mask out the privilege level */

          for (i = 0; i < 32; ++i)
            {
              c->dwarf.loc[UNW_HPPA_GR + i]
                = DWARF_LOC (sc_addr + LINUX_SC_GR_OFF + 4*i, 0);
              c->dwarf.loc[UNW_HPPA_FR + i]
                = DWARF_LOC (sc_addr + LINUX_SC_FR_OFF + 4*i, 0);
            }

          if ((ret = dwarf_get (&c->dwarf, c->dwarf.loc[UNW_HPPA_SP],
                                &c->dwarf.cfa)) < 0)
            {
              Debug (2, "failed to read SP (ret=%d)\n", ret);
              return ret;
            }
#else
# error Implement me!
#endif
        }
      else
        c->dwarf.ip = 0;
    }
  ret = (c->dwarf.ip == 0) ? 0 : 1;
  Debug (2, "returning %d\n", ret);
  return ret;
}
