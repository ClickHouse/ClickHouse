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
#include "ex_tables.h"

#include <signal.h>

#define arm_exidx_step  UNW_OBJ(arm_exidx_step)

static inline int
arm_exidx_step (struct cursor *c)
{
  unw_word_t old_ip, old_cfa;
  uint8_t buf[32];
  int ret;

  old_ip = c->dwarf.ip;
  old_cfa = c->dwarf.cfa;

  /* mark PC unsaved */
  c->dwarf.loc[UNW_ARM_R15] = DWARF_NULL_LOC;

  /* check dynamic info first --- it overrides everything else */
  ret = unwi_find_dynamic_proc_info (c->dwarf.as, c->dwarf.ip, &c->dwarf.pi, 1,
                                     c->dwarf.as_arg);
  if (ret == -UNW_ENOINFO)
    {
      if ((ret = tdep_find_proc_info (&c->dwarf, c->dwarf.ip, 1)) < 0)
        return ret;
    }

  if (c->dwarf.pi.format != UNW_INFO_FORMAT_ARM_EXIDX)
    return -UNW_ENOINFO;

  ret = arm_exidx_extract (&c->dwarf, buf);
  if (ret == -UNW_ESTOPUNWIND)
    return 0;
  else if (ret < 0)
    return ret;

  ret = arm_exidx_decode (buf, ret, &c->dwarf);
  if (ret < 0)
    return ret;

  if (c->dwarf.ip == old_ip && c->dwarf.cfa == old_cfa)
    {
      Dprintf ("%s: ip and cfa unchanged; stopping here (ip=0x%lx)\n",
               __FUNCTION__, (long) c->dwarf.ip);
      return -UNW_EBADFRAME;
    }

  c->dwarf.pi_valid = 0;

  return (c->dwarf.ip == 0) ? 0 : 1;
}

PROTECTED int
unw_step (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret = -UNW_EUNSPEC;

  Debug (1, "(cursor=%p)\n", c);

  /* Check if this is a signal frame. */
  if (unw_is_signal_frame (cursor) > 0)
     return unw_handle_signal_frame (cursor);

#ifdef CONFIG_DEBUG_FRAME
  /* First, try DWARF-based unwinding. */
  if (UNW_TRY_METHOD(UNW_ARM_METHOD_DWARF))
    {
      ret = dwarf_step (&c->dwarf);
      Debug(1, "dwarf_step()=%d\n", ret);

      if (likely (ret > 0))
        return 1;
      else if (unlikely (ret == -UNW_ESTOPUNWIND))
        return ret;

    if (ret < 0 && ret != -UNW_ENOINFO)
      {
        Debug (2, "returning %d\n", ret);
        return ret;
      }
    }
#endif /* CONFIG_DEBUG_FRAME */

  /* Next, try extbl-based unwinding. */
  if (UNW_TRY_METHOD (UNW_ARM_METHOD_EXIDX))
    {
      ret = arm_exidx_step (c);
      if (ret > 0)
        return 1;
      if (ret == -UNW_ESTOPUNWIND || ret == 0)
        return ret;
    }

  /* Fall back on APCS frame parsing.
     Note: This won't work in case the ARM EABI is used. */
#ifdef __FreeBSD__
  if (0)
#else
  if (unlikely (ret < 0))
#endif
    {
      if (UNW_TRY_METHOD(UNW_ARM_METHOD_FRAME))
        {
          Debug (13, "dwarf_step() failed (ret=%d), trying frame-chain\n", ret);
          ret = UNW_ESUCCESS;
          /* DWARF unwinding failed, try to follow APCS/optimized APCS frame chain */
          unw_word_t instr, i;
          dwarf_loc_t ip_loc, fp_loc;
          unw_word_t frame;
          /* Mark all registers unsaved, since we don't know where
             they are saved (if at all), except for the EBP and
             EIP.  */
          if (dwarf_get(&c->dwarf, c->dwarf.loc[UNW_ARM_R11], &frame) < 0)
            {
              return 0;
            }
          for (i = 0; i < DWARF_NUM_PRESERVED_REGS; ++i) {
            c->dwarf.loc[i] = DWARF_NULL_LOC;
          }
          if (frame)
            {
              if (dwarf_get(&c->dwarf, DWARF_LOC(frame, 0), &instr) < 0)
                {
                  return 0;
                }
              instr -= 8;
              if (dwarf_get(&c->dwarf, DWARF_LOC(instr, 0), &instr) < 0)
                {
                  return 0;
                }
              if ((instr & 0xFFFFD800) == 0xE92DD800)
                {
                  /* Standard APCS frame. */
                  ip_loc = DWARF_LOC(frame - 4, 0);
                  fp_loc = DWARF_LOC(frame - 12, 0);
                }
              else
                {
                  /* Codesourcery optimized normal frame. */
                  ip_loc = DWARF_LOC(frame, 0);
                  fp_loc = DWARF_LOC(frame - 4, 0);
                }
              if (dwarf_get(&c->dwarf, ip_loc, &c->dwarf.ip) < 0)
                {
                  return 0;
                }
              c->dwarf.loc[UNW_ARM_R12] = ip_loc;
              c->dwarf.loc[UNW_ARM_R11] = fp_loc;
              c->dwarf.pi_valid = 0;
              Debug(15, "ip=%x\n", c->dwarf.ip);
            }
          else
            {
              ret = -UNW_ENOINFO;
            }
        }
    }
  return ret == -UNW_ENOINFO ? 0 : ret;
}
