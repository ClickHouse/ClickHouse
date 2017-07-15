/* libunwind - a platform-independent unwind library
   Copyright (C) 2003-2005 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>
   Copyright (C) 2010 Konstantin Belousov <kib@freebsd.org>

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

#include "_UPT_internal.h"

#if UNW_TARGET_IA64
# include <elf.h>
# ifdef HAVE_ASM_PTRACE_OFFSETS_H
#   include <asm/ptrace_offsets.h>
# endif
# include "tdep-ia64/rse.h"
#endif

#if HAVE_DECL_PTRACE_POKEUSER || HAVE_TTRACE
int
_UPT_access_reg (unw_addr_space_t as, unw_regnum_t reg, unw_word_t *val,
                 int write, void *arg)
{
  struct UPT_info *ui = arg;
  pid_t pid = ui->pid;

#if UNW_DEBUG
  Debug(16, "using pokeuser: reg: %s [%u], val: %lx, write: %d\n", unw_regname(reg), (unsigned) reg, (long) val, write);

  if (write)
    Debug (16, "%s <- %lx\n", unw_regname (reg), (long) *val);
#endif

#if UNW_TARGET_IA64
  if ((unsigned) reg - UNW_IA64_NAT < 32)
    {
      unsigned long nat_bits, mask;

      /* The Linux ptrace represents the statc NaT bits as a single word.  */
      mask = ((unw_word_t) 1) << (reg - UNW_IA64_NAT);
      errno = 0;
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
      nat_bits = ptrace (PTRACE_PEEKUSER, pid, PT_NAT_BITS, 0);
      if (errno)
        goto badreg;
#endif

      if (write)
        {
          if (*val)
            nat_bits |= mask;
          else
            nat_bits &= ~mask;
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
          errno = 0;
          ptrace (PTRACE_POKEUSER, pid, PT_NAT_BITS, nat_bits);
          if (errno)
            goto badreg;
#endif
        }
      goto out;
    }
  else
    switch (reg)
      {
      case UNW_IA64_GR + 0:
        if (write)
          goto badreg;
        *val = 0;
        return 0;

      case UNW_REG_IP:
        {
          unsigned long ip, psr;

          /* distribute bundle-addr. & slot-number across PT_IIP & PT_IPSR.  */
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
          errno = 0;
          psr = ptrace (PTRACE_PEEKUSER, pid, PT_CR_IPSR, 0);
          if (errno)
            goto badreg;
#endif
          if (write)
            {
              ip = *val & ~0xfUL;
              psr = (psr & ~0x3UL << 41) | (*val & 0x3);
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
              errno = 0;
              ptrace (PTRACE_POKEUSER, pid, PT_CR_IIP, ip);
              ptrace (PTRACE_POKEUSER, pid, PT_CR_IPSR, psr);
              if (errno)
                goto badreg;
#endif
            }
          else
            {
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
              errno = 0;
              ip = ptrace (PTRACE_PEEKUSER, pid, PT_CR_IIP, 0);
              if (errno)
                goto badreg;
#endif
              *val = ip + ((psr >> 41) & 0x3);
            }
          goto out;
        }

      case UNW_IA64_AR_BSPSTORE:
        reg = UNW_IA64_AR_BSP;
        break;

      case UNW_IA64_AR_BSP:
      case UNW_IA64_BSP:
        {
          unsigned long sof, cfm, bsp;

#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
          /* Account for the fact that ptrace() expects bsp to point
             _after_ the current register frame.  */
          errno = 0;
          cfm = ptrace (PTRACE_PEEKUSER, pid, PT_CFM, 0);
          if (errno)
            goto badreg;
#endif
          sof = (cfm & 0x7f);

          if (write)
            {
              bsp = rse_skip_regs (*val, sof);
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
              errno = 0;
              ptrace (PTRACE_POKEUSER, pid, PT_AR_BSP, bsp);
              if (errno)
                goto badreg;
#endif
            }
          else
            {
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
              errno = 0;
              bsp = ptrace (PTRACE_PEEKUSER, pid, PT_AR_BSP, 0);
              if (errno)
                goto badreg;
#endif
              *val = rse_skip_regs (bsp, -sof);
            }
          goto out;
        }

      case UNW_IA64_CFM:
        /* If we change CFM, we need to adjust ptrace's notion of bsp
           accordingly, so that the real bsp remains unchanged.  */
        if (write)
          {
            unsigned long new_sof, old_sof, cfm, bsp;

#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
            errno = 0;
            bsp = ptrace (PTRACE_PEEKUSER, pid, PT_AR_BSP, 0);
            cfm = ptrace (PTRACE_PEEKUSER, pid, PT_CFM, 0);
#endif
            if (errno)
              goto badreg;
            old_sof = (cfm & 0x7f);
            new_sof = (*val & 0x7f);
            if (old_sof != new_sof)
              {
                bsp = rse_skip_regs (bsp, -old_sof + new_sof);
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
                errno = 0;
                ptrace (PTRACE_POKEUSER, pid, PT_AR_BSP, 0);
                if (errno)
                  goto badreg;
#endif
              }
#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
            errno = 0;
            ptrace (PTRACE_POKEUSER, pid, PT_CFM, *val);
            if (errno)
              goto badreg;
#endif
            goto out;
          }
        break;
      }
#endif /* End of IA64 */

  if ((unsigned) reg >= ARRAY_SIZE (_UPT_reg_offset))
    {
#if UNW_DEBUG
      Debug(2, "register out of range: >= %zu / %zu\n", sizeof(_UPT_reg_offset), sizeof(_UPT_reg_offset[0]));
#endif
      errno = EINVAL;
      goto badreg;
    }

#ifdef HAVE_TTRACE
#       warning No support for ttrace() yet.
#else
  errno = 0;
  if (write)
    ptrace (PTRACE_POKEUSER, pid, _UPT_reg_offset[reg], *val);
  else {
#if UNW_DEBUG
    Debug(16, "ptrace PEEKUSER pid: %lu , reg: %lu , offs: %lu\n", (unsigned long)pid, (unsigned long)reg,
        (unsigned long)_UPT_reg_offset[reg]);
#endif
    *val = ptrace (PTRACE_PEEKUSER, pid, _UPT_reg_offset[reg], 0);
  }
  if (errno) {
#if UNW_DEBUG
    Debug(2, "ptrace failure\n");
#endif
    goto badreg;
  }
#endif

#ifdef UNW_TARGET_IA64
 out:
#endif
#if UNW_DEBUG
  if (!write)
    Debug (16, "%s[%u] -> %lx\n", unw_regname (reg), (unsigned) reg, (long) *val);
#endif
  return 0;

 badreg:
  Debug (1, "bad register %s [%u] (error: %s)\n", unw_regname(reg), reg, strerror (errno));
  return -UNW_EBADREG;
}
#elif HAVE_DECL_PT_GETREGS
int
_UPT_access_reg (unw_addr_space_t as, unw_regnum_t reg, unw_word_t *val,
                 int write, void *arg)
{
  struct UPT_info *ui = arg;
  pid_t pid = ui->pid;
  gregset_t regs;
  char *r;

#if UNW_DEBUG
  Debug(16, "using getregs: reg: %s [%u], val: %lx, write: %u\n", unw_regname(reg), (unsigned) reg, (long) val, write);

  if (write)
    Debug (16, "%s [%u] <- %lx\n", unw_regname (reg), (unsigned) reg, (long) *val);
#endif
  if ((unsigned) reg >= ARRAY_SIZE (_UPT_reg_offset))
    {
      errno = EINVAL;
      goto badreg;
    }
  r = (char *)&regs + _UPT_reg_offset[reg];
  if (ptrace(PT_GETREGS, pid, (caddr_t)&regs, 0) == -1)
    goto badreg;
  if (write) {
      memcpy(r, val, sizeof(unw_word_t));
      if (ptrace(PT_SETREGS, pid, (caddr_t)&regs, 0) == -1)
        goto badreg;
  } else
      memcpy(val, r, sizeof(unw_word_t));
  return 0;

 badreg:
  Debug (1, "bad register %s [%u] (error: %s)\n", unw_regname(reg), reg, strerror (errno));
  return -UNW_EBADREG;
}
#else
#error Port me
#endif
