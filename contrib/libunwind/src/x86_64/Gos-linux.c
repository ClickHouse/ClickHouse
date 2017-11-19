/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2003 Hewlett-Packard Co
        Contributed by David Mosberger-Tang <davidm@hpl.hp.com>

   Modified for x86_64 by Max Asbock <masbock@us.ibm.com>

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

#include <sys/syscall.h>

HIDDEN void
tdep_fetch_frame (struct dwarf_cursor *dw, unw_word_t ip, int need_unwind_info)
{
  struct cursor *c = (struct cursor *) dw;
  assert(! need_unwind_info || dw->pi_valid);
  assert(! need_unwind_info || dw->pi.unwind_info);
  if (dw->pi_valid
      && dw->pi.unwind_info
      && ((struct dwarf_cie_info *) dw->pi.unwind_info)->signal_frame)
    c->sigcontext_format = X86_64_SCF_LINUX_RT_SIGFRAME;
  else
    c->sigcontext_format = X86_64_SCF_NONE;

  Debug(5, "fetch frame ip=0x%lx cfa=0x%lx format=%d\n",
        dw->ip, dw->cfa, c->sigcontext_format);
}

HIDDEN int
tdep_cache_frame (struct dwarf_cursor *dw)
{
  struct cursor *c = (struct cursor *) dw;

  Debug(5, "cache frame ip=0x%lx cfa=0x%lx format=%d\n",
        dw->ip, dw->cfa, c->sigcontext_format);
  return c->sigcontext_format;
}

HIDDEN void
tdep_reuse_frame (struct dwarf_cursor *dw, int frame)
{
  struct cursor *c = (struct cursor *) dw;
  c->sigcontext_format = frame;
  if (c->sigcontext_format == X86_64_SCF_LINUX_RT_SIGFRAME)
  {
    c->frame_info.frame_type = UNW_X86_64_FRAME_SIGRETURN;
    /* Offset from cfa to ucontext_t in signal frame.  */
    c->frame_info.cfa_reg_offset = 0;
    c->sigcontext_addr = dw->cfa;
  }

  Debug(5, "reuse frame ip=0x%lx cfa=0x%lx format=%d addr=0x%lx offset=%+d\n",
        dw->ip, dw->cfa, c->sigcontext_format, c->sigcontext_addr,
        (c->sigcontext_format == X86_64_SCF_LINUX_RT_SIGFRAME
         ? c->frame_info.cfa_reg_offset : 0));
}

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  return c->sigcontext_format != X86_64_SCF_NONE;
}

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
#if UNW_DEBUG /* To silence compiler warnings */
  /* Should not get here because we now use kernel-provided dwarf
     information for the signal trampoline and dwarf_step() works.
     Hence unw_step() should never call this function. Maybe
     restore old non-dwarf signal handling here, but then the
     gating on unw_is_signal_frame() needs to be removed. */
  struct cursor *c = (struct cursor *) cursor;
  Debug(1, "old format signal frame? format=%d addr=0x%lx cfa=0x%lx\n",
        c->sigcontext_format, c->sigcontext_addr, c->dwarf.cfa);
#endif
  return -UNW_EBADFRAME;
}

#ifndef UNW_REMOTE_ONLY
HIDDEN void *
x86_64_r_uc_addr (ucontext_t *uc, int reg)
{
  /* NOTE: common_init() in init.h inlines these for fast path access. */
  void *addr;

  switch (reg)
    {
    case UNW_X86_64_R8: addr = &uc->uc_mcontext.gregs[REG_R8]; break;
    case UNW_X86_64_R9: addr = &uc->uc_mcontext.gregs[REG_R9]; break;
    case UNW_X86_64_R10: addr = &uc->uc_mcontext.gregs[REG_R10]; break;
    case UNW_X86_64_R11: addr = &uc->uc_mcontext.gregs[REG_R11]; break;
    case UNW_X86_64_R12: addr = &uc->uc_mcontext.gregs[REG_R12]; break;
    case UNW_X86_64_R13: addr = &uc->uc_mcontext.gregs[REG_R13]; break;
    case UNW_X86_64_R14: addr = &uc->uc_mcontext.gregs[REG_R14]; break;
    case UNW_X86_64_R15: addr = &uc->uc_mcontext.gregs[REG_R15]; break;
    case UNW_X86_64_RDI: addr = &uc->uc_mcontext.gregs[REG_RDI]; break;
    case UNW_X86_64_RSI: addr = &uc->uc_mcontext.gregs[REG_RSI]; break;
    case UNW_X86_64_RBP: addr = &uc->uc_mcontext.gregs[REG_RBP]; break;
    case UNW_X86_64_RBX: addr = &uc->uc_mcontext.gregs[REG_RBX]; break;
    case UNW_X86_64_RDX: addr = &uc->uc_mcontext.gregs[REG_RDX]; break;
    case UNW_X86_64_RAX: addr = &uc->uc_mcontext.gregs[REG_RAX]; break;
    case UNW_X86_64_RCX: addr = &uc->uc_mcontext.gregs[REG_RCX]; break;
    case UNW_X86_64_RSP: addr = &uc->uc_mcontext.gregs[REG_RSP]; break;
    case UNW_X86_64_RIP: addr = &uc->uc_mcontext.gregs[REG_RIP]; break;

    default:
      addr = NULL;
    }
  return addr;
}

/* sigreturn() is a no-op on x86_64 glibc.  */
HIDDEN NORETURN void
x86_64_sigreturn (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;
  mcontext_t *sc_mcontext = &((ucontext_t*)sc)->uc_mcontext;
  /* Copy in saved uc - all preserved regs are at the start of sigcontext */
  memcpy(sc_mcontext, &c->uc->uc_mcontext,
         DWARF_NUM_PRESERVED_REGS * sizeof(unw_word_t));

  Debug (8, "resuming at ip=%llx via sigreturn(%p)\n",
             (unsigned long long) c->dwarf.ip, sc);
  __asm__ __volatile__ ("mov %0, %%rsp;"
                        "mov %1, %%rax;"
                        "syscall"
                        :: "r"(sc), "i"(SYS_rt_sigreturn)
                        : "memory");
  abort();
}

#endif
