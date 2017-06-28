/* libunwind - a platform-independent unwind library
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/ucontext.h>
#include <machine/sigframe.h>
#include <signal.h>
#include <stddef.h>
#include "unwind_i.h"
#include "ucontext_i.h"

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  /* XXXKIB */
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, w2, b0, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  /* Check if RIP points at sigreturn sequence.
48 8d 7c 24 10          lea     SIGF_UC(%rsp),%rdi
6a 00                   pushq   $0
48 c7 c0 a1 01 00 00    movq    $SYS_sigreturn,%rax
0f 05                   syscall
f4              0:      hlt
eb fd                   jmp     0b
  */

  ip = c->dwarf.ip;
  c->sigcontext_format = X86_64_SCF_NONE;
  if ((ret = (*a->access_mem) (as, ip, &w0, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 8, &w1, 0, arg)) < 0
      || (ret = (*a->access_mem) (as, ip + 16, &w2, 0, arg)) < 0)
    return 0;
  w2 &= 0xffffff;
  if (w0 == 0x48006a10247c8d48 &&
      w1 == 0x050f000001a1c0c7 &&
      w2 == 0x0000000000fdebf4)
   {
     c->sigcontext_format = X86_64_SCF_FREEBSD_SIGFRAME;
     return (c->sigcontext_format);
   }
  /* Check if RIP points at standard syscall sequence.
49 89 ca        mov    %rcx,%r10
0f 05           syscall
  */
  if ((ret = (*a->access_mem) (as, ip - 5, &b0, 0, arg)) < 0)
    return (0);
  Debug (12, "b0 0x%lx\n", b0);
  if ((b0 & 0xffffffffffffff) == 0x050fca89490000 ||
      (b0 & 0xffffffffff) == 0x050fca8949)
   {
    c->sigcontext_format = X86_64_SCF_FREEBSD_SYSCALL;
    return (c->sigcontext_format);
   }
  return (X86_64_SCF_NONE);
}

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t ucontext;
  int ret;

  if (c->sigcontext_format == X86_64_SCF_FREEBSD_SIGFRAME)
   {
    ucontext = c->dwarf.cfa + offsetof(struct sigframe, sf_uc);
    c->sigcontext_addr = c->dwarf.cfa;
    Debug(1, "signal frame, skip over trampoline\n");

    struct dwarf_loc rsp_loc = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RSP, 0);
    ret = dwarf_get (&c->dwarf, rsp_loc, &c->dwarf.cfa);
    if (ret < 0)
     {
       Debug (2, "returning %d\n", ret);
       return ret;
     }

    c->dwarf.loc[RAX] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RAX, 0);
    c->dwarf.loc[RDX] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RDX, 0);
    c->dwarf.loc[RCX] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RCX, 0);
    c->dwarf.loc[RBX] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RBX, 0);
    c->dwarf.loc[RSI] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RSI, 0);
    c->dwarf.loc[RDI] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RDI, 0);
    c->dwarf.loc[RBP] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RBP, 0);
    c->dwarf.loc[RSP] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RSP, 0);
    c->dwarf.loc[ R8] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R8, 0);
    c->dwarf.loc[ R9] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R9, 0);
    c->dwarf.loc[R10] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R10, 0);
    c->dwarf.loc[R11] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R11, 0);
    c->dwarf.loc[R12] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R12, 0);
    c->dwarf.loc[R13] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R13, 0);
    c->dwarf.loc[R14] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R14, 0);
    c->dwarf.loc[R15] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_R15, 0);
    c->dwarf.loc[RIP] = DWARF_LOC (ucontext + UC_MCONTEXT_GREGS_RIP, 0);

    return 0;
   }
  else if (c->sigcontext_format == X86_64_SCF_FREEBSD_SYSCALL)
   {
    c->dwarf.loc[RCX] = c->dwarf.loc[R10];
    /*  rsp_loc = DWARF_LOC(c->dwarf.cfa - 8, 0);       */
    /*  rbp_loc = c->dwarf.loc[RBP];                    */
    c->dwarf.loc[RIP] = DWARF_LOC (c->dwarf.cfa, 0);
    ret = dwarf_get (&c->dwarf, c->dwarf.loc[RIP], &c->dwarf.ip);
    Debug (1, "Frame Chain [RIP=0x%Lx] = 0x%Lx\n",
           (unsigned long long) DWARF_GET_LOC (c->dwarf.loc[RIP]),
           (unsigned long long) c->dwarf.ip);
    if (ret < 0)
     {
       Debug (2, "returning %d\n", ret);
       return ret;
     }
    c->dwarf.cfa += 8;
    c->dwarf.use_prev_instr = 1;
    return 1;
   }
  else
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
    case UNW_X86_64_R8: addr = &uc->uc_mcontext.mc_r8; break;
    case UNW_X86_64_R9: addr = &uc->uc_mcontext.mc_r9; break;
    case UNW_X86_64_R10: addr = &uc->uc_mcontext.mc_r10; break;
    case UNW_X86_64_R11: addr = &uc->uc_mcontext.mc_r11; break;
    case UNW_X86_64_R12: addr = &uc->uc_mcontext.mc_r12; break;
    case UNW_X86_64_R13: addr = &uc->uc_mcontext.mc_r13; break;
    case UNW_X86_64_R14: addr = &uc->uc_mcontext.mc_r14; break;
    case UNW_X86_64_R15: addr = &uc->uc_mcontext.mc_r15; break;
    case UNW_X86_64_RDI: addr = &uc->uc_mcontext.mc_rdi; break;
    case UNW_X86_64_RSI: addr = &uc->uc_mcontext.mc_rsi; break;
    case UNW_X86_64_RBP: addr = &uc->uc_mcontext.mc_rbp; break;
    case UNW_X86_64_RBX: addr = &uc->uc_mcontext.mc_rbx; break;
    case UNW_X86_64_RDX: addr = &uc->uc_mcontext.mc_rdx; break;
    case UNW_X86_64_RAX: addr = &uc->uc_mcontext.mc_rax; break;
    case UNW_X86_64_RCX: addr = &uc->uc_mcontext.mc_rcx; break;
    case UNW_X86_64_RSP: addr = &uc->uc_mcontext.mc_rsp; break;
    case UNW_X86_64_RIP: addr = &uc->uc_mcontext.mc_rip; break;

    default:
      addr = NULL;
    }
  return addr;
}

HIDDEN NORETURN void
x86_64_sigreturn (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  ucontext_t *uc = (ucontext_t *)(c->sigcontext_addr +
    offsetof(struct sigframe, sf_uc));

  uc->uc_mcontext.mc_r8 = c->uc->uc_mcontext.mc_r8;
  uc->uc_mcontext.mc_r9 = c->uc->uc_mcontext.mc_r9;
  uc->uc_mcontext.mc_r10 = c->uc->uc_mcontext.mc_r10;
  uc->uc_mcontext.mc_r11 = c->uc->uc_mcontext.mc_r11;
  uc->uc_mcontext.mc_r12 = c->uc->uc_mcontext.mc_r12;
  uc->uc_mcontext.mc_r13 = c->uc->uc_mcontext.mc_r13;
  uc->uc_mcontext.mc_r14 = c->uc->uc_mcontext.mc_r14;
  uc->uc_mcontext.mc_r15 = c->uc->uc_mcontext.mc_r15;
  uc->uc_mcontext.mc_rdi = c->uc->uc_mcontext.mc_rdi;
  uc->uc_mcontext.mc_rsi = c->uc->uc_mcontext.mc_rsi;
  uc->uc_mcontext.mc_rbp = c->uc->uc_mcontext.mc_rbp;
  uc->uc_mcontext.mc_rbx = c->uc->uc_mcontext.mc_rbx;
  uc->uc_mcontext.mc_rdx = c->uc->uc_mcontext.mc_rdx;
  uc->uc_mcontext.mc_rax = c->uc->uc_mcontext.mc_rax;
  uc->uc_mcontext.mc_rcx = c->uc->uc_mcontext.mc_rcx;
  uc->uc_mcontext.mc_rsp = c->uc->uc_mcontext.mc_rsp;
  uc->uc_mcontext.mc_rip = c->uc->uc_mcontext.mc_rip;

  Debug (8, "resuming at ip=%llx via sigreturn(%p)\n",
             (unsigned long long) c->dwarf.ip, uc);
  sigreturn(uc);
  abort();
}
#endif
