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

#include <sys/types.h>
#include <signal.h>
#include <stddef.h>
#include <ucontext.h>
#include <machine/sigframe.h>

#include "unwind_i.h"
#include "offsets.h"

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, w2, w3, w4, w5, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  /* Check if EIP points at sigreturn() sequence.  It can be:
sigcode+4: from amd64 freebsd32 environment
8d 44 24 20             lea    0x20(%esp),%eax
50                      push   %eax
b8 a1 01 00 00          mov    $0x1a1,%eax
50                      push   %eax
cd 80                   int    $0x80

sigcode+4: from real i386
8d 44 24 20             lea    0x20(%esp),%eax
50                      push   %eax
f7 40 54 00 02 00       testl  $0x20000,0x54(%eax)
75 03                   jne    sigcode+21
8e 68 14                mov    0x14(%eax),%gs
b8 a1 01 00 00          mov    $0x1a1,%eax
50                      push   %eax
cd 80                   int    $0x80

freebsd4_sigcode+4:
XXX
osigcode:
XXX
  */
  ip = c->dwarf.ip;
  ret = X86_SCF_NONE;
  c->sigcontext_format = ret;
  if ((*a->access_mem) (as, ip, &w0, 0, arg) < 0 ||
      (*a->access_mem) (as, ip + 4, &w1, 0, arg) < 0 ||
      (*a->access_mem) (as, ip + 8, &w2, 0, arg) < 0 ||
      (*a->access_mem) (as, ip + 12, &w3, 0, arg) < 0)
    return ret;
  if (w0 == 0x2024448d && w1 == 0x01a1b850 && w2 == 0xcd500000 &&
      (w3 & 0xff) == 0x80)
    ret = X86_SCF_FREEBSD_SIGFRAME;
  else {
    if ((*a->access_mem) (as, ip + 16, &w4, 0, arg) < 0 ||
        (*a->access_mem) (as, ip + 20, &w5, 0, arg) < 0)
      return ret;
    if (w0 == 0x2024448d && w1 == 0x5440f750 && w2 == 0x75000200 &&
        w3 == 0x14688e03 && w4 == 0x0001a1b8 && w5 == 0x80cd5000)
      ret = X86_SCF_FREEBSD_SIGFRAME;
  }

  /* Check for syscall */
  if (ret == X86_SCF_NONE && (*a->access_mem) (as, ip - 2, &w0, 0, arg) >= 0 &&
      (w0 & 0xffff) == 0x80cd)
    ret = X86_SCF_FREEBSD_SYSCALL;
  Debug (16, "returning %d\n", ret);
  c->sigcontext_format = ret;
  return (ret);
}

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  if (c->sigcontext_format == X86_SCF_FREEBSD_SIGFRAME) {
    struct sigframe *sf;
    uintptr_t uc_addr;
    struct dwarf_loc esp_loc;

    sf = (struct sigframe *)c->dwarf.cfa;
    uc_addr = (uintptr_t)&(sf->sf_uc);
    c->sigcontext_addr = c->dwarf.cfa;

    esp_loc = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_ESP_OFF, 0);
    ret = dwarf_get (&c->dwarf, esp_loc, &c->dwarf.cfa);
    if (ret < 0)
    {
            Debug (2, "returning 0\n");
            return 0;
    }

    c->dwarf.loc[EIP] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EIP_OFF, 0);
    c->dwarf.loc[ESP] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_ESP_OFF, 0);
    c->dwarf.loc[EAX] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EAX_OFF, 0);
    c->dwarf.loc[ECX] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_ECX_OFF, 0);
    c->dwarf.loc[EDX] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EDX_OFF, 0);
    c->dwarf.loc[EBX] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EBX_OFF, 0);
    c->dwarf.loc[EBP] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EBP_OFF, 0);
    c->dwarf.loc[ESI] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_ESI_OFF, 0);
    c->dwarf.loc[EDI] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EDI_OFF, 0);
    c->dwarf.loc[EFLAGS] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_EFLAGS_OFF, 0);
    c->dwarf.loc[TRAPNO] = DWARF_LOC (uc_addr + FREEBSD_UC_MCONTEXT_TRAPNO_OFF, 0);
    c->dwarf.loc[ST0] = DWARF_NULL_LOC;
  } else if (c->sigcontext_format == X86_SCF_FREEBSD_SYSCALL) {
    c->dwarf.loc[EIP] = DWARF_LOC (c->dwarf.cfa, 0);
    c->dwarf.loc[EAX] = DWARF_NULL_LOC;
    c->dwarf.cfa += 4;
    c->dwarf.use_prev_instr = 1;
  } else {
    Debug (8, "Gstep: not handling frame format %d\n", c->sigcontext_format);
    abort();
  }
  return 0;
}

HIDDEN dwarf_loc_t
x86_get_scratch_loc (struct cursor *c, unw_regnum_t reg)
{
  unw_word_t addr = c->sigcontext_addr, off, xmm_off;
  unw_word_t fpstate, fpformat;
  int ret, is_fpstate = 0, is_xmmstate = 0;

  switch (c->sigcontext_format)
    {
    case X86_SCF_NONE:
      return DWARF_REG_LOC (&c->dwarf, reg);

    case X86_SCF_FREEBSD_SIGFRAME:
      addr += offsetof(struct sigframe, sf_uc) + FREEBSD_UC_MCONTEXT_OFF;
      break;

    case X86_SCF_FREEBSD_SIGFRAME4:
      abort();
      break;

    case X86_SCF_FREEBSD_OSIGFRAME:
      /* XXXKIB */
      abort();
      break;

    case X86_SCF_FREEBSD_SYSCALL:
      /* XXXKIB */
      abort();
      break;

    default:
      /* XXXKIB */
      abort();
      break;
    }

  off = 0; /* shut gcc warning */
  switch (reg)
    {
    case UNW_X86_GS: off = FREEBSD_UC_MCONTEXT_GS_OFF; break;
    case UNW_X86_FS: off = FREEBSD_UC_MCONTEXT_FS_OFF; break;
    case UNW_X86_ES: off = FREEBSD_UC_MCONTEXT_ES_OFF; break;
    case UNW_X86_DS: off = FREEBSD_UC_MCONTEXT_SS_OFF; break;
    case UNW_X86_EDI: off = FREEBSD_UC_MCONTEXT_EDI_OFF; break;
    case UNW_X86_ESI: off = FREEBSD_UC_MCONTEXT_ESI_OFF; break;
    case UNW_X86_EBP: off = FREEBSD_UC_MCONTEXT_EBP_OFF; break;
    case UNW_X86_ESP: off = FREEBSD_UC_MCONTEXT_ESP_OFF; break;
    case UNW_X86_EBX: off = FREEBSD_UC_MCONTEXT_EBX_OFF; break;
    case UNW_X86_EDX: off = FREEBSD_UC_MCONTEXT_EDX_OFF; break;
    case UNW_X86_ECX: off = FREEBSD_UC_MCONTEXT_ECX_OFF; break;
    case UNW_X86_EAX: off = FREEBSD_UC_MCONTEXT_EAX_OFF; break;
    case UNW_X86_TRAPNO: off = FREEBSD_UC_MCONTEXT_TRAPNO_OFF; break;
    case UNW_X86_EIP: off = FREEBSD_UC_MCONTEXT_EIP_OFF; break;
    case UNW_X86_CS: off = FREEBSD_UC_MCONTEXT_CS_OFF; break;
    case UNW_X86_EFLAGS: off = FREEBSD_UC_MCONTEXT_EFLAGS_OFF; break;
    case UNW_X86_SS: off = FREEBSD_UC_MCONTEXT_SS_OFF; break;

    case UNW_X86_FCW:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_CW_OFF;
      xmm_off = FREEBSD_UC_MCONTEXT_CW_XMM_OFF;
      break;
    case UNW_X86_FSW:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_SW_OFF;
      xmm_off = FREEBSD_UC_MCONTEXT_SW_XMM_OFF;
      break;
    case UNW_X86_FTW:
      is_fpstate = 1;
      xmm_off = FREEBSD_UC_MCONTEXT_TAG_XMM_OFF;
      off = FREEBSD_UC_MCONTEXT_TAG_OFF;
      break;
    case UNW_X86_FCS:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_CSSEL_OFF;
      xmm_off = FREEBSD_UC_MCONTEXT_CSSEL_XMM_OFF;
      break;
    case UNW_X86_FIP:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_IPOFF_OFF;
      xmm_off = FREEBSD_UC_MCONTEXT_IPOFF_XMM_OFF;
      break;
    case UNW_X86_FEA:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_DATAOFF_OFF;
      xmm_off = FREEBSD_UC_MCONTEXT_DATAOFF_XMM_OFF;
      break;
    case UNW_X86_FDS:
      is_fpstate = 1;
      off = FREEBSD_US_MCONTEXT_DATASEL_OFF;
      xmm_off = FREEBSD_US_MCONTEXT_DATASEL_XMM_OFF;
      break;
    case UNW_X86_MXCSR:
      is_fpstate = 1;
      is_xmmstate = 1;
      xmm_off = FREEBSD_UC_MCONTEXT_MXCSR_XMM_OFF;
      break;

      /* stacked fp registers */
    case UNW_X86_ST0: case UNW_X86_ST1: case UNW_X86_ST2: case UNW_X86_ST3:
    case UNW_X86_ST4: case UNW_X86_ST5: case UNW_X86_ST6: case UNW_X86_ST7:
      is_fpstate = 1;
      off = FREEBSD_UC_MCONTEXT_ST0_OFF + 10*(reg - UNW_X86_ST0);
      xmm_off = FREEBSD_UC_MCONTEXT_ST0_XMM_OFF + 10*(reg - UNW_X86_ST0);
      break;

     /* SSE fp registers */
    case UNW_X86_XMM0_lo: case UNW_X86_XMM0_hi:
    case UNW_X86_XMM1_lo: case UNW_X86_XMM1_hi:
    case UNW_X86_XMM2_lo: case UNW_X86_XMM2_hi:
    case UNW_X86_XMM3_lo: case UNW_X86_XMM3_hi:
    case UNW_X86_XMM4_lo: case UNW_X86_XMM4_hi:
    case UNW_X86_XMM5_lo: case UNW_X86_XMM5_hi:
    case UNW_X86_XMM6_lo: case UNW_X86_XMM6_hi:
    case UNW_X86_XMM7_lo: case UNW_X86_XMM7_hi:
      is_fpstate = 1;
      is_xmmstate = 1;
      xmm_off = FREEBSD_UC_MCONTEXT_XMM0_OFF + 8*(reg - UNW_X86_XMM0_lo);
      break;
    case UNW_X86_XMM0:
    case UNW_X86_XMM1:
    case UNW_X86_XMM2:
    case UNW_X86_XMM3:
    case UNW_X86_XMM4:
    case UNW_X86_XMM5:
    case UNW_X86_XMM6:
    case UNW_X86_XMM7:
      is_fpstate = 1;
      is_xmmstate = 1;
      xmm_off = FREEBSD_UC_MCONTEXT_XMM0_OFF + 16*(reg - UNW_X86_XMM0);
      break;

    case UNW_X86_FOP:
    case UNW_X86_TSS:
    case UNW_X86_LDT:
    default:
      return DWARF_REG_LOC (&c->dwarf, reg);
    }

  if (is_fpstate)
    {
      if ((ret = dwarf_get (&c->dwarf,
           DWARF_MEM_LOC (&c->dwarf, addr + FREEBSD_UC_MCONTEXT_FPSTATE_OFF),
           &fpstate)) < 0)
        return DWARF_NULL_LOC;
      if (fpstate == FREEBSD_UC_MCONTEXT_FPOWNED_NONE)
        return DWARF_NULL_LOC;
      if ((ret = dwarf_get (&c->dwarf,
           DWARF_MEM_LOC (&c->dwarf, addr + FREEBSD_UC_MCONTEXT_FPFORMAT_OFF),
           &fpformat)) < 0)
        return DWARF_NULL_LOC;
      if (fpformat == FREEBSD_UC_MCONTEXT_FPFMT_NODEV ||
          (is_xmmstate && fpformat != FREEBSD_UC_MCONTEXT_FPFMT_XMM))
        return DWARF_NULL_LOC;
      if (is_xmmstate)
        off = xmm_off;
    }

    return DWARF_MEM_LOC (c, addr + off);
}

#ifndef UNW_REMOTE_ONLY
HIDDEN void *
x86_r_uc_addr (ucontext_t *uc, int reg)
{
  void *addr;

  switch (reg)
    {
    case UNW_X86_GS:  addr = &uc->uc_mcontext.mc_gs; break;
    case UNW_X86_FS:  addr = &uc->uc_mcontext.mc_fs; break;
    case UNW_X86_ES:  addr = &uc->uc_mcontext.mc_es; break;
    case UNW_X86_DS:  addr = &uc->uc_mcontext.mc_ds; break;
    case UNW_X86_EAX: addr = &uc->uc_mcontext.mc_eax; break;
    case UNW_X86_EBX: addr = &uc->uc_mcontext.mc_ebx; break;
    case UNW_X86_ECX: addr = &uc->uc_mcontext.mc_ecx; break;
    case UNW_X86_EDX: addr = &uc->uc_mcontext.mc_edx; break;
    case UNW_X86_ESI: addr = &uc->uc_mcontext.mc_esi; break;
    case UNW_X86_EDI: addr = &uc->uc_mcontext.mc_edi; break;
    case UNW_X86_EBP: addr = &uc->uc_mcontext.mc_ebp; break;
    case UNW_X86_EIP: addr = &uc->uc_mcontext.mc_eip; break;
    case UNW_X86_ESP: addr = &uc->uc_mcontext.mc_esp; break;
    case UNW_X86_TRAPNO:  addr = &uc->uc_mcontext.mc_trapno; break;
    case UNW_X86_CS:  addr = &uc->uc_mcontext.mc_cs; break;
    case UNW_X86_EFLAGS:  addr = &uc->uc_mcontext.mc_eflags; break;
    case UNW_X86_SS:  addr = &uc->uc_mcontext.mc_ss; break;

    default:
      addr = NULL;
    }
  return addr;
}

HIDDEN int
x86_local_resume (unw_addr_space_t as, unw_cursor_t *cursor, void *arg)
{
  struct cursor *c = (struct cursor *) cursor;
  ucontext_t *uc = c->uc;

  /* Ensure c->pi is up-to-date.  On x86, it's relatively common to be
     missing DWARF unwind info.  We don't want to fail in that case,
     because the frame-chain still would let us do a backtrace at
     least.  */
  dwarf_make_proc_info (&c->dwarf);

  if (c->sigcontext_format == X86_SCF_NONE) {
      Debug (8, "resuming at ip=%x via setcontext()\n", c->dwarf.ip);
      setcontext (uc);
      abort();
  } else if (c->sigcontext_format == X86_SCF_FREEBSD_SIGFRAME) {
      struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;

      Debug (8, "resuming at ip=%x via sigreturn(%p)\n", c->dwarf.ip, sc);
      sigreturn((ucontext_t *)((const char *)sc + FREEBSD_SC_UCONTEXT_OFF));
      abort();
  } else {
      Debug (8, "resuming at ip=%x for sigcontext format %d not implemented\n",
      c->dwarf.ip, c->sigcontext_format);
      abort();
  }
  return -UNW_EINVAL;
}

#endif
