/* libunwind - a platform-independent unwind library
   Copyright (C) 2002-2004 Hewlett-Packard Co
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

#include "unwind_i.h"
#include "offsets.h"

PROTECTED int
unw_is_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  unw_word_t w0, w1, ip;
  unw_addr_space_t as;
  unw_accessors_t *a;
  void *arg;
  int ret;

  as = c->dwarf.as;
  a = unw_get_accessors (as);
  arg = c->dwarf.as_arg;

  /* Check if EIP points at sigreturn() sequence.  On Linux, this is:

    __restore:
        0x58                            pop %eax
        0xb8 0x77 0x00 0x00 0x00        movl 0x77,%eax
        0xcd 0x80                       int 0x80

     without SA_SIGINFO, and

    __restore_rt:
       0xb8 0xad 0x00 0x00 0x00        movl 0xad,%eax
       0xcd 0x80                       int 0x80
       0x00

     if SA_SIGINFO is specified.
  */
  ip = c->dwarf.ip;
  if ((*a->access_mem) (as, ip, &w0, 0, arg) < 0
      || (*a->access_mem) (as, ip + 4, &w1, 0, arg) < 0)
    ret = 0;
  else
    ret = ((w0 == 0x0077b858 && w1 == 0x80cd0000)
         || (w0 == 0x0000adb8 && (w1 & 0xffffff) == 0x80cd00));
  Debug (16, "returning %d\n", ret);
  return ret;
}

PROTECTED int
unw_handle_signal_frame (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  int ret;

  /* c->esp points at the arguments to the handler.  Without
     SA_SIGINFO, the arguments consist of a signal number
     followed by a struct sigcontext.  With SA_SIGINFO, the
     arguments consist a signal number, a siginfo *, and a
     ucontext *. */
  unw_word_t sc_addr;
  unw_word_t siginfo_ptr_addr = c->dwarf.cfa + 4;
  unw_word_t sigcontext_ptr_addr = c->dwarf.cfa + 8;
  unw_word_t siginfo_ptr, sigcontext_ptr;
  struct dwarf_loc esp_loc, siginfo_ptr_loc, sigcontext_ptr_loc;

  siginfo_ptr_loc = DWARF_LOC (siginfo_ptr_addr, 0);
  sigcontext_ptr_loc = DWARF_LOC (sigcontext_ptr_addr, 0);
  ret = (dwarf_get (&c->dwarf, siginfo_ptr_loc, &siginfo_ptr)
         | dwarf_get (&c->dwarf, sigcontext_ptr_loc, &sigcontext_ptr));
  if (ret < 0)
    {
      Debug (2, "returning 0\n");
      return 0;
    }
  if (siginfo_ptr < c->dwarf.cfa
      || siginfo_ptr > c->dwarf.cfa + 256
      || sigcontext_ptr < c->dwarf.cfa
      || sigcontext_ptr > c->dwarf.cfa + 256)
    {
      /* Not plausible for SA_SIGINFO signal */
      c->sigcontext_format = X86_SCF_LINUX_SIGFRAME;
      c->sigcontext_addr = sc_addr = c->dwarf.cfa + 4;
    }
  else
    {
      /* If SA_SIGINFO were not specified, we actually read
         various segment pointers instead.  We believe that at
         least fs and _fsh are always zero for linux, so it is
         not just unlikely, but impossible that we would end
         up here. */
      c->sigcontext_format = X86_SCF_LINUX_RT_SIGFRAME;
      c->sigcontext_addr = sigcontext_ptr;
      sc_addr = sigcontext_ptr + LINUX_UC_MCONTEXT_OFF;
    }
  esp_loc = DWARF_LOC (sc_addr + LINUX_SC_ESP_OFF, 0);
  ret = dwarf_get (&c->dwarf, esp_loc, &c->dwarf.cfa);
  if (ret < 0)
    {
      Debug (2, "returning 0\n");
      return 0;
    }

  c->dwarf.loc[EAX] = DWARF_LOC (sc_addr + LINUX_SC_EAX_OFF, 0);
  c->dwarf.loc[ECX] = DWARF_LOC (sc_addr + LINUX_SC_ECX_OFF, 0);
  c->dwarf.loc[EDX] = DWARF_LOC (sc_addr + LINUX_SC_EDX_OFF, 0);
  c->dwarf.loc[EBX] = DWARF_LOC (sc_addr + LINUX_SC_EBX_OFF, 0);
  c->dwarf.loc[EBP] = DWARF_LOC (sc_addr + LINUX_SC_EBP_OFF, 0);
  c->dwarf.loc[ESI] = DWARF_LOC (sc_addr + LINUX_SC_ESI_OFF, 0);
  c->dwarf.loc[EDI] = DWARF_LOC (sc_addr + LINUX_SC_EDI_OFF, 0);
  c->dwarf.loc[EFLAGS] = DWARF_NULL_LOC;
  c->dwarf.loc[TRAPNO] = DWARF_NULL_LOC;
  c->dwarf.loc[ST0] = DWARF_NULL_LOC;
  c->dwarf.loc[EIP] = DWARF_LOC (sc_addr + LINUX_SC_EIP_OFF, 0);
  c->dwarf.loc[ESP] = DWARF_LOC (sc_addr + LINUX_SC_ESP_OFF, 0);

  return 0;
}

HIDDEN dwarf_loc_t
x86_get_scratch_loc (struct cursor *c, unw_regnum_t reg)
{
  unw_word_t addr = c->sigcontext_addr, fpstate_addr, off;
  int ret, is_fpstate = 0;

  switch (c->sigcontext_format)
    {
    case X86_SCF_NONE:
      return DWARF_REG_LOC (&c->dwarf, reg);

    case X86_SCF_LINUX_SIGFRAME:
      break;

    case X86_SCF_LINUX_RT_SIGFRAME:
      addr += LINUX_UC_MCONTEXT_OFF;
      break;

    default:
      return DWARF_NULL_LOC;
    }

  switch (reg)
    {
    case UNW_X86_GS: off = LINUX_SC_GS_OFF; break;
    case UNW_X86_FS: off = LINUX_SC_FS_OFF; break;
    case UNW_X86_ES: off = LINUX_SC_ES_OFF; break;
    case UNW_X86_DS: off = LINUX_SC_DS_OFF; break;
    case UNW_X86_EDI: off = LINUX_SC_EDI_OFF; break;
    case UNW_X86_ESI: off = LINUX_SC_ESI_OFF; break;
    case UNW_X86_EBP: off = LINUX_SC_EBP_OFF; break;
    case UNW_X86_ESP: off = LINUX_SC_ESP_OFF; break;
    case UNW_X86_EBX: off = LINUX_SC_EBX_OFF; break;
    case UNW_X86_EDX: off = LINUX_SC_EDX_OFF; break;
    case UNW_X86_ECX: off = LINUX_SC_ECX_OFF; break;
    case UNW_X86_EAX: off = LINUX_SC_EAX_OFF; break;
    case UNW_X86_TRAPNO: off = LINUX_SC_TRAPNO_OFF; break;
    case UNW_X86_EIP: off = LINUX_SC_EIP_OFF; break;
    case UNW_X86_CS: off = LINUX_SC_CS_OFF; break;
    case UNW_X86_EFLAGS: off = LINUX_SC_EFLAGS_OFF; break;
    case UNW_X86_SS: off = LINUX_SC_SS_OFF; break;

      /* The following is probably not correct for all possible cases.
         Somebody who understands this better should review this for
         correctness.  */

    case UNW_X86_FCW: is_fpstate = 1; off = LINUX_FPSTATE_CW_OFF; break;
    case UNW_X86_FSW: is_fpstate = 1; off = LINUX_FPSTATE_SW_OFF; break;
    case UNW_X86_FTW: is_fpstate = 1; off = LINUX_FPSTATE_TAG_OFF; break;
    case UNW_X86_FCS: is_fpstate = 1; off = LINUX_FPSTATE_CSSEL_OFF; break;
    case UNW_X86_FIP: is_fpstate = 1; off = LINUX_FPSTATE_IPOFF_OFF; break;
    case UNW_X86_FEA: is_fpstate = 1; off = LINUX_FPSTATE_DATAOFF_OFF; break;
    case UNW_X86_FDS: is_fpstate = 1; off = LINUX_FPSTATE_DATASEL_OFF; break;
    case UNW_X86_MXCSR: is_fpstate = 1; off = LINUX_FPSTATE_MXCSR_OFF; break;

      /* stacked fp registers */
    case UNW_X86_ST0: case UNW_X86_ST1: case UNW_X86_ST2: case UNW_X86_ST3:
    case UNW_X86_ST4: case UNW_X86_ST5: case UNW_X86_ST6: case UNW_X86_ST7:
      is_fpstate = 1;
      off = LINUX_FPSTATE_ST0_OFF + 10*(reg - UNW_X86_ST0);
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
      off = LINUX_FPSTATE_XMM0_OFF + 8*(reg - UNW_X86_XMM0_lo);
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
      off = LINUX_FPSTATE_XMM0_OFF + 16*(reg - UNW_X86_XMM0);
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
                            DWARF_MEM_LOC (&c->dwarf,
                                           addr + LINUX_SC_FPSTATE_OFF),
                            &fpstate_addr)) < 0)
        return DWARF_NULL_LOC;

      if (!fpstate_addr)
        return DWARF_NULL_LOC;

      return DWARF_MEM_LOC (c, fpstate_addr + off);
    }
  else
    return DWARF_MEM_LOC (c, addr + off);
}

#ifndef UNW_REMOTE_ONLY
HIDDEN void *
x86_r_uc_addr (ucontext_t *uc, int reg)
{
  void *addr;

  switch (reg)
    {
    case UNW_X86_GS:  addr = &uc->uc_mcontext.gregs[REG_GS]; break;
    case UNW_X86_FS:  addr = &uc->uc_mcontext.gregs[REG_FS]; break;
    case UNW_X86_ES:  addr = &uc->uc_mcontext.gregs[REG_ES]; break;
    case UNW_X86_DS:  addr = &uc->uc_mcontext.gregs[REG_DS]; break;
    case UNW_X86_EAX: addr = &uc->uc_mcontext.gregs[REG_EAX]; break;
    case UNW_X86_EBX: addr = &uc->uc_mcontext.gregs[REG_EBX]; break;
    case UNW_X86_ECX: addr = &uc->uc_mcontext.gregs[REG_ECX]; break;
    case UNW_X86_EDX: addr = &uc->uc_mcontext.gregs[REG_EDX]; break;
    case UNW_X86_ESI: addr = &uc->uc_mcontext.gregs[REG_ESI]; break;
    case UNW_X86_EDI: addr = &uc->uc_mcontext.gregs[REG_EDI]; break;
    case UNW_X86_EBP: addr = &uc->uc_mcontext.gregs[REG_EBP]; break;
    case UNW_X86_EIP: addr = &uc->uc_mcontext.gregs[REG_EIP]; break;
    case UNW_X86_ESP: addr = &uc->uc_mcontext.gregs[REG_ESP]; break;
    case UNW_X86_TRAPNO:  addr = &uc->uc_mcontext.gregs[REG_TRAPNO]; break;
    case UNW_X86_CS:  addr = &uc->uc_mcontext.gregs[REG_CS]; break;
    case UNW_X86_EFLAGS:  addr = &uc->uc_mcontext.gregs[REG_EFL]; break;
    case UNW_X86_SS:  addr = &uc->uc_mcontext.gregs[REG_SS]; break;

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

  if (unlikely (c->sigcontext_format != X86_SCF_NONE))
    {
      struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;

      Debug (8, "resuming at ip=%x via sigreturn(%p)\n", c->dwarf.ip, sc);
      x86_sigreturn (sc);
    }
  else
    {
      Debug (8, "resuming at ip=%x via setcontext()\n", c->dwarf.ip);
      setcontext (uc);
    }
  return -UNW_EINVAL;
}

/* sigreturn() is a no-op on x86 glibc.  */
HIDDEN void
x86_sigreturn (unw_cursor_t *cursor)
{
  struct cursor *c = (struct cursor *) cursor;
  struct sigcontext *sc = (struct sigcontext *) c->sigcontext_addr;
  mcontext_t *sc_mcontext = &((struct ucontext*)sc)->uc_mcontext;
  /* Copy in saved uc - all preserved regs are at the start of sigcontext */
  memcpy(sc_mcontext, &c->uc->uc_mcontext,
         DWARF_NUM_PRESERVED_REGS * sizeof(unw_word_t));

  Debug (8, "resuming at ip=%llx via sigreturn(%p)\n",
             (unsigned long long) c->dwarf.ip, sc);
  __asm__ __volatile__ ("mov %0, %%esp;"
                        "mov %1, %%eax;"
                        "syscall"
                        :: "r"(sc), "i"(SYS_rt_sigreturn)
                        : "memory");
  abort();
}
#endif
