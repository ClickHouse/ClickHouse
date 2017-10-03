/* libunwind - a platform-independent unwind library

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

#include "_UCD_lib.h"

#include "_UCD_internal.h"

int
_UCD_access_reg (unw_addr_space_t as,
                                unw_regnum_t regnum, unw_word_t *valp,
                                int write, void *arg)
{
  if (write)
    {
      Debug(0, "write is not supported\n");
      return -UNW_EINVAL;
    }

  struct UCD_info *ui = arg;

#if defined(UNW_TARGET_X86)
  switch (regnum) {
  case UNW_X86_EAX:
     *valp = ui->prstatus->pr_reg.r_eax;
     break;
  case UNW_X86_EDX:
     *valp = ui->prstatus->pr_reg.r_edx;
     break;
  case UNW_X86_ECX:
     *valp = ui->prstatus->pr_reg.r_ecx;
     break;
  case UNW_X86_EBX:
     *valp = ui->prstatus->pr_reg.r_ebx;
     break;
  case UNW_X86_ESI:
     *valp = ui->prstatus->pr_reg.r_esi;
     break;
  case UNW_X86_EDI:
     *valp = ui->prstatus->pr_reg.r_edi;
     break;
  case UNW_X86_EBP:
     *valp = ui->prstatus->pr_reg.r_ebp;
     break;
  case UNW_X86_ESP:
     *valp = ui->prstatus->pr_reg.r_esp;
     break;
  case UNW_X86_EIP:
     *valp = ui->prstatus->pr_reg.r_eip;
     break;
  case UNW_X86_EFLAGS:
     *valp = ui->prstatus->pr_reg.r_eflags;
     break;
  case UNW_X86_TRAPNO:
     *valp = ui->prstatus->pr_reg.r_trapno;
     break;
  default:
      Debug(0, "bad regnum:%d\n", regnum);
      return -UNW_EINVAL;
  }
#elif defined(UNW_TARGET_X86_64)
  switch (regnum) {
  case UNW_X86_64_RAX:
     *valp = ui->prstatus->pr_reg.r_rax;
     break;
  case UNW_X86_64_RDX:
     *valp = ui->prstatus->pr_reg.r_rdx;
     break;
  case UNW_X86_64_RCX:
     *valp = ui->prstatus->pr_reg.r_rcx;
     break;
  case UNW_X86_64_RBX:
     *valp = ui->prstatus->pr_reg.r_rbx;
     break;
  case UNW_X86_64_RSI:
     *valp = ui->prstatus->pr_reg.r_rsi;
     break;
  case UNW_X86_64_RDI:
     *valp = ui->prstatus->pr_reg.r_rdi;
     break;
  case UNW_X86_64_RBP:
     *valp = ui->prstatus->pr_reg.r_rbp;
     break;
  case UNW_X86_64_RSP:
     *valp = ui->prstatus->pr_reg.r_rsp;
     break;
  case UNW_X86_64_RIP:
     *valp = ui->prstatus->pr_reg.r_rip;
     break;
  default:
      Debug(0, "bad regnum:%d\n", regnum);
      return -UNW_EINVAL;
  }
#elif defined(UNW_TARGET_ARM)
  if (regnum >= UNW_ARM_R0 && regnum <= UNW_ARM_R12) {
     *valp = ui->prstatus->pr_reg.r[regnum];
  } else {
     switch (regnum) {
     case UNW_ARM_R13:
       *valp = ui->prstatus->pr_reg.r_sp;
       break;
     case UNW_ARM_R14:
       *valp = ui->prstatus->pr_reg.r_lr;
       break;
     case UNW_ARM_R15:
       *valp = ui->prstatus->pr_reg.r_pc;
       break;
     default:
       Debug(0, "bad regnum:%d\n", regnum);
       return -UNW_EINVAL;
     }
  }
#else
#error Port me
#endif

  return 0;
}
