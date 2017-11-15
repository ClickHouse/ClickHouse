#include <stdio.h>
#include <stddef.h>
#include <ucontext.h>
#include <asm/sigcontext.h>

#define UC(N,X) \
  printf ("#define LINUX_UC_" N "_OFF\t0x%X\n", offsetof (ucontext_t, X))

#define SC(N,X) \
  printf ("#define LINUX_SC_" N "_OFF\t0x%X\n", offsetof (struct sigcontext, X))

int
main (void)
{
  printf (
"/* Linux-specific definitions: */\n\n"

"/* Define various structure offsets to simplify cross-compilation.  */\n\n"

"/* Offsets for SH Linux \"ucontext_t\":  */\n\n");

  UC ("FLAGS", uc_flags);
  UC ("LINK", uc_link);
  UC ("STACK", uc_stack);
  UC ("MCONTEXT", uc_mcontext);
  UC ("SIGMASK", uc_sigmask);

  printf ("\n/* Offsets for SH Linux \"struct sigcontext\":  */\n\n");

  SC ("R0",  sc_regs[0]);
  SC ("R1",  sc_regs[1]);
  SC ("R2",  sc_regs[2]);
  SC ("R3",  sc_regs[3]);
  SC ("R4",  sc_regs[4]);
  SC ("R5",  sc_regs[5]);
  SC ("R6",  sc_regs[6]);
  SC ("R7",  sc_regs[7]);
  SC ("R8",  sc_regs[8]);
  SC ("R9",  sc_regs[9]);
  SC ("R10", sc_regs[10]);
  SC ("R11", sc_regs[11]);
  SC ("R12", sc_regs[12]);
  SC ("R13", sc_regs[13]);
  SC ("R14", sc_regs[14]);
  SC ("R15", sc_regs[15]);

  SC ("PC", sc_pc);
  SC ("PR", sc_pr);

  return 0;
}
