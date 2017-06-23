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

"/* Offsets for AArch64 Linux \"ucontext_t\":  */\n\n");

  UC ("FLAGS", uc_flags);
  UC ("LINK", uc_link);
  UC ("STACK", uc_stack);
  UC ("MCONTEXT", uc_mcontext);
  UC ("SIGMASK", uc_sigmask);

  printf ("\n/* Offsets for AArch64 Linux \"struct sigcontext\":  */\n\n");

  SC ("R0",  regs[0]);
  SC ("R1",  regs[1]);
  SC ("R2",  regs[2]);
  SC ("R3",  regs[3]);
  SC ("R4",  regs[4]);
  SC ("R5",  regs[5]);
  SC ("R6",  regs[6]);
  SC ("R7",  regs[7]);
  SC ("R8",  regs[8]);
  SC ("R9",  regs[9]);
  SC ("R10", regs[10]);
  SC ("R11", regs[11]);
  SC ("R12", regs[12]);
  SC ("R13", regs[13]);
  SC ("R14", regs[14]);
  SC ("R15", regs[15]);
  SC ("R16", regs[16]);
  SC ("R17", regs[17]);
  SC ("R18", regs[18]);
  SC ("R19", regs[19]);
  SC ("R20", regs[20]);
  SC ("R21", regs[21]);
  SC ("R22", regs[22]);
  SC ("R23", regs[23]);
  SC ("R24", regs[24]);
  SC ("R25", regs[25]);
  SC ("R26", regs[26]);
  SC ("R27", regs[27]);
  SC ("R28", regs[28]);
  SC ("R29", regs[29]);
  SC ("R30", regs[30]);
  SC ("R31", regs[31]);

  SC ("PC", pc);
  SC ("SP", sp);
  SC ("Fault", fault_address);
  SC ("state", pstate);
  return 0;
}
