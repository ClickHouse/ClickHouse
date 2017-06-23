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

"/* Offsets for ARM Linux \"ucontext_t\":  */\n\n");

  UC ("FLAGS", uc_flags);
  UC ("LINK", uc_link);
  UC ("STACK", uc_stack);
  UC ("MCONTEXT", uc_mcontext);
  UC ("SIGMASK", uc_sigmask);
  UC ("REGSPACE", uc_regspace);

  printf ("\n/* Offsets for ARM Linux \"struct sigcontext\":  */\n\n");

  SC ("TRAPNO", trap_no);
  SC ("ERRORCODE", error_code);
  SC ("OLDMASK", oldmask);
  SC ("R0", arm_r0);
  SC ("R1", arm_r1);
  SC ("R2", arm_r2);
  SC ("R3", arm_r3);
  SC ("R4", arm_r4);
  SC ("R5", arm_r5);
  SC ("R6", arm_r6);
  SC ("R7", arm_r7);
  SC ("R8", arm_r8);
  SC ("R9", arm_r9);
  SC ("R10", arm_r10);
  SC ("FP", arm_fp);
  SC ("IP", arm_ip);
  SC ("SP", arm_sp);
  SC ("LR", arm_lr);
  SC ("PC", arm_pc);
  SC ("CPSR", arm_cpsr);
  SC ("FAULTADDR", fault_address);

  return 0;
}
