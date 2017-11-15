#include <stdio.h>
#include <stddef.h>
#include <ucontext.h>

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

"/* Offsets for TILEGX Linux \"ucontext_t\":  */\n\n");

  UC ("FLAGS", uc_flags);
  UC ("LINK", uc_link);
  UC ("STACK", uc_stack);
  UC ("MCONTEXT", uc_mcontext);
  UC ("SIGMASK", uc_sigmask);

  UC ("MCONTEXT_GREGS", uc_mcontext.gregs);

  return 0;
}
