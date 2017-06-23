/* Linux-specific definitions: */

/* Define various structure offsets to simplify cross-compilation.  */

/* Offsets for TILEGX Linux "ucontext_t":  */

#define LINUX_UC_FLAGS_OFF      0x0
#define LINUX_UC_LINK_OFF       0x8
#define LINUX_UC_STACK_OFF      0x10
#define LINUX_UC_MCONTEXT_OFF   0x28
#define LINUX_UC_SIGMASK_OFF    0x228
#define LINUX_UC_MCONTEXT_GREGS 0x28
