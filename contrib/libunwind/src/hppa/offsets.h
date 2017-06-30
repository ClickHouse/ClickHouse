#define LINUX_UC_FLAGS_OFF      0x000
#define LINUX_UC_LINK_OFF       0x004
#define LINUX_UC_STACK_OFF      0x008
#define LINUX_UC_MCONTEXT_OFF   0x018
#define LINUX_UC_SIGMASK_OFF    0x1b8

#define LINUX_SC_FLAGS_OFF      0x000
#define LINUX_SC_GR_OFF         0x004
#define LINUX_SC_FR_OFF         0x088
#define LINUX_SC_IASQ_OFF       0x188
#define LINUX_SC_IAOQ_OFF       0x190
#define LINUX_SC_SAR_OFF        0x198

/* The signal frame contains 4 words of space for the sigreturn
   trampoline, the siginfo structure, and then the sigcontext
   structure.  See include/asm-parisc/compat_rt_sigframe.h.  */
#define LINUX_RT_SIGFRAME_UC_OFF        0xac
