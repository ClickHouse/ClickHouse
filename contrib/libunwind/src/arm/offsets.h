/* Linux-specific definitions: */

/* Define various structure offsets to simplify cross-compilation.  */

/* Offsets for ARM Linux "ucontext_t":  */

#define LINUX_UC_FLAGS_OFF      0x00
#define LINUX_UC_LINK_OFF       0x04
#define LINUX_UC_STACK_OFF      0x08
#define LINUX_UC_MCONTEXT_OFF   0x14
#define LINUX_UC_SIGMASK_OFF    0x68
#define LINUX_UC_REGSPACE_OFF   0xE8

/* Offsets for ARM Linux "struct sigcontext":  */

#define LINUX_SC_TRAPNO_OFF     0x00
#define LINUX_SC_ERRORCODE_OFF  0x04
#define LINUX_SC_OLDMASK_OFF    0x08
#define LINUX_SC_R0_OFF         0x0C
#define LINUX_SC_R1_OFF         0x10
#define LINUX_SC_R2_OFF         0x14
#define LINUX_SC_R3_OFF         0x18
#define LINUX_SC_R4_OFF         0x1C
#define LINUX_SC_R5_OFF         0x20
#define LINUX_SC_R6_OFF         0x24
#define LINUX_SC_R7_OFF         0x28
#define LINUX_SC_R8_OFF         0x2C
#define LINUX_SC_R9_OFF         0x30
#define LINUX_SC_R10_OFF        0x34
#define LINUX_SC_FP_OFF         0x38
#define LINUX_SC_IP_OFF         0x3C
#define LINUX_SC_SP_OFF         0x40
#define LINUX_SC_LR_OFF         0x44
#define LINUX_SC_PC_OFF         0x48
#define LINUX_SC_CPSR_OFF       0x4C
#define LINUX_SC_FAULTADDR_OFF  0x50

/* FreeBSD-specific definitions: */

#define FREEBSD_SC_UCONTEXT_OFF		0x40
#define	FREEBSD_UC_MCONTEXT_OFF		0x10
#define FREEBSD_MC_R0_OFF		0
