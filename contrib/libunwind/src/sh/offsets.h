/* Linux-specific definitions: */

/* Define various structure offsets to simplify cross-compilation.  */

/* Offsets for SH Linux "ucontext_t":  */

#define LINUX_UC_FLAGS_OFF      0x0
#define LINUX_UC_LINK_OFF       0x4
#define LINUX_UC_STACK_OFF      0x8
#define LINUX_UC_MCONTEXT_OFF   0x14
#define LINUX_UC_SIGMASK_OFF    0xFC

/* Offsets for SH Linux "struct sigcontext":  */

#define LINUX_SC_R0_OFF 0x4
#define LINUX_SC_R1_OFF 0x8
#define LINUX_SC_R2_OFF 0xC
#define LINUX_SC_R3_OFF 0x10
#define LINUX_SC_R4_OFF 0x14
#define LINUX_SC_R5_OFF 0x18
#define LINUX_SC_R6_OFF 0x1C
#define LINUX_SC_R7_OFF 0x20
#define LINUX_SC_R8_OFF 0x24
#define LINUX_SC_R9_OFF 0x28
#define LINUX_SC_R10_OFF        0x2C
#define LINUX_SC_R11_OFF        0x30
#define LINUX_SC_R12_OFF        0x34
#define LINUX_SC_R13_OFF        0x38
#define LINUX_SC_R14_OFF        0x3C
#define LINUX_SC_R15_OFF        0x40
#define LINUX_SC_PC_OFF 0x44
#define LINUX_SC_PR_OFF 0x48
