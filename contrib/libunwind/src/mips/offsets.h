/* Linux-specific definitions: */

/* Define various structure offsets to simplify cross-compilation.  */

/* FIXME: Currently these are only used in getcontext.S, which is only used
   for a local unwinder, so we can use the compile-time ABI.  At a later date
   we will want all three here, to use for signal handlers.  Also, because
   of the three ABIs, gen-offsets.c can not quite generate this file.  */

/* Offsets for MIPS Linux "ucontext_t":  */

/* First 24 bytes in sigframe are argument save space and padding for
what used to be signal trampolines. Ref: arch/mips/kernel/signal.c */
#define LINUX_SF_TRAMP_SIZE 0x18

#if _MIPS_SIM == _ABIO32

# define LINUX_UC_FLAGS_OFF     0x0
# define LINUX_UC_LINK_OFF      0x4
# define LINUX_UC_STACK_OFF     0x8
# define LINUX_UC_MCONTEXT_OFF  0x18
# define LINUX_UC_SIGMASK_OFF   0x268
# define LINUX_UC_MCONTEXT_PC   0x20
# define LINUX_UC_MCONTEXT_GREGS        0x28

#elif _MIPS_SIM == _ABIN32

# define LINUX_UC_FLAGS_OFF     0x0
# define LINUX_UC_LINK_OFF      0x4
# define LINUX_UC_STACK_OFF     0x8
# define LINUX_UC_MCONTEXT_OFF  0x18
# define LINUX_UC_SIGMASK_OFF   0x270
# define LINUX_UC_MCONTEXT_PC   0x258
# define LINUX_UC_MCONTEXT_GREGS        0x18

#elif _MIPS_SIM == _ABI64

# define LINUX_UC_FLAGS_OFF     0x0
# define LINUX_UC_LINK_OFF      0x8
# define LINUX_UC_STACK_OFF     0x10
# define LINUX_UC_MCONTEXT_OFF  0x28
# define LINUX_UC_SIGMASK_OFF   0x280
# define LINUX_UC_MCONTEXT_PC   0x268
# define LINUX_UC_MCONTEXT_GREGS        0x28

#else

#error Unsupported ABI

#endif

#define LINUX_SC_R0_OFF   (LINUX_UC_MCONTEXT_GREGS - LINUX_UC_MCONTEXT_OFF)
#define LINUX_SC_R1_OFF   (LINUX_SC_R0_OFF + 1*8)
#define LINUX_SC_R2_OFF   (LINUX_SC_R0_OFF + 2*8)
#define LINUX_SC_R3_OFF   (LINUX_SC_R0_OFF + 3*8)
#define LINUX_SC_R4_OFF   (LINUX_SC_R0_OFF + 4*8)
#define LINUX_SC_R5_OFF   (LINUX_SC_R0_OFF + 5*8)
#define LINUX_SC_R6_OFF   (LINUX_SC_R0_OFF + 6*8)
#define LINUX_SC_R7_OFF   (LINUX_SC_R0_OFF + 7*8)
#define LINUX_SC_R8_OFF   (LINUX_SC_R0_OFF + 8*8)
#define LINUX_SC_R9_OFF   (LINUX_SC_R0_OFF + 9*8)
#define LINUX_SC_R10_OFF  (LINUX_SC_R0_OFF + 10*8)
#define LINUX_SC_R11_OFF  (LINUX_SC_R0_OFF + 11*8)
#define LINUX_SC_R12_OFF  (LINUX_SC_R0_OFF + 12*8)
#define LINUX_SC_R13_OFF  (LINUX_SC_R0_OFF + 13*8)
#define LINUX_SC_R14_OFF  (LINUX_SC_R0_OFF + 14*8)
#define LINUX_SC_R15_OFF  (LINUX_SC_R0_OFF + 15*8)
#define LINUX_SC_R16_OFF  (LINUX_SC_R0_OFF + 16*8)
#define LINUX_SC_R17_OFF  (LINUX_SC_R0_OFF + 17*8)
#define LINUX_SC_R18_OFF  (LINUX_SC_R0_OFF + 18*8)
#define LINUX_SC_R19_OFF  (LINUX_SC_R0_OFF + 19*8)
#define LINUX_SC_R20_OFF  (LINUX_SC_R0_OFF + 20*8)
#define LINUX_SC_R21_OFF  (LINUX_SC_R0_OFF + 21*8)
#define LINUX_SC_R22_OFF  (LINUX_SC_R0_OFF + 22*8)
#define LINUX_SC_R23_OFF  (LINUX_SC_R0_OFF + 23*8)
#define LINUX_SC_R24_OFF  (LINUX_SC_R0_OFF + 24*8)
#define LINUX_SC_R25_OFF  (LINUX_SC_R0_OFF + 25*8)
#define LINUX_SC_R26_OFF  (LINUX_SC_R0_OFF + 26*8)
#define LINUX_SC_R27_OFF  (LINUX_SC_R0_OFF + 27*8)
#define LINUX_SC_R28_OFF  (LINUX_SC_R0_OFF + 28*8)
#define LINUX_SC_R29_OFF  (LINUX_SC_R0_OFF + 29*8)
#define LINUX_SC_R30_OFF  (LINUX_SC_R0_OFF + 30*8)
#define LINUX_SC_R31_OFF  (LINUX_SC_R0_OFF + 31*8)

#define LINUX_SC_SP_OFF   LINUX_SC_R29_OFF
#define LINUX_SC_PC_OFF   (LINUX_UC_MCONTEXT_PC - LINUX_UC_MCONTEXT_OFF)
