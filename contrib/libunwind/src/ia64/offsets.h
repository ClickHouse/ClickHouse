/* Linux-specific definitions: */

/* Define various structure offsets to simplify cross-compilation.  */

/* The first three 64-bit words in a signal frame contain the signal
   number, siginfo pointer, and sigcontext pointer passed to the
   signal handler.  We use this to locate the sigcontext pointer.  */

#define LINUX_SIGFRAME_ARG2_OFF 0x10

#define LINUX_SC_FLAGS_OFF      0x000
#define LINUX_SC_NAT_OFF        0x008
#define LINUX_SC_STACK_OFF      0x010
#define LINUX_SC_IP_OFF         0x028
#define LINUX_SC_CFM_OFF        0x030
#define LINUX_SC_UM_OFF         0x038
#define LINUX_SC_AR_RSC_OFF     0x040
#define LINUX_SC_AR_BSP_OFF     0x048
#define LINUX_SC_AR_RNAT_OFF    0x050
#define LINUX_SC_AR_CCV         0x058
#define LINUX_SC_AR_UNAT_OFF    0x060
#define LINUX_SC_AR_FPSR_OFF    0x068
#define LINUX_SC_AR_PFS_OFF     0x070
#define LINUX_SC_AR_LC_OFF      0x078
#define LINUX_SC_PR_OFF         0x080
#define LINUX_SC_BR_OFF         0x088
#define LINUX_SC_GR_OFF         0x0c8
#define LINUX_SC_FR_OFF         0x1d0
#define LINUX_SC_RBS_BASE_OFF   0x9d0
#define LINUX_SC_LOADRS_OFF     0x9d8
#define LINUX_SC_AR_CSD_OFF     0x9e0
#define LINUX_SC_AR_SSD_OFF     0x9e8
#define LINUX_SC_MASK           0xa50

/* Layout of old Linux kernel interrupt frame (struct pt_regs).  */

#define LINUX_OLD_PT_IPSR_OFF   0x000
#define LINUX_OLD_PT_IIP_OFF    0x008
#define LINUX_OLD_PT_IFS_OFF    0x010
#define LINUX_OLD_PT_UNAT_OFF   0x018
#define LINUX_OLD_PT_PFS_OFF    0x020
#define LINUX_OLD_PT_RSC_OFF    0x028
#define LINUX_OLD_PT_RNAT_OFF   0x030
#define LINUX_OLD_PT_BSPSTORE_OFF 0x038
#define LINUX_OLD_PT_PR_OFF     0x040
#define LINUX_OLD_PT_B6_OFF     0x048
#define LINUX_OLD_PT_LOADRS_OFF 0x050
#define LINUX_OLD_PT_R1_OFF     0x058
#define LINUX_OLD_PT_R2_OFF     0x060
#define LINUX_OLD_PT_R3_OFF     0x068
#define LINUX_OLD_PT_R12_OFF    0x070
#define LINUX_OLD_PT_R13_OFF    0x078
#define LINUX_OLD_PT_R14_OFF    0x080
#define LINUX_OLD_PT_R15_OFF    0x088
#define LINUX_OLD_PT_R8_OFF     0x090
#define LINUX_OLD_PT_R9_OFF     0x098
#define LINUX_OLD_PT_R10_OFF    0x0a0
#define LINUX_OLD_PT_R11_OFF    0x0a8
#define LINUX_OLD_PT_R16_OFF    0x0b0
#define LINUX_OLD_PT_R17_OFF    0x0b8
#define LINUX_OLD_PT_R18_OFF    0x0c0
#define LINUX_OLD_PT_R19_OFF    0x0c8
#define LINUX_OLD_PT_R20_OFF    0x0d0
#define LINUX_OLD_PT_R21_OFF    0x0d8
#define LINUX_OLD_PT_R22_OFF    0x0e0
#define LINUX_OLD_PT_R23_OFF    0x0e8
#define LINUX_OLD_PT_R24_OFF    0x0f0
#define LINUX_OLD_PT_R25_OFF    0x0f8
#define LINUX_OLD_PT_R26_OFF    0x100
#define LINUX_OLD_PT_R27_OFF    0x108
#define LINUX_OLD_PT_R28_OFF    0x110
#define LINUX_OLD_PT_R29_OFF    0x118
#define LINUX_OLD_PT_R30_OFF    0x120
#define LINUX_OLD_PT_R31_OFF    0x128
#define LINUX_OLD_PT_CCV_OFF    0x130
#define LINUX_OLD_PT_FPSR_OFF   0x138
#define LINUX_OLD_PT_B0_OFF     0x140
#define LINUX_OLD_PT_B7_OFF     0x148
#define LINUX_OLD_PT_F6_OFF     0x150
#define LINUX_OLD_PT_F7_OFF     0x160
#define LINUX_OLD_PT_F8_OFF     0x170
#define LINUX_OLD_PT_F9_OFF     0x180

/* Layout of new Linux kernel interrupt frame (struct pt_regs).  */

#define LINUX_PT_B6_OFF         0
#define LINUX_PT_B7_OFF         8
#define LINUX_PT_CSD_OFF        16
#define LINUX_PT_SSD_OFF        24
#define LINUX_PT_R8_OFF         32
#define LINUX_PT_R9_OFF         40
#define LINUX_PT_R10_OFF        48
#define LINUX_PT_R11_OFF        56
#define LINUX_PT_IPSR_OFF       64
#define LINUX_PT_IIP_OFF        72
#define LINUX_PT_IFS_OFF        80
#define LINUX_PT_UNAT_OFF       88
#define LINUX_PT_PFS_OFF        96
#define LINUX_PT_RSC_OFF        104
#define LINUX_PT_RNAT_OFF       112
#define LINUX_PT_BSPSTORE_OFF   120
#define LINUX_PT_PR_OFF         128
#define LINUX_PT_B0_OFF         136
#define LINUX_PT_LOADRS_OFF     144
#define LINUX_PT_R1_OFF         152
#define LINUX_PT_R12_OFF        160
#define LINUX_PT_R13_OFF        168
#define LINUX_PT_FPSR_OFF       176
#define LINUX_PT_R15_OFF        184
#define LINUX_PT_R14_OFF        192
#define LINUX_PT_R2_OFF         200
#define LINUX_PT_R3_OFF         208
#define LINUX_PT_R16_OFF        216
#define LINUX_PT_R17_OFF        224
#define LINUX_PT_R18_OFF        232
#define LINUX_PT_R19_OFF        240
#define LINUX_PT_R20_OFF        248
#define LINUX_PT_R21_OFF        256
#define LINUX_PT_R22_OFF        264
#define LINUX_PT_R23_OFF        272
#define LINUX_PT_R24_OFF        280
#define LINUX_PT_R25_OFF        288
#define LINUX_PT_R26_OFF        296
#define LINUX_PT_R27_OFF        304
#define LINUX_PT_R28_OFF        312
#define LINUX_PT_R29_OFF        320
#define LINUX_PT_R30_OFF        328
#define LINUX_PT_R31_OFF        336
#define LINUX_PT_CCV_OFF        344
#define LINUX_PT_F6_OFF         352
#define LINUX_PT_F7_OFF         368
#define LINUX_PT_F8_OFF         384
#define LINUX_PT_F9_OFF         400
#define LINUX_PT_F10_OFF        416
#define LINUX_PT_F11_OFF        432

#define LINUX_PT_P_NONSYS       5       /* must match pNonSys in entry.h */
