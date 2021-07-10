/* Machine description for AArch64 architecture.
   Copyright (C) 2009-2018 Free Software Foundation, Inc.
   Contributed by ARM Ltd.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3, or (at your option)
   any later version.

   GCC is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */


#ifndef GCC_AARCH64_H
#define GCC_AARCH64_H

/* Target CPU builtins.  */
#define TARGET_CPU_CPP_BUILTINS()	\
  aarch64_cpu_cpp_builtins (pfile)



#define REGISTER_TARGET_PRAGMAS() aarch64_register_pragmas ()

/* Target machine storage layout.  */

#define PROMOTE_MODE(MODE, UNSIGNEDP, TYPE)	\
  if (GET_MODE_CLASS (MODE) == MODE_INT		\
      && GET_MODE_SIZE (MODE) < 4)		\
    {						\
      if (MODE == QImode || MODE == HImode)	\
	{					\
	  MODE = SImode;			\
	}					\
    }

/* Bits are always numbered from the LSBit.  */
#define BITS_BIG_ENDIAN 0

/* Big/little-endian flavour.  */
#define BYTES_BIG_ENDIAN (TARGET_BIG_END != 0)
#define WORDS_BIG_ENDIAN (BYTES_BIG_ENDIAN)

/* AdvSIMD is supported in the default configuration, unless disabled by
   -mgeneral-regs-only or by the +nosimd extension.  */
#define TARGET_SIMD (!TARGET_GENERAL_REGS_ONLY && AARCH64_ISA_SIMD)
#define TARGET_FLOAT (!TARGET_GENERAL_REGS_ONLY && AARCH64_ISA_FP)

#define UNITS_PER_WORD		8

#define UNITS_PER_VREG		16

#define PARM_BOUNDARY		64

#define STACK_BOUNDARY		128

#define FUNCTION_BOUNDARY	32

#define EMPTY_FIELD_BOUNDARY	32

#define BIGGEST_ALIGNMENT	128

#define SHORT_TYPE_SIZE		16

#define INT_TYPE_SIZE		32

#define LONG_TYPE_SIZE		(TARGET_ILP32 ? 32 : 64)

#define POINTER_SIZE		(TARGET_ILP32 ? 32 : 64)

#define LONG_LONG_TYPE_SIZE	64

#define FLOAT_TYPE_SIZE		32

#define DOUBLE_TYPE_SIZE	64

#define LONG_DOUBLE_TYPE_SIZE	128

/* The architecture reserves all bits of the address for hardware use,
   so the vbit must go into the delta field of pointers to member
   functions.  This is the same config as that in the AArch32
   port.  */
#define TARGET_PTRMEMFUNC_VBIT_LOCATION ptrmemfunc_vbit_in_delta

/* Align definitions of arrays, unions and structures so that
   initializations and copies can be made more efficient.  This is not
   ABI-changing, so it only affects places where we can see the
   definition.  Increasing the alignment tends to introduce padding,
   so don't do this when optimizing for size/conserving stack space.  */
#define AARCH64_EXPAND_ALIGNMENT(COND, EXP, ALIGN)			\
  (((COND) && ((ALIGN) < BITS_PER_WORD)					\
    && (TREE_CODE (EXP) == ARRAY_TYPE					\
	|| TREE_CODE (EXP) == UNION_TYPE				\
	|| TREE_CODE (EXP) == RECORD_TYPE)) ? BITS_PER_WORD : (ALIGN))

/* Align global data.  */
#define DATA_ALIGNMENT(EXP, ALIGN)			\
  AARCH64_EXPAND_ALIGNMENT (!optimize_size, EXP, ALIGN)

/* Similarly, make sure that objects on the stack are sensibly aligned.  */
#define LOCAL_ALIGNMENT(EXP, ALIGN)				\
  AARCH64_EXPAND_ALIGNMENT (!flag_conserve_stack, EXP, ALIGN)

#define STRUCTURE_SIZE_BOUNDARY		8

/* Heap alignment (same as BIGGEST_ALIGNMENT and STACK_BOUNDARY).  */
#define MALLOC_ABI_ALIGNMENT  128

/* Defined by the ABI */
#define WCHAR_TYPE "unsigned int"
#define WCHAR_TYPE_SIZE			32

/* Using long long breaks -ansi and -std=c90, so these will need to be
   made conditional for an LLP64 ABI.  */

#define SIZE_TYPE	"long unsigned int"

#define PTRDIFF_TYPE	"long int"

#define PCC_BITFIELD_TYPE_MATTERS	1

/* Major revision number of the ARM Architecture implemented by the target.  */
extern unsigned aarch64_architecture_version;

/* Instruction tuning/selection flags.  */

/* Bit values used to identify processor capabilities.  */
#define AARCH64_FL_SIMD       (1 << 0)	/* Has SIMD instructions.  */
#define AARCH64_FL_FP         (1 << 1)	/* Has FP.  */
#define AARCH64_FL_CRYPTO     (1 << 2)	/* Has crypto.  */
#define AARCH64_FL_CRC        (1 << 3)	/* Has CRC.  */
/* ARMv8.1-A architecture extensions.  */
#define AARCH64_FL_LSE	      (1 << 4)  /* Has Large System Extensions.  */
#define AARCH64_FL_RDMA       (1 << 5)  /* Has Round Double Multiply Add.  */
#define AARCH64_FL_V8_1       (1 << 6)  /* Has ARMv8.1-A extensions.  */
/* ARMv8.2-A architecture extensions.  */
#define AARCH64_FL_V8_2       (1 << 8)  /* Has ARMv8.2-A features.  */
#define AARCH64_FL_F16	      (1 << 9)  /* Has ARMv8.2-A FP16 extensions.  */
#define AARCH64_FL_SVE        (1 << 10) /* Has Scalable Vector Extensions.  */
/* ARMv8.3-A architecture extensions.  */
#define AARCH64_FL_V8_3       (1 << 11)  /* Has ARMv8.3-A features.  */
#define AARCH64_FL_RCPC       (1 << 12)  /* Has support for RCpc model.  */
#define AARCH64_FL_DOTPROD    (1 << 13)  /* Has ARMv8.2-A Dot Product ins.  */
/* New flags to split crypto into aes and sha2.  */
#define AARCH64_FL_AES	      (1 << 14)  /* Has Crypto AES.  */
#define AARCH64_FL_SHA2	      (1 << 15)  /* Has Crypto SHA2.  */
/* ARMv8.4-A architecture extensions.  */
#define AARCH64_FL_V8_4	      (1 << 16)  /* Has ARMv8.4-A features.  */
#define AARCH64_FL_SM4	      (1 << 17)  /* Has ARMv8.4-A SM3 and SM4.  */
#define AARCH64_FL_SHA3	      (1 << 18)  /* Has ARMv8.4-a SHA3 and SHA512.  */
#define AARCH64_FL_F16FML     (1 << 19)  /* Has ARMv8.4-a FP16 extensions.  */

/* Has FP and SIMD.  */
#define AARCH64_FL_FPSIMD     (AARCH64_FL_FP | AARCH64_FL_SIMD)

/* Has FP without SIMD.  */
#define AARCH64_FL_FPQ16      (AARCH64_FL_FP & ~AARCH64_FL_SIMD)

/* Architecture flags that effect instruction selection.  */
#define AARCH64_FL_FOR_ARCH8       (AARCH64_FL_FPSIMD)
#define AARCH64_FL_FOR_ARCH8_1			       \
  (AARCH64_FL_FOR_ARCH8 | AARCH64_FL_LSE | AARCH64_FL_CRC \
   | AARCH64_FL_RDMA | AARCH64_FL_V8_1)
#define AARCH64_FL_FOR_ARCH8_2			\
  (AARCH64_FL_FOR_ARCH8_1 | AARCH64_FL_V8_2)
#define AARCH64_FL_FOR_ARCH8_3			\
  (AARCH64_FL_FOR_ARCH8_2 | AARCH64_FL_V8_3)
#define AARCH64_FL_FOR_ARCH8_4			\
  (AARCH64_FL_FOR_ARCH8_3 | AARCH64_FL_V8_4 | AARCH64_FL_F16FML \
   | AARCH64_FL_DOTPROD)

/* Macros to test ISA flags.  */

#define AARCH64_ISA_CRC            (aarch64_isa_flags & AARCH64_FL_CRC)
#define AARCH64_ISA_CRYPTO         (aarch64_isa_flags & AARCH64_FL_CRYPTO)
#define AARCH64_ISA_FP             (aarch64_isa_flags & AARCH64_FL_FP)
#define AARCH64_ISA_SIMD           (aarch64_isa_flags & AARCH64_FL_SIMD)
#define AARCH64_ISA_LSE		   (aarch64_isa_flags & AARCH64_FL_LSE)
#define AARCH64_ISA_RDMA	   (aarch64_isa_flags & AARCH64_FL_RDMA)
#define AARCH64_ISA_V8_2	   (aarch64_isa_flags & AARCH64_FL_V8_2)
#define AARCH64_ISA_F16		   (aarch64_isa_flags & AARCH64_FL_F16)
#define AARCH64_ISA_SVE            (aarch64_isa_flags & AARCH64_FL_SVE)
#define AARCH64_ISA_V8_3	   (aarch64_isa_flags & AARCH64_FL_V8_3)
#define AARCH64_ISA_DOTPROD	   (aarch64_isa_flags & AARCH64_FL_DOTPROD)
#define AARCH64_ISA_AES	           (aarch64_isa_flags & AARCH64_FL_AES)
#define AARCH64_ISA_SHA2	   (aarch64_isa_flags & AARCH64_FL_SHA2)
#define AARCH64_ISA_V8_4	   (aarch64_isa_flags & AARCH64_FL_V8_4)
#define AARCH64_ISA_SM4	           (aarch64_isa_flags & AARCH64_FL_SM4)
#define AARCH64_ISA_SHA3	   (aarch64_isa_flags & AARCH64_FL_SHA3)
#define AARCH64_ISA_F16FML	   (aarch64_isa_flags & AARCH64_FL_F16FML)

/* Crypto is an optional extension to AdvSIMD.  */
#define TARGET_CRYPTO (TARGET_SIMD && AARCH64_ISA_CRYPTO)

/* SHA2 is an optional extension to AdvSIMD.  */
#define TARGET_SHA2 ((TARGET_SIMD && AARCH64_ISA_SHA2) || TARGET_CRYPTO)

/* SHA3 is an optional extension to AdvSIMD.  */
#define TARGET_SHA3 (TARGET_SIMD && AARCH64_ISA_SHA3)

/* AES is an optional extension to AdvSIMD.  */
#define TARGET_AES ((TARGET_SIMD && AARCH64_ISA_AES) || TARGET_CRYPTO)

/* SM is an optional extension to AdvSIMD.  */
#define TARGET_SM4 (TARGET_SIMD && AARCH64_ISA_SM4)

/* FP16FML is an optional extension to AdvSIMD.  */
#define TARGET_F16FML (TARGET_SIMD && AARCH64_ISA_F16FML && TARGET_FP_F16INST)

/* CRC instructions that can be enabled through +crc arch extension.  */
#define TARGET_CRC32 (AARCH64_ISA_CRC)

/* Atomic instructions that can be enabled through the +lse extension.  */
#define TARGET_LSE (AARCH64_ISA_LSE)

/* ARMv8.2-A FP16 support that can be enabled through the +fp16 extension.  */
#define TARGET_FP_F16INST (TARGET_FLOAT && AARCH64_ISA_F16)
#define TARGET_SIMD_F16INST (TARGET_SIMD && AARCH64_ISA_F16)

/* Dot Product is an optional extension to AdvSIMD enabled through +dotprod.  */
#define TARGET_DOTPROD (TARGET_SIMD && AARCH64_ISA_DOTPROD)

/* SVE instructions, enabled through +sve.  */
#define TARGET_SVE (AARCH64_ISA_SVE)

/* ARMv8.3-A features.  */
#define TARGET_ARMV8_3	(AARCH64_ISA_V8_3)

/* Make sure this is always defined so we don't have to check for ifdefs
   but rather use normal ifs.  */
#ifndef TARGET_FIX_ERR_A53_835769_DEFAULT
#define TARGET_FIX_ERR_A53_835769_DEFAULT 0
#else
#undef TARGET_FIX_ERR_A53_835769_DEFAULT
#define TARGET_FIX_ERR_A53_835769_DEFAULT 1
#endif

/* Apply the workaround for Cortex-A53 erratum 835769.  */
#define TARGET_FIX_ERR_A53_835769	\
  ((aarch64_fix_a53_err835769 == 2)	\
  ? TARGET_FIX_ERR_A53_835769_DEFAULT : aarch64_fix_a53_err835769)

/* Make sure this is always defined so we don't have to check for ifdefs
   but rather use normal ifs.  */
#ifndef TARGET_FIX_ERR_A53_843419_DEFAULT
#define TARGET_FIX_ERR_A53_843419_DEFAULT 0
#else
#undef TARGET_FIX_ERR_A53_843419_DEFAULT
#define TARGET_FIX_ERR_A53_843419_DEFAULT 1
#endif

/* Apply the workaround for Cortex-A53 erratum 843419.  */
#define TARGET_FIX_ERR_A53_843419	\
  ((aarch64_fix_a53_err843419 == 2)	\
  ? TARGET_FIX_ERR_A53_843419_DEFAULT : aarch64_fix_a53_err843419)

/* ARMv8.1-A Adv.SIMD support.  */
#define TARGET_SIMD_RDMA (TARGET_SIMD && AARCH64_ISA_RDMA)

/* Standard register usage.  */

/* 31 64-bit general purpose registers R0-R30:
   R30		LR (link register)
   R29		FP (frame pointer)
   R19-R28	Callee-saved registers
   R18		The platform register; use as temporary register.
   R17		IP1 The second intra-procedure-call temporary register
		(can be used by call veneers and PLT code); otherwise use
		as a temporary register
   R16		IP0 The first intra-procedure-call temporary register (can
		be used by call veneers and PLT code); otherwise use as a
		temporary register
   R9-R15	Temporary registers
   R8		Structure value parameter / temporary register
   R0-R7	Parameter/result registers

   SP		stack pointer, encoded as X/R31 where permitted.
   ZR		zero register, encoded as X/R31 elsewhere

   32 x 128-bit floating-point/vector registers
   V16-V31	Caller-saved (temporary) registers
   V8-V15	Callee-saved registers
   V0-V7	Parameter/result registers

   The vector register V0 holds scalar B0, H0, S0 and D0 in its least
   significant bits.  Unlike AArch32 S1 is not packed into D0, etc.

   P0-P7        Predicate low registers: valid in all predicate contexts
   P8-P15       Predicate high registers: used as scratch space

   VG           Pseudo "vector granules" register

   VG is the number of 64-bit elements in an SVE vector.  We define
   it as a hard register so that we can easily map it to the DWARF VG
   register.  GCC internally uses the poly_int variable aarch64_sve_vg
   instead.  */

/* Note that we don't mark X30 as a call-clobbered register.  The idea is
   that it's really the call instructions themselves which clobber X30.
   We don't care what the called function does with it afterwards.

   This approach makes it easier to implement sibcalls.  Unlike normal
   calls, sibcalls don't clobber X30, so the register reaches the
   called function intact.  EPILOGUE_USES says that X30 is useful
   to the called function.  */

#define FIXED_REGISTERS					\
  {							\
    0, 0, 0, 0,   0, 0, 0, 0,	/* R0 - R7 */		\
    0, 0, 0, 0,   0, 0, 0, 0,	/* R8 - R15 */		\
    0, 0, 0, 0,   0, 0, 0, 0,	/* R16 - R23 */		\
    0, 0, 0, 0,   0, 1, 0, 1,	/* R24 - R30, SP */	\
    0, 0, 0, 0,   0, 0, 0, 0,   /* V0 - V7 */           \
    0, 0, 0, 0,   0, 0, 0, 0,   /* V8 - V15 */		\
    0, 0, 0, 0,   0, 0, 0, 0,   /* V16 - V23 */         \
    0, 0, 0, 0,   0, 0, 0, 0,   /* V24 - V31 */         \
    1, 1, 1, 1,			/* SFP, AP, CC, VG */	\
    0, 0, 0, 0,   0, 0, 0, 0,   /* P0 - P7 */           \
    0, 0, 0, 0,   0, 0, 0, 0,   /* P8 - P15 */          \
  }

#define CALL_USED_REGISTERS				\
  {							\
    1, 1, 1, 1,   1, 1, 1, 1,	/* R0 - R7 */		\
    1, 1, 1, 1,   1, 1, 1, 1,	/* R8 - R15 */		\
    1, 1, 1, 0,   0, 0, 0, 0,	/* R16 - R23 */		\
    0, 0, 0, 0,   0, 1, 1, 1,	/* R24 - R30, SP */	\
    1, 1, 1, 1,   1, 1, 1, 1,	/* V0 - V7 */		\
    0, 0, 0, 0,   0, 0, 0, 0,	/* V8 - V15 */		\
    1, 1, 1, 1,   1, 1, 1, 1,   /* V16 - V23 */         \
    1, 1, 1, 1,   1, 1, 1, 1,   /* V24 - V31 */         \
    1, 1, 1, 1,			/* SFP, AP, CC, VG */	\
    1, 1, 1, 1,   1, 1, 1, 1,	/* P0 - P7 */		\
    1, 1, 1, 1,   1, 1, 1, 1,	/* P8 - P15 */		\
  }

#define REGISTER_NAMES						\
  {								\
    "x0",  "x1",  "x2",  "x3",  "x4",  "x5",  "x6",  "x7",	\
    "x8",  "x9",  "x10", "x11", "x12", "x13", "x14", "x15",	\
    "x16", "x17", "x18", "x19", "x20", "x21", "x22", "x23",	\
    "x24", "x25", "x26", "x27", "x28", "x29", "x30", "sp",	\
    "v0",  "v1",  "v2",  "v3",  "v4",  "v5",  "v6",  "v7",	\
    "v8",  "v9",  "v10", "v11", "v12", "v13", "v14", "v15",	\
    "v16", "v17", "v18", "v19", "v20", "v21", "v22", "v23",	\
    "v24", "v25", "v26", "v27", "v28", "v29", "v30", "v31",	\
    "sfp", "ap",  "cc",  "vg",					\
    "p0",  "p1",  "p2",  "p3",  "p4",  "p5",  "p6",  "p7",	\
    "p8",  "p9",  "p10", "p11", "p12", "p13", "p14", "p15",	\
  }

/* Generate the register aliases for core register N */
#define R_ALIASES(N) {"r" # N, R0_REGNUM + (N)}, \
                     {"w" # N, R0_REGNUM + (N)}

#define V_ALIASES(N) {"q" # N, V0_REGNUM + (N)}, \
                     {"d" # N, V0_REGNUM + (N)}, \
                     {"s" # N, V0_REGNUM + (N)}, \
                     {"h" # N, V0_REGNUM + (N)}, \
                     {"b" # N, V0_REGNUM + (N)}, \
                     {"z" # N, V0_REGNUM + (N)}

/* Provide aliases for all of the ISA defined register name forms.
   These aliases are convenient for use in the clobber lists of inline
   asm statements.  */

#define ADDITIONAL_REGISTER_NAMES \
  { R_ALIASES(0),  R_ALIASES(1),  R_ALIASES(2),  R_ALIASES(3),  \
    R_ALIASES(4),  R_ALIASES(5),  R_ALIASES(6),  R_ALIASES(7),  \
    R_ALIASES(8),  R_ALIASES(9),  R_ALIASES(10), R_ALIASES(11), \
    R_ALIASES(12), R_ALIASES(13), R_ALIASES(14), R_ALIASES(15), \
    R_ALIASES(16), R_ALIASES(17), R_ALIASES(18), R_ALIASES(19), \
    R_ALIASES(20), R_ALIASES(21), R_ALIASES(22), R_ALIASES(23), \
    R_ALIASES(24), R_ALIASES(25), R_ALIASES(26), R_ALIASES(27), \
    R_ALIASES(28), R_ALIASES(29), R_ALIASES(30), {"wsp", R0_REGNUM + 31}, \
    V_ALIASES(0),  V_ALIASES(1),  V_ALIASES(2),  V_ALIASES(3),  \
    V_ALIASES(4),  V_ALIASES(5),  V_ALIASES(6),  V_ALIASES(7),  \
    V_ALIASES(8),  V_ALIASES(9),  V_ALIASES(10), V_ALIASES(11), \
    V_ALIASES(12), V_ALIASES(13), V_ALIASES(14), V_ALIASES(15), \
    V_ALIASES(16), V_ALIASES(17), V_ALIASES(18), V_ALIASES(19), \
    V_ALIASES(20), V_ALIASES(21), V_ALIASES(22), V_ALIASES(23), \
    V_ALIASES(24), V_ALIASES(25), V_ALIASES(26), V_ALIASES(27), \
    V_ALIASES(28), V_ALIASES(29), V_ALIASES(30), V_ALIASES(31)  \
  }

/* Say that the epilogue uses the return address register.  Note that
   in the case of sibcalls, the values "used by the epilogue" are
   considered live at the start of the called function.  */

#define EPILOGUE_USES(REGNO) \
  (epilogue_completed && (REGNO) == LR_REGNUM)

/* EXIT_IGNORE_STACK should be nonzero if, when returning from a function,
   the stack pointer does not matter.  This is only true if the function
   uses alloca.  */
#define EXIT_IGNORE_STACK	(cfun->calls_alloca)

#define STATIC_CHAIN_REGNUM		R18_REGNUM
#define HARD_FRAME_POINTER_REGNUM	R29_REGNUM
#define FRAME_POINTER_REGNUM		SFP_REGNUM
#define STACK_POINTER_REGNUM		SP_REGNUM
#define ARG_POINTER_REGNUM		AP_REGNUM
#define FIRST_PSEUDO_REGISTER		(P15_REGNUM + 1)

/* The number of (integer) argument register available.  */
#define NUM_ARG_REGS			8
#define NUM_FP_ARG_REGS			8

/* A Homogeneous Floating-Point or Short-Vector Aggregate may have at most
   four members.  */
#define HA_MAX_NUM_FLDS		4

/* External dwarf register number scheme.  These number are used to
   identify registers in dwarf debug information, the values are
   defined by the AArch64 ABI.  The numbering scheme is independent of
   GCC's internal register numbering scheme.  */

#define AARCH64_DWARF_R0        0

/* The number of R registers, note 31! not 32.  */
#define AARCH64_DWARF_NUMBER_R 31

#define AARCH64_DWARF_SP       31
#define AARCH64_DWARF_VG       46
#define AARCH64_DWARF_P0       48
#define AARCH64_DWARF_V0       64

/* The number of V registers.  */
#define AARCH64_DWARF_NUMBER_V 32

/* For signal frames we need to use an alternative return column.  This
   value must not correspond to a hard register and must be out of the
   range of DWARF_FRAME_REGNUM().  */
#define DWARF_ALT_FRAME_RETURN_COLUMN   \
  (AARCH64_DWARF_V0 + AARCH64_DWARF_NUMBER_V)

/* We add 1 extra frame register for use as the
   DWARF_ALT_FRAME_RETURN_COLUMN.  */
#define DWARF_FRAME_REGISTERS           (DWARF_ALT_FRAME_RETURN_COLUMN + 1)


#define DBX_REGISTER_NUMBER(REGNO)	aarch64_dbx_register_number (REGNO)
/* Provide a definition of DWARF_FRAME_REGNUM here so that fallback unwinders
   can use DWARF_ALT_FRAME_RETURN_COLUMN defined below.  This is just the same
   as the default definition in dwarf2out.c.  */
#undef DWARF_FRAME_REGNUM
#define DWARF_FRAME_REGNUM(REGNO)	DBX_REGISTER_NUMBER (REGNO)

#define DWARF_FRAME_RETURN_COLUMN	DWARF_FRAME_REGNUM (LR_REGNUM)

#define DWARF2_UNWIND_INFO 1

/* Use R0 through R3 to pass exception handling information.  */
#define EH_RETURN_DATA_REGNO(N) \
  ((N) < 4 ? ((unsigned int) R0_REGNUM + (N)) : INVALID_REGNUM)

/* Select a format to encode pointers in exception handling data.  */
#define ASM_PREFERRED_EH_DATA_FORMAT(CODE, GLOBAL) \
  aarch64_asm_preferred_eh_data_format ((CODE), (GLOBAL))

/* Output the assembly strings we want to add to a function definition.  */
#define ASM_DECLARE_FUNCTION_NAME(STR, NAME, DECL)	\
  aarch64_declare_function_name (STR, NAME, DECL)

/* For EH returns X4 contains the stack adjustment.  */
#define EH_RETURN_STACKADJ_RTX	gen_rtx_REG (Pmode, R4_REGNUM)
#define EH_RETURN_HANDLER_RTX  aarch64_eh_return_handler_rtx ()

/* Don't use __builtin_setjmp until we've defined it.  */
#undef DONT_USE_BUILTIN_SETJMP
#define DONT_USE_BUILTIN_SETJMP 1

/* Register in which the structure value is to be returned.  */
#define AARCH64_STRUCT_VALUE_REGNUM R8_REGNUM

/* Non-zero if REGNO is part of the Core register set.

   The rather unusual way of expressing this check is to avoid
   warnings when building the compiler when R0_REGNUM is 0 and REGNO
   is unsigned.  */
#define GP_REGNUM_P(REGNO)						\
  (((unsigned) (REGNO - R0_REGNUM)) <= (R30_REGNUM - R0_REGNUM))

#define FP_REGNUM_P(REGNO)			\
  (((unsigned) (REGNO - V0_REGNUM)) <= (V31_REGNUM - V0_REGNUM))

#define FP_LO_REGNUM_P(REGNO)            \
  (((unsigned) (REGNO - V0_REGNUM)) <= (V15_REGNUM - V0_REGNUM))

#define PR_REGNUM_P(REGNO)\
  (((unsigned) (REGNO - P0_REGNUM)) <= (P15_REGNUM - P0_REGNUM))

#define PR_LO_REGNUM_P(REGNO)\
  (((unsigned) (REGNO - P0_REGNUM)) <= (P7_REGNUM - P0_REGNUM))


/* Register and constant classes.  */

enum reg_class
{
  NO_REGS,
  TAILCALL_ADDR_REGS,
  GENERAL_REGS,
  STACK_REG,
  POINTER_REGS,
  FP_LO_REGS,
  FP_REGS,
  POINTER_AND_FP_REGS,
  PR_LO_REGS,
  PR_HI_REGS,
  PR_REGS,
  ALL_REGS,
  LIM_REG_CLASSES		/* Last */
};

#define N_REG_CLASSES	((int) LIM_REG_CLASSES)

#define REG_CLASS_NAMES				\
{						\
  "NO_REGS",					\
  "TAILCALL_ADDR_REGS",				\
  "GENERAL_REGS",				\
  "STACK_REG",					\
  "POINTER_REGS",				\
  "FP_LO_REGS",					\
  "FP_REGS",					\
  "POINTER_AND_FP_REGS",			\
  "PR_LO_REGS",					\
  "PR_HI_REGS",					\
  "PR_REGS",					\
  "ALL_REGS"					\
}

#define REG_CLASS_CONTENTS						\
{									\
  { 0x00000000, 0x00000000, 0x00000000 },	/* NO_REGS */		\
  { 0x0004ffff, 0x00000000, 0x00000000 },	/* TAILCALL_ADDR_REGS */\
  { 0x7fffffff, 0x00000000, 0x00000003 },	/* GENERAL_REGS */	\
  { 0x80000000, 0x00000000, 0x00000000 },	/* STACK_REG */		\
  { 0xffffffff, 0x00000000, 0x00000003 },	/* POINTER_REGS */	\
  { 0x00000000, 0x0000ffff, 0x00000000 },       /* FP_LO_REGS  */	\
  { 0x00000000, 0xffffffff, 0x00000000 },       /* FP_REGS  */		\
  { 0xffffffff, 0xffffffff, 0x00000003 },	/* POINTER_AND_FP_REGS */\
  { 0x00000000, 0x00000000, 0x00000ff0 },	/* PR_LO_REGS */	\
  { 0x00000000, 0x00000000, 0x000ff000 },	/* PR_HI_REGS */	\
  { 0x00000000, 0x00000000, 0x000ffff0 },	/* PR_REGS */		\
  { 0xffffffff, 0xffffffff, 0x000fffff }	/* ALL_REGS */		\
}

#define REGNO_REG_CLASS(REGNO)	aarch64_regno_regclass (REGNO)

#define INDEX_REG_CLASS	GENERAL_REGS
#define BASE_REG_CLASS  POINTER_REGS

/* Register pairs used to eliminate unneeded registers that point into
   the stack frame.  */
#define ELIMINABLE_REGS							\
{									\
  { ARG_POINTER_REGNUM,		STACK_POINTER_REGNUM		},	\
  { ARG_POINTER_REGNUM,		HARD_FRAME_POINTER_REGNUM	},	\
  { FRAME_POINTER_REGNUM,	STACK_POINTER_REGNUM		},	\
  { FRAME_POINTER_REGNUM,	HARD_FRAME_POINTER_REGNUM	},	\
}

#define INITIAL_ELIMINATION_OFFSET(FROM, TO, OFFSET) \
  (OFFSET) = aarch64_initial_elimination_offset (FROM, TO)

/* CPU/ARCH option handling.  */
#include "config/aarch64/aarch64-opts.h"

enum target_cpus
{
#define AARCH64_CORE(NAME, INTERNAL_IDENT, SCHED, ARCH, FLAGS, COSTS, IMP, PART, VARIANT) \
  TARGET_CPU_##INTERNAL_IDENT,
#include "aarch64-cores.def"
  TARGET_CPU_generic
};

/* If there is no CPU defined at configure, use generic as default.  */
#ifndef TARGET_CPU_DEFAULT
#define TARGET_CPU_DEFAULT \
  (TARGET_CPU_generic | (AARCH64_CPU_DEFAULT_FLAGS << 6))
#endif

/* If inserting NOP before a mult-accumulate insn remember to adjust the
   length so that conditional branching code is updated appropriately.  */
#define ADJUST_INSN_LENGTH(insn, length)	\
  do						\
    {						\
       if (aarch64_madd_needs_nop (insn))	\
         length += 4;				\
    } while (0)

#define FINAL_PRESCAN_INSN(INSN, OPVEC, NOPERANDS)	\
    aarch64_final_prescan_insn (INSN);			\

/* The processor for which instructions should be scheduled.  */
extern enum aarch64_processor aarch64_tune;

/* RTL generation support.  */
#define INIT_EXPANDERS aarch64_init_expanders ()


/* Stack layout; function entry, exit and calling.  */
#define STACK_GROWS_DOWNWARD	1

#define FRAME_GROWS_DOWNWARD	1

#define ACCUMULATE_OUTGOING_ARGS	1

#define FIRST_PARM_OFFSET(FNDECL) 0

/* Fix for VFP */
#define LIBCALL_VALUE(MODE)  \
  gen_rtx_REG (MODE, FLOAT_MODE_P (MODE) ? V0_REGNUM : R0_REGNUM)

#define DEFAULT_PCC_STRUCT_RETURN 0

#ifdef HAVE_POLY_INT_H
struct GTY (()) aarch64_frame
{
  HOST_WIDE_INT reg_offset[FIRST_PSEUDO_REGISTER];

  /* The number of extra stack bytes taken up by register varargs.
     This area is allocated by the callee at the very top of the
     frame.  This value is rounded up to a multiple of
     STACK_BOUNDARY.  */
  HOST_WIDE_INT saved_varargs_size;

  /* The size of the saved callee-save int/FP registers.  */

  HOST_WIDE_INT saved_regs_size;

  /* Offset from the base of the frame (incomming SP) to the
     top of the locals area.  This value is always a multiple of
     STACK_BOUNDARY.  */
  poly_int64 locals_offset;

  /* Offset from the base of the frame (incomming SP) to the
     hard_frame_pointer.  This value is always a multiple of
     STACK_BOUNDARY.  */
  poly_int64 hard_fp_offset;

  /* The size of the frame.  This value is the offset from base of the
     frame (incomming SP) to the stack_pointer.  This value is always
     a multiple of STACK_BOUNDARY.  */
  poly_int64 frame_size;

  /* The size of the initial stack adjustment before saving callee-saves.  */
  poly_int64 initial_adjust;

  /* The writeback value when pushing callee-save registers.
     It is zero when no push is used.  */
  HOST_WIDE_INT callee_adjust;

  /* The offset from SP to the callee-save registers after initial_adjust.
     It may be non-zero if no push is used (ie. callee_adjust == 0).  */
  poly_int64 callee_offset;

  /* The size of the stack adjustment after saving callee-saves.  */
  poly_int64 final_adjust;

  /* Store FP,LR and setup a frame pointer.  */
  bool emit_frame_chain;

  unsigned wb_candidate1;
  unsigned wb_candidate2;

  bool laid_out;
};

typedef struct GTY (()) machine_function
{
  struct aarch64_frame frame;
  /* One entry for each hard register.  */
  bool reg_is_wrapped_separately[LAST_SAVED_REGNUM];
} machine_function;
#endif

/* Which ABI to use.  */
enum aarch64_abi_type
{
  AARCH64_ABI_LP64 = 0,
  AARCH64_ABI_ILP32 = 1
};

#ifndef AARCH64_ABI_DEFAULT
#define AARCH64_ABI_DEFAULT AARCH64_ABI_LP64
#endif

#define TARGET_ILP32	(aarch64_abi & AARCH64_ABI_ILP32)

enum arm_pcs
{
  ARM_PCS_AAPCS64,		/* Base standard AAPCS for 64 bit.  */
  ARM_PCS_UNKNOWN
};




/* We can't use machine_mode inside a generator file because it
   hasn't been created yet; we shouldn't be using any code that
   needs the real definition though, so this ought to be safe.  */
#ifdef GENERATOR_FILE
#define MACHMODE int
#else
#include "insn-modes.h"
#define MACHMODE machine_mode
#endif

#ifndef USED_FOR_TARGET
/* AAPCS related state tracking.  */
typedef struct
{
  enum arm_pcs pcs_variant;
  int aapcs_arg_processed;	/* No need to lay out this argument again.  */
  int aapcs_ncrn;		/* Next Core register number.  */
  int aapcs_nextncrn;		/* Next next core register number.  */
  int aapcs_nvrn;		/* Next Vector register number.  */
  int aapcs_nextnvrn;		/* Next Next Vector register number.  */
  rtx aapcs_reg;		/* Register assigned to this argument.  This
				   is NULL_RTX if this parameter goes on
				   the stack.  */
  MACHMODE aapcs_vfp_rmode;
  int aapcs_stack_words;	/* If the argument is passed on the stack, this
				   is the number of words needed, after rounding
				   up.  Only meaningful when
				   aapcs_reg == NULL_RTX.  */
  int aapcs_stack_size;		/* The total size (in words, per 8 byte) of the
				   stack arg area so far.  */
} CUMULATIVE_ARGS;
#endif

#define BLOCK_REG_PADDING(MODE, TYPE, FIRST) \
  (aarch64_pad_reg_upward (MODE, TYPE, FIRST) ? PAD_UPWARD : PAD_DOWNWARD)

#define PAD_VARARGS_DOWN	0

#define INIT_CUMULATIVE_ARGS(CUM, FNTYPE, LIBNAME, FNDECL, N_NAMED_ARGS) \
  aarch64_init_cumulative_args (&(CUM), FNTYPE, LIBNAME, FNDECL, N_NAMED_ARGS)

#define FUNCTION_ARG_REGNO_P(REGNO) \
  aarch64_function_arg_regno_p(REGNO)


/* ISA Features.  */

/* Addressing modes, etc.  */
#define HAVE_POST_INCREMENT	1
#define HAVE_PRE_INCREMENT	1
#define HAVE_POST_DECREMENT	1
#define HAVE_PRE_DECREMENT	1
#define HAVE_POST_MODIFY_DISP	1
#define HAVE_PRE_MODIFY_DISP	1

#define MAX_REGS_PER_ADDRESS	2

#define CONSTANT_ADDRESS_P(X)		aarch64_constant_address_p(X)

#define REGNO_OK_FOR_BASE_P(REGNO)	\
  aarch64_regno_ok_for_base_p (REGNO, true)

#define REGNO_OK_FOR_INDEX_P(REGNO) \
  aarch64_regno_ok_for_index_p (REGNO, true)

#define LEGITIMATE_PIC_OPERAND_P(X) \
  aarch64_legitimate_pic_operand_p (X)

#define CASE_VECTOR_MODE Pmode

#define DEFAULT_SIGNED_CHAR 0

/* An integer expression for the size in bits of the largest integer machine
   mode that should actually be used.  We allow pairs of registers.  */
#define MAX_FIXED_MODE_SIZE GET_MODE_BITSIZE (TImode)

/* Maximum bytes moved by a single instruction (load/store pair).  */
#define MOVE_MAX (UNITS_PER_WORD * 2)

/* The base cost overhead of a memcpy call, for MOVE_RATIO and friends.  */
#define AARCH64_CALL_RATIO 8

/* MOVE_RATIO dictates when we will use the move_by_pieces infrastructure.
   move_by_pieces will continually copy the largest safe chunks.  So a
   7-byte copy is a 4-byte + 2-byte + byte copy.  This proves inefficient
   for both size and speed of copy, so we will instead use the "movmem"
   standard name to implement the copy.  This logic does not apply when
   targeting -mstrict-align, so keep a sensible default in that case.  */
#define MOVE_RATIO(speed) \
  (!STRICT_ALIGNMENT ? 2 : (((speed) ? 15 : AARCH64_CALL_RATIO) / 2))

/* For CLEAR_RATIO, when optimizing for size, give a better estimate
   of the length of a memset call, but use the default otherwise.  */
#define CLEAR_RATIO(speed) \
  ((speed) ? 15 : AARCH64_CALL_RATIO)

/* SET_RATIO is similar to CLEAR_RATIO, but for a non-zero constant, so when
   optimizing for size adjust the ratio to account for the overhead of loading
   the constant.  */
#define SET_RATIO(speed) \
  ((speed) ? 15 : AARCH64_CALL_RATIO - 2)

/* Disable auto-increment in move_by_pieces et al.  Use of auto-increment is
   rarely a good idea in straight-line code since it adds an extra address
   dependency between each instruction.  Better to use incrementing offsets.  */
#define USE_LOAD_POST_INCREMENT(MODE)   0
#define USE_LOAD_POST_DECREMENT(MODE)   0
#define USE_LOAD_PRE_INCREMENT(MODE)    0
#define USE_LOAD_PRE_DECREMENT(MODE)    0
#define USE_STORE_POST_INCREMENT(MODE)  0
#define USE_STORE_POST_DECREMENT(MODE)  0
#define USE_STORE_PRE_INCREMENT(MODE)   0
#define USE_STORE_PRE_DECREMENT(MODE)   0

/* WORD_REGISTER_OPERATIONS does not hold for AArch64.
   The assigned word_mode is DImode but operations narrower than SImode
   behave as 32-bit operations if using the W-form of the registers rather
   than as word_mode (64-bit) operations as WORD_REGISTER_OPERATIONS
   expects.  */
#define WORD_REGISTER_OPERATIONS 0

/* Define if loading from memory in MODE, an integral mode narrower than
   BITS_PER_WORD will either zero-extend or sign-extend.  The value of this
   macro should be the code that says which one of the two operations is
   implicitly done, or UNKNOWN if none.  */
#define LOAD_EXTEND_OP(MODE) ZERO_EXTEND

/* Define this macro to be non-zero if instructions will fail to work
   if given data not on the nominal alignment.  */
#define STRICT_ALIGNMENT		TARGET_STRICT_ALIGN

/* Define this macro to be non-zero if accessing less than a word of
   memory is no faster than accessing a word of memory, i.e., if such
   accesses require more than one instruction or if there is no
   difference in cost.
   Although there's no difference in instruction count or cycles,
   in AArch64 we don't want to expand to a sub-word to a 64-bit access
   if we don't have to, for power-saving reasons.  */
#define SLOW_BYTE_ACCESS		0

#define NO_FUNCTION_CSE	1

/* Specify the machine mode that the hardware addresses have.
   After generation of rtl, the compiler makes no further distinction
   between pointers and any other objects of this machine mode.  */
#define Pmode		DImode

/* A C expression whose value is zero if pointers that need to be extended
   from being `POINTER_SIZE' bits wide to `Pmode' are sign-extended and
   greater then zero if they are zero-extended and less then zero if the
   ptr_extend instruction should be used.  */
#define POINTERS_EXTEND_UNSIGNED 1

/* Mode of a function address in a call instruction (for indexing purposes).  */
#define FUNCTION_MODE	Pmode

#define SELECT_CC_MODE(OP, X, Y)	aarch64_select_cc_mode (OP, X, Y)

#define REVERSIBLE_CC_MODE(MODE) 1

#define REVERSE_CONDITION(CODE, MODE)		\
  (((MODE) == CCFPmode || (MODE) == CCFPEmode)	\
   ? reverse_condition_maybe_unordered (CODE)	\
   : reverse_condition (CODE))

#define CLZ_DEFINED_VALUE_AT_ZERO(MODE, VALUE) \
  ((VALUE) = GET_MODE_UNIT_BITSIZE (MODE), 2)
#define CTZ_DEFINED_VALUE_AT_ZERO(MODE, VALUE) \
  ((VALUE) = GET_MODE_UNIT_BITSIZE (MODE), 2)

#define INCOMING_RETURN_ADDR_RTX gen_rtx_REG (Pmode, LR_REGNUM)

#define RETURN_ADDR_RTX aarch64_return_addr

/* 3 insns + padding + 2 pointer-sized entries.  */
#define TRAMPOLINE_SIZE	(TARGET_ILP32 ? 24 : 32)

/* Trampolines contain dwords, so must be dword aligned.  */
#define TRAMPOLINE_ALIGNMENT 64

/* Put trampolines in the text section so that mapping symbols work
   correctly.  */
#define TRAMPOLINE_SECTION text_section

/* To start with.  */
#define BRANCH_COST(SPEED_P, PREDICTABLE_P) \
  (aarch64_branch_cost (SPEED_P, PREDICTABLE_P))


/* Assembly output.  */

/* For now we'll make all jump tables pc-relative.  */
#define CASE_VECTOR_PC_RELATIVE	1

#define CASE_VECTOR_SHORTEN_MODE(min, max, body)	\
  ((min < -0x1fff0 || max > 0x1fff0) ? SImode		\
   : (min < -0x1f0 || max > 0x1f0) ? HImode		\
   : QImode)

/* Jump table alignment is explicit in ASM_OUTPUT_CASE_LABEL.  */
#define ADDR_VEC_ALIGN(JUMPTABLE) 0

#define MCOUNT_NAME "_mcount"

#define NO_PROFILE_COUNTERS 1

/* Emit rtl for profiling.  Output assembler code to FILE
   to call "_mcount" for profiling a function entry.  */
#define PROFILE_HOOK(LABEL)						\
  {									\
    rtx fun, lr;							\
    lr = get_hard_reg_initial_val (Pmode, LR_REGNUM);			\
    fun = gen_rtx_SYMBOL_REF (Pmode, MCOUNT_NAME);			\
    emit_library_call (fun, LCT_NORMAL, VOIDmode, lr, Pmode);		\
  }

/* All the work done in PROFILE_HOOK, but still required.  */
#define FUNCTION_PROFILER(STREAM, LABELNO) do { } while (0)

/* For some reason, the Linux headers think they know how to define
   these macros.  They don't!!!  */
#undef ASM_APP_ON
#undef ASM_APP_OFF
#define ASM_APP_ON	"\t" ASM_COMMENT_START " Start of user assembly\n"
#define ASM_APP_OFF	"\t" ASM_COMMENT_START " End of user assembly\n"

#define CONSTANT_POOL_BEFORE_FUNCTION 0

/* This definition should be relocated to aarch64-elf-raw.h.  This macro
   should be undefined in aarch64-linux.h and a clear_cache pattern
   implmented to emit either the call to __aarch64_sync_cache_range()
   directly or preferably the appropriate sycall or cache clear
   instructions inline.  */
#define CLEAR_INSN_CACHE(beg, end)				\
  extern void  __aarch64_sync_cache_range (void *, void *);	\
  __aarch64_sync_cache_range (beg, end)

#define SHIFT_COUNT_TRUNCATED (!TARGET_SIMD)

/* Choose appropriate mode for caller saves, so we do the minimum
   required size of load/store.  */
#define HARD_REGNO_CALLER_SAVE_MODE(REGNO, NREGS, MODE) \
  aarch64_hard_regno_caller_save_mode ((REGNO), (NREGS), (MODE))

#undef SWITCHABLE_TARGET
#define SWITCHABLE_TARGET 1

/* Check TLS Descriptors mechanism is selected.  */
#define TARGET_TLS_DESC (aarch64_tls_dialect == TLS_DESCRIPTORS)

extern enum aarch64_code_model aarch64_cmodel;

/* When using the tiny addressing model conditional and unconditional branches
   can span the whole of the available address space (1MB).  */
#define HAS_LONG_COND_BRANCH				\
  (aarch64_cmodel == AARCH64_CMODEL_TINY		\
   || aarch64_cmodel == AARCH64_CMODEL_TINY_PIC)

#define HAS_LONG_UNCOND_BRANCH				\
  (aarch64_cmodel == AARCH64_CMODEL_TINY		\
   || aarch64_cmodel == AARCH64_CMODEL_TINY_PIC)

#define TARGET_SUPPORTS_WIDE_INT 1

/* Modes valid for AdvSIMD D registers, i.e. that fit in half a Q register.  */
#define AARCH64_VALID_SIMD_DREG_MODE(MODE) \
  ((MODE) == V2SImode || (MODE) == V4HImode || (MODE) == V8QImode \
   || (MODE) == V2SFmode || (MODE) == V4HFmode || (MODE) == DImode \
   || (MODE) == DFmode)

/* Modes valid for AdvSIMD Q registers.  */
#define AARCH64_VALID_SIMD_QREG_MODE(MODE) \
  ((MODE) == V4SImode || (MODE) == V8HImode || (MODE) == V16QImode \
   || (MODE) == V4SFmode || (MODE) == V8HFmode || (MODE) == V2DImode \
   || (MODE) == V2DFmode)

#define ENDIAN_LANE_N(NUNITS, N) \
  (BYTES_BIG_ENDIAN ? NUNITS - 1 - N : N)

/* Support for a configure-time default CPU, etc.  We currently support
   --with-arch and --with-cpu.  Both are ignored if either is specified
   explicitly on the command line at run time.  */
#define OPTION_DEFAULT_SPECS				\
  {"arch", "%{!march=*:%{!mcpu=*:-march=%(VALUE)}}" },	\
  {"cpu",  "%{!march=*:%{!mcpu=*:-mcpu=%(VALUE)}}" },

#define MCPU_TO_MARCH_SPEC \
   " %{mcpu=*:-march=%:rewrite_mcpu(%{mcpu=*:%*})}"

extern const char *aarch64_rewrite_mcpu (int argc, const char **argv);
#define MCPU_TO_MARCH_SPEC_FUNCTIONS \
  { "rewrite_mcpu", aarch64_rewrite_mcpu },

#if defined(__aarch64__)
extern const char *host_detect_local_cpu (int argc, const char **argv);
#define HAVE_LOCAL_CPU_DETECT
# define EXTRA_SPEC_FUNCTIONS						\
  { "local_cpu_detect", host_detect_local_cpu },			\
  MCPU_TO_MARCH_SPEC_FUNCTIONS

# define MCPU_MTUNE_NATIVE_SPECS					\
   " %{march=native:%<march=native %:local_cpu_detect(arch)}"		\
   " %{mcpu=native:%<mcpu=native %:local_cpu_detect(cpu)}"		\
   " %{mtune=native:%<mtune=native %:local_cpu_detect(tune)}"
#else
# define MCPU_MTUNE_NATIVE_SPECS ""
# define EXTRA_SPEC_FUNCTIONS MCPU_TO_MARCH_SPEC_FUNCTIONS
#endif

#define ASM_CPU_SPEC \
   MCPU_TO_MARCH_SPEC

#define EXTRA_SPECS						\
  { "asm_cpu_spec",		ASM_CPU_SPEC }

#define ASM_OUTPUT_POOL_EPILOGUE  aarch64_asm_output_pool_epilogue

/* This type is the user-visible __fp16, and a pointer to that type.  We
   need it in many places in the backend.  Defined in aarch64-builtins.c.  */
extern tree aarch64_fp16_type_node;
extern tree aarch64_fp16_ptr_type_node;

/* The generic unwind code in libgcc does not initialize the frame pointer.
   So in order to unwind a function using a frame pointer, the very first
   function that is unwound must save the frame pointer.  That way the frame
   pointer is restored and its value is now valid - otherwise _Unwind_GetGR
   crashes.  Libgcc can now be safely built with -fomit-frame-pointer.  */
#define LIBGCC2_UNWIND_ATTRIBUTE \
  __attribute__((optimize ("no-omit-frame-pointer")))

#ifndef USED_FOR_TARGET
extern poly_uint16 aarch64_sve_vg;

/* The number of bits and bytes in an SVE vector.  */
#define BITS_PER_SVE_VECTOR (poly_uint16 (aarch64_sve_vg * 64))
#define BYTES_PER_SVE_VECTOR (poly_uint16 (aarch64_sve_vg * 8))

/* The number of bytes in an SVE predicate.  */
#define BYTES_PER_SVE_PRED aarch64_sve_vg

/* The SVE mode for a vector of bytes.  */
#define SVE_BYTE_MODE VNx16QImode

/* The maximum number of bytes in a fixed-size vector.  This is 256 bytes
   (for -msve-vector-bits=2048) multiplied by the maximum number of
   vectors in a structure mode (4).

   This limit must not be used for variable-size vectors, since
   VL-agnostic code must work with arbitary vector lengths.  */
#define MAX_COMPILE_TIME_VEC_BYTES (256 * 4)
#endif

#define REGMODE_NATURAL_SIZE(MODE) aarch64_regmode_natural_size (MODE)

#endif /* GCC_AARCH64_H */
