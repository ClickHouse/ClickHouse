/* Functions and structures shared between arm and aarch64.

   Copyright (C) 1991-2018 Free Software Foundation, Inc.
   Contributed by ARM Ltd.

   This file is part of GCC.

   GCC is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published
   by the Free Software Foundation; either version 3, or (at your
   option) any later version.

   GCC is distributed in the hope that it will be useful, but WITHOUT
   ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public
   License for more details.

   You should have received a copy of the GNU General Public License
   along with GCC; see the file COPYING3.  If not see
   <http://www.gnu.org/licenses/>.  */


#ifndef GCC_AARCH_COMMON_PROTOS_H
#define GCC_AARCH_COMMON_PROTOS_H

extern int aarch_accumulator_forwarding (rtx_insn *, rtx_insn *);
extern int aarch_crypto_can_dual_issue (rtx_insn *, rtx_insn *);
extern bool aarch_rev16_p (rtx);
extern bool aarch_rev16_shleft_mask_imm_p (rtx, machine_mode);
extern bool aarch_rev16_shright_mask_imm_p (rtx, machine_mode);
extern int arm_early_load_addr_dep (rtx, rtx);
extern int arm_early_load_addr_dep_ptr (rtx, rtx);
extern int arm_early_store_addr_dep (rtx, rtx);
extern int arm_early_store_addr_dep_ptr (rtx, rtx);
extern int arm_mac_accumulator_is_mul_result (rtx, rtx);
extern int arm_mac_accumulator_is_result (rtx, rtx);
extern int arm_no_early_alu_shift_dep (rtx, rtx);
extern int arm_no_early_alu_shift_value_dep (rtx, rtx);
extern int arm_no_early_mul_dep (rtx, rtx);
extern int arm_no_early_store_addr_dep (rtx, rtx);
extern bool arm_rtx_shift_left_p (rtx);

/* RTX cost table definitions.  These are used when tuning for speed rather
   than for size and should reflect the _additional_ cost over the cost
   of the fastest instruction in the machine, which is COSTS_N_INSNS (1).
   Therefore it's okay for some costs to be 0.
   Costs may not have a negative value.  */
struct alu_cost_table
{
  const int arith;		/* ADD/SUB.  */
  const int logical;		/* AND/ORR/EOR/BIC, etc.  */
  const int shift;		/* Simple shift.  */
  const int shift_reg;		/* Simple shift by reg.  */
  const int arith_shift;	/* Additional when arith also shifts...  */
  const int arith_shift_reg;	/* ... and when the shift is by a reg.  */
  const int log_shift;		/* Additional when logic also shifts...  */
  const int log_shift_reg;	/* ... and when the shift is by a reg.  */
  const int extend;		/* Zero/sign extension.  */
  const int extend_arith;	/* Extend and arith.  */
  const int bfi;		/* Bit-field insert.  */
  const int bfx;		/* Bit-field extraction.  */
  const int clz;		/* Count Leading Zeros.  */
  const int rev;		/* Reverse bits/bytes.  */
  const int non_exec;		/* Extra cost when not executing insn.  */
  const bool non_exec_costs_exec; /* True if non-execution must add the exec
				     cost.  */
};

struct mult_cost_table
{
  const int simple;
  const int flag_setting;	/* Additional cost if multiply sets flags. */
  const int extend;
  const int add;
  const int extend_add;
  const int idiv;
};

/* Calculations of LDM costs are complex.  We assume an initial cost
   (ldm_1st) which will load the number of registers mentioned in
   ldm_regs_per_insn_1st registers; then each additional
   ldm_regs_per_insn_subsequent registers cost one more insn.
   Similarly for STM operations.
   Therefore the ldm_regs_per_insn_1st/stm_regs_per_insn_1st and
   ldm_regs_per_insn_subsequent/stm_regs_per_insn_subsequent fields indicate
   the number of registers loaded/stored and are expressed by a simple integer
   and not by a COSTS_N_INSNS (N) expression.
   */
struct mem_cost_table
{
  const int load;
  const int load_sign_extend;	/* Additional to load cost.  */
  const int ldrd;		/* Cost of LDRD.  */
  const int ldm_1st;
  const int ldm_regs_per_insn_1st;
  const int ldm_regs_per_insn_subsequent;
  const int loadf;		/* SFmode.  */
  const int loadd;		/* DFmode.  */
  const int load_unaligned;	/* Extra for unaligned loads.  */
  const int store;
  const int strd;
  const int stm_1st;
  const int stm_regs_per_insn_1st;
  const int stm_regs_per_insn_subsequent;
  const int storef;		/* SFmode.  */
  const int stored;		/* DFmode.  */
  const int store_unaligned;	/* Extra for unaligned stores.  */
  const int loadv;		/* Vector load.  */
  const int storev;		/* Vector store.  */
};

struct fp_cost_table
{
  const int div;
  const int mult;
  const int mult_addsub;	/* Non-fused.  */
  const int fma;		/* Fused.  */
  const int addsub;
  const int fpconst;		/* Immediate.  */
  const int neg;		/* NEG and ABS.  */
  const int compare;
  const int widen;		/* Widen to this size.  */
  const int narrow;		/* Narrow from this size.  */
  const int toint;
  const int fromint;
  const int roundint;		/* V8 round to integral, remains FP format.  */
};

struct vector_cost_table
{
  const int alu;
};

struct cpu_cost_table
{
  const struct alu_cost_table alu;
  const struct mult_cost_table mult[2]; /* SImode and DImode.  */
  const struct mem_cost_table ldst;
  const struct fp_cost_table fp[2]; /* SFmode and DFmode.  */
  const struct vector_cost_table vect;
};


#endif /* GCC_AARCH_COMMON_PROTOS_H */
