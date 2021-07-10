/* Generated automatically by the program `genflags'
   from the machine description file `md'.  */

#ifndef GCC_INSN_FLAGS_H
#define GCC_INSN_FLAGS_H

#define HAVE_indirect_jump 1
#define HAVE_jump 1
#define HAVE_ccmpsi 1
#define HAVE_ccmpdi 1
#define HAVE_fccmpsf (TARGET_FLOAT)
#define HAVE_fccmpdf (TARGET_FLOAT)
#define HAVE_fccmpesf (TARGET_FLOAT)
#define HAVE_fccmpedf (TARGET_FLOAT)
#define HAVE_condjump 1
#define HAVE_casesi_dispatch 1
#define HAVE_nop 1
#define HAVE_prefetch 1
#define HAVE_trap 1
#define HAVE_simple_return 1
#define HAVE_insv_immsi (UINTVAL (operands[1]) < GET_MODE_BITSIZE (SImode) \
   && UINTVAL (operands[1]) % 16 == 0)
#define HAVE_insv_immdi (UINTVAL (operands[1]) < GET_MODE_BITSIZE (DImode) \
   && UINTVAL (operands[1]) % 16 == 0)
#define HAVE_load_pairsi (rtx_equal_p (XEXP (operands[3], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[1], 0), \
			       GET_MODE_SIZE (SImode))))
#define HAVE_load_pairdi (rtx_equal_p (XEXP (operands[3], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[1], 0), \
			       GET_MODE_SIZE (DImode))))
#define HAVE_store_pairsi (rtx_equal_p (XEXP (operands[2], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[0], 0), \
			       GET_MODE_SIZE (SImode))))
#define HAVE_store_pairdi (rtx_equal_p (XEXP (operands[2], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[0], 0), \
			       GET_MODE_SIZE (DImode))))
#define HAVE_load_pairsf (rtx_equal_p (XEXP (operands[3], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[1], 0), \
			       GET_MODE_SIZE (SFmode))))
#define HAVE_load_pairdf (rtx_equal_p (XEXP (operands[3], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[1], 0), \
			       GET_MODE_SIZE (DFmode))))
#define HAVE_store_pairsf (rtx_equal_p (XEXP (operands[2], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[0], 0), \
			       GET_MODE_SIZE (SFmode))))
#define HAVE_store_pairdf (rtx_equal_p (XEXP (operands[2], 0), \
		plus_constant (Pmode, \
			       XEXP (operands[0], 0), \
			       GET_MODE_SIZE (DFmode))))
#define HAVE_loadwb_pairsi_si ((INTVAL (operands[5]) == GET_MODE_SIZE (SImode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_loadwb_pairsi_di ((INTVAL (operands[5]) == GET_MODE_SIZE (SImode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_loadwb_pairdi_si ((INTVAL (operands[5]) == GET_MODE_SIZE (DImode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_loadwb_pairdi_di ((INTVAL (operands[5]) == GET_MODE_SIZE (DImode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_loadwb_pairsf_si ((INTVAL (operands[5]) == GET_MODE_SIZE (SFmode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_loadwb_pairsf_di ((INTVAL (operands[5]) == GET_MODE_SIZE (SFmode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_loadwb_pairdf_si ((INTVAL (operands[5]) == GET_MODE_SIZE (DFmode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_loadwb_pairdf_di ((INTVAL (operands[5]) == GET_MODE_SIZE (DFmode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_storewb_pairsi_si ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (SImode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_storewb_pairsi_di ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (SImode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_storewb_pairdi_si ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (DImode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_storewb_pairdi_di ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (DImode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_storewb_pairsf_si ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (SFmode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_storewb_pairsf_di ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (SFmode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_storewb_pairdf_si ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (DFmode)) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_storewb_pairdf_di ((INTVAL (operands[5]) == INTVAL (operands[4]) + GET_MODE_SIZE (DFmode)) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_addsi3_compare0 1
#define HAVE_adddi3_compare0 1
#define HAVE_addsi3_compareC 1
#define HAVE_adddi3_compareC 1
#define HAVE_aarch64_subsi_compare0 1
#define HAVE_aarch64_subdi_compare0 1
#define HAVE_subsi3 1
#define HAVE_subdi3 1
#define HAVE_subsi3_compare1 1
#define HAVE_subdi3_compare1 1
#define HAVE_subsi3_compare1_imm (INTVAL (operands[3]) == -INTVAL (operands[2]))
#define HAVE_subdi3_compare1_imm (INTVAL (operands[3]) == -INTVAL (operands[2]))
#define HAVE_negsi2 1
#define HAVE_negdi2 1
#define HAVE_negsi2_compare0 1
#define HAVE_negdi2_compare0 1
#define HAVE_mulsi3 1
#define HAVE_muldi3 1
#define HAVE_maddsi 1
#define HAVE_madddi 1
#define HAVE_mulsidi3 1
#define HAVE_umulsidi3 1
#define HAVE_maddsidi4 1
#define HAVE_umaddsidi4 1
#define HAVE_msubsidi4 1
#define HAVE_umsubsidi4 1
#define HAVE_smuldi3_highpart 1
#define HAVE_umuldi3_highpart 1
#define HAVE_divsi3 1
#define HAVE_udivsi3 1
#define HAVE_divdi3 1
#define HAVE_udivdi3 1
#define HAVE_cmpsi 1
#define HAVE_cmpdi 1
#define HAVE_fcmpsf (TARGET_FLOAT)
#define HAVE_fcmpdf (TARGET_FLOAT)
#define HAVE_fcmpesf (TARGET_FLOAT)
#define HAVE_fcmpedf (TARGET_FLOAT)
#define HAVE_aarch64_cstoreqi 1
#define HAVE_aarch64_cstorehi 1
#define HAVE_aarch64_cstoresi 1
#define HAVE_aarch64_cstoredi 1
#define HAVE_cstoreqi_neg 1
#define HAVE_cstorehi_neg 1
#define HAVE_cstoresi_neg 1
#define HAVE_cstoredi_neg 1
#define HAVE_aarch64_crc32b (TARGET_CRC32)
#define HAVE_aarch64_crc32h (TARGET_CRC32)
#define HAVE_aarch64_crc32w (TARGET_CRC32)
#define HAVE_aarch64_crc32x (TARGET_CRC32)
#define HAVE_aarch64_crc32cb (TARGET_CRC32)
#define HAVE_aarch64_crc32ch (TARGET_CRC32)
#define HAVE_aarch64_crc32cw (TARGET_CRC32)
#define HAVE_aarch64_crc32cx (TARGET_CRC32)
#define HAVE_csinc3si_insn 1
#define HAVE_csinc3di_insn 1
#define HAVE_csneg3_uxtw_insn 1
#define HAVE_csneg3si_insn 1
#define HAVE_csneg3di_insn 1
#define HAVE_aarch64_uqdecsi (TARGET_SVE)
#define HAVE_aarch64_uqdecdi (TARGET_SVE)
#define HAVE_andsi3 1
#define HAVE_iorsi3 1
#define HAVE_xorsi3 1
#define HAVE_anddi3 1
#define HAVE_iordi3 1
#define HAVE_xordi3 1
#define HAVE_one_cmplsi2 1
#define HAVE_one_cmpldi2 1
#define HAVE_and_one_cmpl_ashlsi3 1
#define HAVE_ior_one_cmpl_ashlsi3 1
#define HAVE_xor_one_cmpl_ashlsi3 1
#define HAVE_and_one_cmpl_ashrsi3 1
#define HAVE_ior_one_cmpl_ashrsi3 1
#define HAVE_xor_one_cmpl_ashrsi3 1
#define HAVE_and_one_cmpl_lshrsi3 1
#define HAVE_ior_one_cmpl_lshrsi3 1
#define HAVE_xor_one_cmpl_lshrsi3 1
#define HAVE_and_one_cmpl_rotrsi3 1
#define HAVE_ior_one_cmpl_rotrsi3 1
#define HAVE_xor_one_cmpl_rotrsi3 1
#define HAVE_and_one_cmpl_ashldi3 1
#define HAVE_ior_one_cmpl_ashldi3 1
#define HAVE_xor_one_cmpl_ashldi3 1
#define HAVE_and_one_cmpl_ashrdi3 1
#define HAVE_ior_one_cmpl_ashrdi3 1
#define HAVE_xor_one_cmpl_ashrdi3 1
#define HAVE_and_one_cmpl_lshrdi3 1
#define HAVE_ior_one_cmpl_lshrdi3 1
#define HAVE_xor_one_cmpl_lshrdi3 1
#define HAVE_and_one_cmpl_rotrdi3 1
#define HAVE_ior_one_cmpl_rotrdi3 1
#define HAVE_xor_one_cmpl_rotrdi3 1
#define HAVE_clzsi2 1
#define HAVE_clzdi2 1
#define HAVE_clrsbsi2 1
#define HAVE_clrsbdi2 1
#define HAVE_rbitsi2 1
#define HAVE_rbitdi2 1
#define HAVE_ctzsi2 1
#define HAVE_ctzdi2 1
#define HAVE_bswapsi2 1
#define HAVE_bswapdi2 1
#define HAVE_bswaphi2 1
#define HAVE_rev16si2 (aarch_rev16_shleft_mask_imm_p (operands[3], SImode) \
   && aarch_rev16_shright_mask_imm_p (operands[2], SImode))
#define HAVE_rev16di2 (aarch_rev16_shleft_mask_imm_p (operands[3], DImode) \
   && aarch_rev16_shright_mask_imm_p (operands[2], DImode))
#define HAVE_rev16si2_alt (aarch_rev16_shleft_mask_imm_p (operands[3], SImode) \
   && aarch_rev16_shright_mask_imm_p (operands[2], SImode))
#define HAVE_rev16di2_alt (aarch_rev16_shleft_mask_imm_p (operands[3], DImode) \
   && aarch_rev16_shright_mask_imm_p (operands[2], DImode))
#define HAVE_btrunchf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_ceilhf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_floorhf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_frintnhf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_nearbyinthf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_rinthf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_roundhf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_btruncsf2 (TARGET_FLOAT)
#define HAVE_ceilsf2 (TARGET_FLOAT)
#define HAVE_floorsf2 (TARGET_FLOAT)
#define HAVE_frintnsf2 (TARGET_FLOAT)
#define HAVE_nearbyintsf2 (TARGET_FLOAT)
#define HAVE_rintsf2 (TARGET_FLOAT)
#define HAVE_roundsf2 (TARGET_FLOAT)
#define HAVE_btruncdf2 (TARGET_FLOAT)
#define HAVE_ceildf2 (TARGET_FLOAT)
#define HAVE_floordf2 (TARGET_FLOAT)
#define HAVE_frintndf2 (TARGET_FLOAT)
#define HAVE_nearbyintdf2 (TARGET_FLOAT)
#define HAVE_rintdf2 (TARGET_FLOAT)
#define HAVE_rounddf2 (TARGET_FLOAT)
#define HAVE_lbtrunchfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lceilhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfloorhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lroundhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfrintnhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lbtruncuhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lceiluhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lflooruhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lrounduhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfrintnuhfsi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lbtrunchfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lceilhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfloorhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lroundhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfrintnhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lbtruncuhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lceiluhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lflooruhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lrounduhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lfrintnuhfdi2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_lbtruncsfsi2 (TARGET_FLOAT)
#define HAVE_lceilsfsi2 (TARGET_FLOAT)
#define HAVE_lfloorsfsi2 (TARGET_FLOAT)
#define HAVE_lroundsfsi2 (TARGET_FLOAT)
#define HAVE_lfrintnsfsi2 (TARGET_FLOAT)
#define HAVE_lbtruncusfsi2 (TARGET_FLOAT)
#define HAVE_lceilusfsi2 (TARGET_FLOAT)
#define HAVE_lfloorusfsi2 (TARGET_FLOAT)
#define HAVE_lroundusfsi2 (TARGET_FLOAT)
#define HAVE_lfrintnusfsi2 (TARGET_FLOAT)
#define HAVE_lbtruncsfdi2 (TARGET_FLOAT)
#define HAVE_lceilsfdi2 (TARGET_FLOAT)
#define HAVE_lfloorsfdi2 (TARGET_FLOAT)
#define HAVE_lroundsfdi2 (TARGET_FLOAT)
#define HAVE_lfrintnsfdi2 (TARGET_FLOAT)
#define HAVE_lbtruncusfdi2 (TARGET_FLOAT)
#define HAVE_lceilusfdi2 (TARGET_FLOAT)
#define HAVE_lfloorusfdi2 (TARGET_FLOAT)
#define HAVE_lroundusfdi2 (TARGET_FLOAT)
#define HAVE_lfrintnusfdi2 (TARGET_FLOAT)
#define HAVE_lbtruncdfsi2 (TARGET_FLOAT)
#define HAVE_lceildfsi2 (TARGET_FLOAT)
#define HAVE_lfloordfsi2 (TARGET_FLOAT)
#define HAVE_lrounddfsi2 (TARGET_FLOAT)
#define HAVE_lfrintndfsi2 (TARGET_FLOAT)
#define HAVE_lbtruncudfsi2 (TARGET_FLOAT)
#define HAVE_lceiludfsi2 (TARGET_FLOAT)
#define HAVE_lfloorudfsi2 (TARGET_FLOAT)
#define HAVE_lroundudfsi2 (TARGET_FLOAT)
#define HAVE_lfrintnudfsi2 (TARGET_FLOAT)
#define HAVE_lbtruncdfdi2 (TARGET_FLOAT)
#define HAVE_lceildfdi2 (TARGET_FLOAT)
#define HAVE_lfloordfdi2 (TARGET_FLOAT)
#define HAVE_lrounddfdi2 (TARGET_FLOAT)
#define HAVE_lfrintndfdi2 (TARGET_FLOAT)
#define HAVE_lbtruncudfdi2 (TARGET_FLOAT)
#define HAVE_lceiludfdi2 (TARGET_FLOAT)
#define HAVE_lfloorudfdi2 (TARGET_FLOAT)
#define HAVE_lroundudfdi2 (TARGET_FLOAT)
#define HAVE_lfrintnudfdi2 (TARGET_FLOAT)
#define HAVE_fmahf4 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_fmasf4 (TARGET_FLOAT)
#define HAVE_fmadf4 (TARGET_FLOAT)
#define HAVE_fnmahf4 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_fnmasf4 (TARGET_FLOAT)
#define HAVE_fnmadf4 (TARGET_FLOAT)
#define HAVE_fmssf4 (TARGET_FLOAT)
#define HAVE_fmsdf4 (TARGET_FLOAT)
#define HAVE_fnmssf4 (TARGET_FLOAT)
#define HAVE_fnmsdf4 (TARGET_FLOAT)
#define HAVE_extendsfdf2 (TARGET_FLOAT)
#define HAVE_extendhfsf2 (TARGET_FLOAT)
#define HAVE_extendhfdf2 (TARGET_FLOAT)
#define HAVE_truncdfsf2 (TARGET_FLOAT)
#define HAVE_truncsfhf2 (TARGET_FLOAT)
#define HAVE_truncdfhf2 (TARGET_FLOAT)
#define HAVE_fix_truncsfsi2 (TARGET_FLOAT)
#define HAVE_fixuns_truncsfsi2 (TARGET_FLOAT)
#define HAVE_fix_truncdfdi2 (TARGET_FLOAT)
#define HAVE_fixuns_truncdfdi2 (TARGET_FLOAT)
#define HAVE_fix_trunchfsi2 (TARGET_FP_F16INST)
#define HAVE_fixuns_trunchfsi2 (TARGET_FP_F16INST)
#define HAVE_fix_trunchfdi2 (TARGET_FP_F16INST)
#define HAVE_fixuns_trunchfdi2 (TARGET_FP_F16INST)
#define HAVE_fix_truncdfsi2 (TARGET_FLOAT)
#define HAVE_fixuns_truncdfsi2 (TARGET_FLOAT)
#define HAVE_fix_truncsfdi2 (TARGET_FLOAT)
#define HAVE_fixuns_truncsfdi2 (TARGET_FLOAT)
#define HAVE_floatsisf2 (TARGET_FLOAT)
#define HAVE_floatunssisf2 (TARGET_FLOAT)
#define HAVE_floatdidf2 (TARGET_FLOAT)
#define HAVE_floatunsdidf2 (TARGET_FLOAT)
#define HAVE_floatdisf2 (TARGET_FLOAT)
#define HAVE_floatunsdisf2 (TARGET_FLOAT)
#define HAVE_floatsidf2 (TARGET_FLOAT)
#define HAVE_floatunssidf2 (TARGET_FLOAT)
#define HAVE_aarch64_fp16_floatsihf2 (TARGET_FP_F16INST)
#define HAVE_aarch64_fp16_floatunssihf2 (TARGET_FP_F16INST)
#define HAVE_aarch64_fp16_floatdihf2 (TARGET_FP_F16INST)
#define HAVE_aarch64_fp16_floatunsdihf2 (TARGET_FP_F16INST)
#define HAVE_fcvtzssf3 1
#define HAVE_fcvtzusf3 1
#define HAVE_fcvtzsdf3 1
#define HAVE_fcvtzudf3 1
#define HAVE_scvtfsi3 1
#define HAVE_ucvtfsi3 1
#define HAVE_scvtfdi3 1
#define HAVE_ucvtfdi3 1
#define HAVE_fcvtzshfsi3 (TARGET_FP_F16INST)
#define HAVE_fcvtzuhfsi3 (TARGET_FP_F16INST)
#define HAVE_fcvtzshfdi3 (TARGET_FP_F16INST)
#define HAVE_fcvtzuhfdi3 (TARGET_FP_F16INST)
#define HAVE_scvtfsihf3 (TARGET_FP_F16INST)
#define HAVE_ucvtfsihf3 (TARGET_FP_F16INST)
#define HAVE_scvtfdihf3 (TARGET_FP_F16INST)
#define HAVE_ucvtfdihf3 (TARGET_FP_F16INST)
#define HAVE_fcvtzshf3 (TARGET_SIMD)
#define HAVE_fcvtzuhf3 (TARGET_SIMD)
#define HAVE_scvtfhi3 (TARGET_SIMD)
#define HAVE_ucvtfhi3 (TARGET_SIMD)
#define HAVE_addhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_addsf3 (TARGET_FLOAT)
#define HAVE_adddf3 (TARGET_FLOAT)
#define HAVE_subhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_subsf3 (TARGET_FLOAT)
#define HAVE_subdf3 (TARGET_FLOAT)
#define HAVE_mulhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_mulsf3 (TARGET_FLOAT)
#define HAVE_muldf3 (TARGET_FLOAT)
#define HAVE_neghf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_negsf2 (TARGET_FLOAT)
#define HAVE_negdf2 (TARGET_FLOAT)
#define HAVE_abshf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_abssf2 (TARGET_FLOAT)
#define HAVE_absdf2 (TARGET_FLOAT)
#define HAVE_smaxsf3 (TARGET_FLOAT)
#define HAVE_smaxdf3 (TARGET_FLOAT)
#define HAVE_sminsf3 (TARGET_FLOAT)
#define HAVE_smindf3 (TARGET_FLOAT)
#define HAVE_smax_nanhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_smin_nanhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_fmaxhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_fminhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_smax_nansf3 (TARGET_FLOAT)
#define HAVE_smin_nansf3 (TARGET_FLOAT)
#define HAVE_fmaxsf3 (TARGET_FLOAT)
#define HAVE_fminsf3 (TARGET_FLOAT)
#define HAVE_smax_nandf3 (TARGET_FLOAT)
#define HAVE_smin_nandf3 (TARGET_FLOAT)
#define HAVE_fmaxdf3 (TARGET_FLOAT)
#define HAVE_fmindf3 (TARGET_FLOAT)
#define HAVE_aarch64_movdi_tilow (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movdi_tflow (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movdi_tihigh (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movdi_tfhigh (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movtihigh_di (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movtfhigh_di (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movtilow_di (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movtflow_di (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_aarch64_movtilow_tilow (TARGET_FLOAT && (reload_completed || reload_in_progress))
#define HAVE_add_losym_si (ptr_mode == SImode || Pmode == SImode)
#define HAVE_add_losym_di (ptr_mode == DImode || Pmode == DImode)
#define HAVE_ldr_got_small_si (ptr_mode == SImode)
#define HAVE_ldr_got_small_di (ptr_mode == DImode)
#define HAVE_ldr_got_small_sidi (TARGET_ILP32)
#define HAVE_ldr_got_small_28k_si (ptr_mode == SImode)
#define HAVE_ldr_got_small_28k_di (ptr_mode == DImode)
#define HAVE_ldr_got_small_28k_sidi (TARGET_ILP32)
#define HAVE_ldr_got_tiny 1
#define HAVE_aarch64_load_tp_hard 1
#define HAVE_tlsie_small_si (ptr_mode == SImode)
#define HAVE_tlsie_small_di (ptr_mode == DImode)
#define HAVE_tlsie_small_sidi 1
#define HAVE_tlsie_tiny_si (ptr_mode == SImode)
#define HAVE_tlsie_tiny_di (ptr_mode == DImode)
#define HAVE_tlsie_tiny_sidi 1
#define HAVE_tlsle12_si (ptr_mode == SImode || Pmode == SImode)
#define HAVE_tlsle12_di (ptr_mode == DImode || Pmode == DImode)
#define HAVE_tlsle24_si (ptr_mode == SImode || Pmode == SImode)
#define HAVE_tlsle24_di (ptr_mode == DImode || Pmode == DImode)
#define HAVE_tlsle32_si (ptr_mode == SImode || Pmode == SImode)
#define HAVE_tlsle32_di (ptr_mode == DImode || Pmode == DImode)
#define HAVE_tlsle48_si (ptr_mode == SImode || Pmode == SImode)
#define HAVE_tlsle48_di (ptr_mode == DImode || Pmode == DImode)
#define HAVE_tlsdesc_small_advsimd_si ((TARGET_TLS_DESC && !TARGET_SVE) && (ptr_mode == SImode))
#define HAVE_tlsdesc_small_advsimd_di ((TARGET_TLS_DESC && !TARGET_SVE) && (ptr_mode == DImode))
#define HAVE_tlsdesc_small_sve_si ((TARGET_TLS_DESC && TARGET_SVE) && (ptr_mode == SImode))
#define HAVE_tlsdesc_small_sve_di ((TARGET_TLS_DESC && TARGET_SVE) && (ptr_mode == DImode))
#define HAVE_stack_tie 1
#define HAVE_pacisp 1
#define HAVE_autisp 1
#define HAVE_paci1716 1
#define HAVE_auti1716 1
#define HAVE_xpaclri 1
#define HAVE_blockage 1
#define HAVE_probe_stack_range 1
#define HAVE_stack_protect_set_si (ptr_mode == SImode)
#define HAVE_stack_protect_set_di (ptr_mode == DImode)
#define HAVE_stack_protect_test_si (ptr_mode == SImode)
#define HAVE_stack_protect_test_di (ptr_mode == DImode)
#define HAVE_set_fpcr 1
#define HAVE_get_fpcr 1
#define HAVE_set_fpsr 1
#define HAVE_get_fpsr 1
#define HAVE_speculation_tracker 1
#define HAVE_speculation_barrier 1
#define HAVE_despeculate_simpleqi 1
#define HAVE_despeculate_simplehi 1
#define HAVE_despeculate_simplesi 1
#define HAVE_despeculate_simpledi 1
#define HAVE_despeculate_simpleti 1
#define HAVE_aarch64_simd_dupv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_dupv2df (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_dup_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_128v8qi (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_64v16qi (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_128v4hi (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_64v8hi (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_128v2si (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_64v4si (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_128v4hf (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_64v8hf (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_128v2sf (TARGET_SIMD)
#define HAVE_aarch64_dup_lane_to_64v4sf (TARGET_SIMD)
#define HAVE_aarch64_store_lane0v8qi (TARGET_SIMD \
   && ENDIAN_LANE_N (8, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v16qi (TARGET_SIMD \
   && ENDIAN_LANE_N (16, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v4hi (TARGET_SIMD \
   && ENDIAN_LANE_N (4, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v8hi (TARGET_SIMD \
   && ENDIAN_LANE_N (8, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v2si (TARGET_SIMD \
   && ENDIAN_LANE_N (2, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v4si (TARGET_SIMD \
   && ENDIAN_LANE_N (4, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v2di (TARGET_SIMD \
   && ENDIAN_LANE_N (2, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v4hf (TARGET_SIMD \
   && ENDIAN_LANE_N (4, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v8hf (TARGET_SIMD \
   && ENDIAN_LANE_N (8, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v2sf (TARGET_SIMD \
   && ENDIAN_LANE_N (2, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v4sf (TARGET_SIMD \
   && ENDIAN_LANE_N (4, INTVAL (operands[2])) == 0)
#define HAVE_aarch64_store_lane0v2df (TARGET_SIMD \
   && ENDIAN_LANE_N (2, INTVAL (operands[2])) == 0)
#define HAVE_load_pairv8qi (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[3], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V8QImode))))
#define HAVE_load_pairv4hi (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[3], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V4HImode))))
#define HAVE_load_pairv4hf (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[3], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V4HFmode))))
#define HAVE_load_pairv2si (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[3], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V2SImode))))
#define HAVE_load_pairv2sf (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[3], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V2SFmode))))
#define HAVE_store_pairv8qi (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[0], 0), \
				  GET_MODE_SIZE (V8QImode))))
#define HAVE_store_pairv4hi (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[0], 0), \
				  GET_MODE_SIZE (V4HImode))))
#define HAVE_store_pairv4hf (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[0], 0), \
				  GET_MODE_SIZE (V4HFmode))))
#define HAVE_store_pairv2si (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[0], 0), \
				  GET_MODE_SIZE (V2SImode))))
#define HAVE_store_pairv2sf (TARGET_SIMD \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[0], 0), \
				  GET_MODE_SIZE (V2SFmode))))
#define HAVE_aarch64_simd_mov_from_v16qilow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v8hilow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v4silow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v2dilow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v8hflow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v4sflow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v2dflow (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v16qihigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v8hihigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v4sihigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v2dihigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v8hfhigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v4sfhigh (TARGET_SIMD && reload_completed)
#define HAVE_aarch64_simd_mov_from_v2dfhigh (TARGET_SIMD && reload_completed)
#define HAVE_ornv8qi3 (TARGET_SIMD)
#define HAVE_ornv16qi3 (TARGET_SIMD)
#define HAVE_ornv4hi3 (TARGET_SIMD)
#define HAVE_ornv8hi3 (TARGET_SIMD)
#define HAVE_ornv2si3 (TARGET_SIMD)
#define HAVE_ornv4si3 (TARGET_SIMD)
#define HAVE_ornv2di3 (TARGET_SIMD)
#define HAVE_bicv8qi3 (TARGET_SIMD)
#define HAVE_bicv16qi3 (TARGET_SIMD)
#define HAVE_bicv4hi3 (TARGET_SIMD)
#define HAVE_bicv8hi3 (TARGET_SIMD)
#define HAVE_bicv2si3 (TARGET_SIMD)
#define HAVE_bicv4si3 (TARGET_SIMD)
#define HAVE_bicv2di3 (TARGET_SIMD)
#define HAVE_addv8qi3 (TARGET_SIMD)
#define HAVE_addv16qi3 (TARGET_SIMD)
#define HAVE_addv4hi3 (TARGET_SIMD)
#define HAVE_addv8hi3 (TARGET_SIMD)
#define HAVE_addv2si3 (TARGET_SIMD)
#define HAVE_addv4si3 (TARGET_SIMD)
#define HAVE_addv2di3 (TARGET_SIMD)
#define HAVE_subv8qi3 (TARGET_SIMD)
#define HAVE_subv16qi3 (TARGET_SIMD)
#define HAVE_subv4hi3 (TARGET_SIMD)
#define HAVE_subv8hi3 (TARGET_SIMD)
#define HAVE_subv2si3 (TARGET_SIMD)
#define HAVE_subv4si3 (TARGET_SIMD)
#define HAVE_subv2di3 (TARGET_SIMD)
#define HAVE_mulv8qi3 (TARGET_SIMD)
#define HAVE_mulv16qi3 (TARGET_SIMD)
#define HAVE_mulv4hi3 (TARGET_SIMD)
#define HAVE_mulv8hi3 (TARGET_SIMD)
#define HAVE_mulv2si3 (TARGET_SIMD)
#define HAVE_mulv4si3 (TARGET_SIMD)
#define HAVE_bswapv4hi2 (TARGET_SIMD)
#define HAVE_bswapv8hi2 (TARGET_SIMD)
#define HAVE_bswapv2si2 (TARGET_SIMD)
#define HAVE_bswapv4si2 (TARGET_SIMD)
#define HAVE_bswapv2di2 (TARGET_SIMD)
#define HAVE_aarch64_rbitv8qi (TARGET_SIMD)
#define HAVE_aarch64_rbitv16qi (TARGET_SIMD)
#define HAVE_aarch64_sdotv8qi (TARGET_DOTPROD)
#define HAVE_aarch64_udotv8qi (TARGET_DOTPROD)
#define HAVE_aarch64_sdotv16qi (TARGET_DOTPROD)
#define HAVE_aarch64_udotv16qi (TARGET_DOTPROD)
#define HAVE_aarch64_sdot_lanev8qi (TARGET_DOTPROD)
#define HAVE_aarch64_udot_lanev8qi (TARGET_DOTPROD)
#define HAVE_aarch64_sdot_lanev16qi (TARGET_DOTPROD)
#define HAVE_aarch64_udot_lanev16qi (TARGET_DOTPROD)
#define HAVE_aarch64_sdot_laneqv8qi (TARGET_DOTPROD)
#define HAVE_aarch64_udot_laneqv8qi (TARGET_DOTPROD)
#define HAVE_aarch64_sdot_laneqv16qi (TARGET_DOTPROD)
#define HAVE_aarch64_udot_laneqv16qi (TARGET_DOTPROD)
#define HAVE_aarch64_rsqrtev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtev2sf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtev4sf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtev2df (TARGET_SIMD)
#define HAVE_aarch64_rsqrtehf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtesf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtedf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtsv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtsv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtsv2sf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtsv4sf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtsv2df (TARGET_SIMD)
#define HAVE_aarch64_rsqrtshf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_rsqrtssf (TARGET_SIMD)
#define HAVE_aarch64_rsqrtsdf (TARGET_SIMD)
#define HAVE_negv8qi2 (TARGET_SIMD)
#define HAVE_negv16qi2 (TARGET_SIMD)
#define HAVE_negv4hi2 (TARGET_SIMD)
#define HAVE_negv8hi2 (TARGET_SIMD)
#define HAVE_negv2si2 (TARGET_SIMD)
#define HAVE_negv4si2 (TARGET_SIMD)
#define HAVE_negv2di2 (TARGET_SIMD)
#define HAVE_absv8qi2 (TARGET_SIMD)
#define HAVE_absv16qi2 (TARGET_SIMD)
#define HAVE_absv4hi2 (TARGET_SIMD)
#define HAVE_absv8hi2 (TARGET_SIMD)
#define HAVE_absv2si2 (TARGET_SIMD)
#define HAVE_absv4si2 (TARGET_SIMD)
#define HAVE_absv2di2 (TARGET_SIMD)
#define HAVE_aarch64_absv8qi (TARGET_SIMD)
#define HAVE_aarch64_absv16qi (TARGET_SIMD)
#define HAVE_aarch64_absv4hi (TARGET_SIMD)
#define HAVE_aarch64_absv8hi (TARGET_SIMD)
#define HAVE_aarch64_absv2si (TARGET_SIMD)
#define HAVE_aarch64_absv4si (TARGET_SIMD)
#define HAVE_aarch64_absv2di (TARGET_SIMD)
#define HAVE_aarch64_absdi (TARGET_SIMD)
#define HAVE_abdv8qi_3 (TARGET_SIMD)
#define HAVE_abdv16qi_3 (TARGET_SIMD)
#define HAVE_abdv4hi_3 (TARGET_SIMD)
#define HAVE_abdv8hi_3 (TARGET_SIMD)
#define HAVE_abdv2si_3 (TARGET_SIMD)
#define HAVE_abdv4si_3 (TARGET_SIMD)
#define HAVE_abav8qi_3 (TARGET_SIMD)
#define HAVE_abav16qi_3 (TARGET_SIMD)
#define HAVE_abav4hi_3 (TARGET_SIMD)
#define HAVE_abav8hi_3 (TARGET_SIMD)
#define HAVE_abav2si_3 (TARGET_SIMD)
#define HAVE_abav4si_3 (TARGET_SIMD)
#define HAVE_fabdv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fabdv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fabdv2sf3 (TARGET_SIMD)
#define HAVE_fabdv4sf3 (TARGET_SIMD)
#define HAVE_fabdv2df3 (TARGET_SIMD)
#define HAVE_fabdhf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fabdsf3 (TARGET_SIMD)
#define HAVE_fabddf3 (TARGET_SIMD)
#define HAVE_andv8qi3 (TARGET_SIMD)
#define HAVE_andv16qi3 (TARGET_SIMD)
#define HAVE_andv4hi3 (TARGET_SIMD)
#define HAVE_andv8hi3 (TARGET_SIMD)
#define HAVE_andv2si3 (TARGET_SIMD)
#define HAVE_andv4si3 (TARGET_SIMD)
#define HAVE_andv2di3 (TARGET_SIMD)
#define HAVE_iorv8qi3 (TARGET_SIMD)
#define HAVE_iorv16qi3 (TARGET_SIMD)
#define HAVE_iorv4hi3 (TARGET_SIMD)
#define HAVE_iorv8hi3 (TARGET_SIMD)
#define HAVE_iorv2si3 (TARGET_SIMD)
#define HAVE_iorv4si3 (TARGET_SIMD)
#define HAVE_iorv2di3 (TARGET_SIMD)
#define HAVE_xorv8qi3 (TARGET_SIMD)
#define HAVE_xorv16qi3 (TARGET_SIMD)
#define HAVE_xorv4hi3 (TARGET_SIMD)
#define HAVE_xorv8hi3 (TARGET_SIMD)
#define HAVE_xorv2si3 (TARGET_SIMD)
#define HAVE_xorv4si3 (TARGET_SIMD)
#define HAVE_xorv2di3 (TARGET_SIMD)
#define HAVE_one_cmplv8qi2 (TARGET_SIMD)
#define HAVE_one_cmplv16qi2 (TARGET_SIMD)
#define HAVE_one_cmplv4hi2 (TARGET_SIMD)
#define HAVE_one_cmplv8hi2 (TARGET_SIMD)
#define HAVE_one_cmplv2si2 (TARGET_SIMD)
#define HAVE_one_cmplv4si2 (TARGET_SIMD)
#define HAVE_one_cmplv2di2 (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_lshrv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ashrv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_imm_shlv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_sshlv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv8qi_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv16qi_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv4hi_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv8hi_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv2si_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv4si_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv2di_unsigned (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv8qi_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv16qi_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv4hi_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv8hi_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv2si_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv4si_signed (TARGET_SIMD)
#define HAVE_aarch64_simd_reg_shlv2di_signed (TARGET_SIMD)
#define HAVE_vec_shr_v8qi (TARGET_SIMD)
#define HAVE_vec_shr_v4hi (TARGET_SIMD)
#define HAVE_vec_shr_v4hf (TARGET_SIMD)
#define HAVE_vec_shr_v2si (TARGET_SIMD)
#define HAVE_vec_shr_v2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_setv2df (TARGET_SIMD)
#define HAVE_aarch64_mlav8qi (TARGET_SIMD)
#define HAVE_aarch64_mlav16qi (TARGET_SIMD)
#define HAVE_aarch64_mlav4hi (TARGET_SIMD)
#define HAVE_aarch64_mlav8hi (TARGET_SIMD)
#define HAVE_aarch64_mlav2si (TARGET_SIMD)
#define HAVE_aarch64_mlav4si (TARGET_SIMD)
#define HAVE_aarch64_mlsv8qi (TARGET_SIMD)
#define HAVE_aarch64_mlsv16qi (TARGET_SIMD)
#define HAVE_aarch64_mlsv4hi (TARGET_SIMD)
#define HAVE_aarch64_mlsv8hi (TARGET_SIMD)
#define HAVE_aarch64_mlsv2si (TARGET_SIMD)
#define HAVE_aarch64_mlsv4si (TARGET_SIMD)
#define HAVE_smaxv8qi3 (TARGET_SIMD)
#define HAVE_sminv8qi3 (TARGET_SIMD)
#define HAVE_umaxv8qi3 (TARGET_SIMD)
#define HAVE_uminv8qi3 (TARGET_SIMD)
#define HAVE_smaxv16qi3 (TARGET_SIMD)
#define HAVE_sminv16qi3 (TARGET_SIMD)
#define HAVE_umaxv16qi3 (TARGET_SIMD)
#define HAVE_uminv16qi3 (TARGET_SIMD)
#define HAVE_smaxv4hi3 (TARGET_SIMD)
#define HAVE_sminv4hi3 (TARGET_SIMD)
#define HAVE_umaxv4hi3 (TARGET_SIMD)
#define HAVE_uminv4hi3 (TARGET_SIMD)
#define HAVE_smaxv8hi3 (TARGET_SIMD)
#define HAVE_sminv8hi3 (TARGET_SIMD)
#define HAVE_umaxv8hi3 (TARGET_SIMD)
#define HAVE_uminv8hi3 (TARGET_SIMD)
#define HAVE_smaxv2si3 (TARGET_SIMD)
#define HAVE_sminv2si3 (TARGET_SIMD)
#define HAVE_umaxv2si3 (TARGET_SIMD)
#define HAVE_uminv2si3 (TARGET_SIMD)
#define HAVE_smaxv4si3 (TARGET_SIMD)
#define HAVE_sminv4si3 (TARGET_SIMD)
#define HAVE_umaxv4si3 (TARGET_SIMD)
#define HAVE_uminv4si3 (TARGET_SIMD)
#define HAVE_aarch64_umaxpv8qi (TARGET_SIMD)
#define HAVE_aarch64_uminpv8qi (TARGET_SIMD)
#define HAVE_aarch64_smaxpv8qi (TARGET_SIMD)
#define HAVE_aarch64_sminpv8qi (TARGET_SIMD)
#define HAVE_aarch64_umaxpv16qi (TARGET_SIMD)
#define HAVE_aarch64_uminpv16qi (TARGET_SIMD)
#define HAVE_aarch64_smaxpv16qi (TARGET_SIMD)
#define HAVE_aarch64_sminpv16qi (TARGET_SIMD)
#define HAVE_aarch64_umaxpv4hi (TARGET_SIMD)
#define HAVE_aarch64_uminpv4hi (TARGET_SIMD)
#define HAVE_aarch64_smaxpv4hi (TARGET_SIMD)
#define HAVE_aarch64_sminpv4hi (TARGET_SIMD)
#define HAVE_aarch64_umaxpv8hi (TARGET_SIMD)
#define HAVE_aarch64_uminpv8hi (TARGET_SIMD)
#define HAVE_aarch64_smaxpv8hi (TARGET_SIMD)
#define HAVE_aarch64_sminpv8hi (TARGET_SIMD)
#define HAVE_aarch64_umaxpv2si (TARGET_SIMD)
#define HAVE_aarch64_uminpv2si (TARGET_SIMD)
#define HAVE_aarch64_smaxpv2si (TARGET_SIMD)
#define HAVE_aarch64_sminpv2si (TARGET_SIMD)
#define HAVE_aarch64_umaxpv4si (TARGET_SIMD)
#define HAVE_aarch64_uminpv4si (TARGET_SIMD)
#define HAVE_aarch64_smaxpv4si (TARGET_SIMD)
#define HAVE_aarch64_sminpv4si (TARGET_SIMD)
#define HAVE_aarch64_smax_nanpv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smin_nanpv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smaxpv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_sminpv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smax_nanpv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smin_nanpv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smaxpv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_sminpv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_smax_nanpv2sf (TARGET_SIMD)
#define HAVE_aarch64_smin_nanpv2sf (TARGET_SIMD)
#define HAVE_aarch64_smaxpv2sf (TARGET_SIMD)
#define HAVE_aarch64_sminpv2sf (TARGET_SIMD)
#define HAVE_aarch64_smax_nanpv4sf (TARGET_SIMD)
#define HAVE_aarch64_smin_nanpv4sf (TARGET_SIMD)
#define HAVE_aarch64_smaxpv4sf (TARGET_SIMD)
#define HAVE_aarch64_sminpv4sf (TARGET_SIMD)
#define HAVE_aarch64_smax_nanpv2df (TARGET_SIMD)
#define HAVE_aarch64_smin_nanpv2df (TARGET_SIMD)
#define HAVE_aarch64_smaxpv2df (TARGET_SIMD)
#define HAVE_aarch64_sminpv2df (TARGET_SIMD)
#define HAVE_move_lo_quad_internal_v16qi (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v8hi (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v4si (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v8hf (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v4sf (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v2di (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_v2df (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v16qi (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v8hi (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v4si (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v8hf (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v4sf (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v2di (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_move_lo_quad_internal_be_v2df (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v16qi (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v8hi (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v4si (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v2di (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v8hf (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v4sf (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_v2df (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v16qi (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v8hi (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v4si (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v2di (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v8hf (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v4sf (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_move_hi_quad_be_v2df (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_simd_vec_pack_trunc_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_pack_trunc_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_pack_trunc_v2di (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v8hi (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v4si (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_lo_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_lo_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_lo_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_lo_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_lo_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_lo_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_hi_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_hi_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_hi_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_hi_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_hi_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacku_hi_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_lo_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_lo_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_lo_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_lo_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_lo_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_lo_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_hi_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_hi_v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_hi_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_hi_v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_smult_hi_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_umult_hi_v4si (TARGET_SIMD)
#define HAVE_addv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_addv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_addv2sf3 (TARGET_SIMD)
#define HAVE_addv4sf3 (TARGET_SIMD)
#define HAVE_addv2df3 (TARGET_SIMD)
#define HAVE_subv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_subv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_subv2sf3 (TARGET_SIMD)
#define HAVE_subv4sf3 (TARGET_SIMD)
#define HAVE_subv2df3 (TARGET_SIMD)
#define HAVE_mulv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_mulv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_mulv2sf3 (TARGET_SIMD)
#define HAVE_mulv4sf3 (TARGET_SIMD)
#define HAVE_mulv2df3 (TARGET_SIMD)
#define HAVE_negv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_negv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_negv2sf2 (TARGET_SIMD)
#define HAVE_negv4sf2 (TARGET_SIMD)
#define HAVE_negv2df2 (TARGET_SIMD)
#define HAVE_absv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_absv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_absv2sf2 (TARGET_SIMD)
#define HAVE_absv4sf2 (TARGET_SIMD)
#define HAVE_absv2df2 (TARGET_SIMD)
#define HAVE_fmav4hf4 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fmav8hf4 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fmav2sf4 (TARGET_SIMD)
#define HAVE_fmav4sf4 (TARGET_SIMD)
#define HAVE_fmav2df4 (TARGET_SIMD)
#define HAVE_fnmav4hf4 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fnmav8hf4 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fnmav2sf4 (TARGET_SIMD)
#define HAVE_fnmav4sf4 (TARGET_SIMD)
#define HAVE_fnmav2df4 (TARGET_SIMD)
#define HAVE_btruncv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ceilv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floorv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_frintnv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_nearbyintv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_rintv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_roundv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_btruncv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ceilv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floorv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_frintnv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_nearbyintv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_rintv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_roundv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_btruncv2sf2 (TARGET_SIMD)
#define HAVE_ceilv2sf2 (TARGET_SIMD)
#define HAVE_floorv2sf2 (TARGET_SIMD)
#define HAVE_frintnv2sf2 (TARGET_SIMD)
#define HAVE_nearbyintv2sf2 (TARGET_SIMD)
#define HAVE_rintv2sf2 (TARGET_SIMD)
#define HAVE_roundv2sf2 (TARGET_SIMD)
#define HAVE_btruncv4sf2 (TARGET_SIMD)
#define HAVE_ceilv4sf2 (TARGET_SIMD)
#define HAVE_floorv4sf2 (TARGET_SIMD)
#define HAVE_frintnv4sf2 (TARGET_SIMD)
#define HAVE_nearbyintv4sf2 (TARGET_SIMD)
#define HAVE_rintv4sf2 (TARGET_SIMD)
#define HAVE_roundv4sf2 (TARGET_SIMD)
#define HAVE_btruncv2df2 (TARGET_SIMD)
#define HAVE_ceilv2df2 (TARGET_SIMD)
#define HAVE_floorv2df2 (TARGET_SIMD)
#define HAVE_frintnv2df2 (TARGET_SIMD)
#define HAVE_nearbyintv2df2 (TARGET_SIMD)
#define HAVE_rintv2df2 (TARGET_SIMD)
#define HAVE_roundv2df2 (TARGET_SIMD)
#define HAVE_lbtruncv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lceilv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfloorv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lroundv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfrintnv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lbtruncuv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lceiluv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lflooruv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lrounduv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfrintnuv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lbtruncv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lceilv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfloorv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lroundv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfrintnv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lbtruncuv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lceiluv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lflooruv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lrounduv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lfrintnuv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_lbtruncv2sfv2si2 (TARGET_SIMD)
#define HAVE_lceilv2sfv2si2 (TARGET_SIMD)
#define HAVE_lfloorv2sfv2si2 (TARGET_SIMD)
#define HAVE_lroundv2sfv2si2 (TARGET_SIMD)
#define HAVE_lfrintnv2sfv2si2 (TARGET_SIMD)
#define HAVE_lbtruncuv2sfv2si2 (TARGET_SIMD)
#define HAVE_lceiluv2sfv2si2 (TARGET_SIMD)
#define HAVE_lflooruv2sfv2si2 (TARGET_SIMD)
#define HAVE_lrounduv2sfv2si2 (TARGET_SIMD)
#define HAVE_lfrintnuv2sfv2si2 (TARGET_SIMD)
#define HAVE_lbtruncv4sfv4si2 (TARGET_SIMD)
#define HAVE_lceilv4sfv4si2 (TARGET_SIMD)
#define HAVE_lfloorv4sfv4si2 (TARGET_SIMD)
#define HAVE_lroundv4sfv4si2 (TARGET_SIMD)
#define HAVE_lfrintnv4sfv4si2 (TARGET_SIMD)
#define HAVE_lbtruncuv4sfv4si2 (TARGET_SIMD)
#define HAVE_lceiluv4sfv4si2 (TARGET_SIMD)
#define HAVE_lflooruv4sfv4si2 (TARGET_SIMD)
#define HAVE_lrounduv4sfv4si2 (TARGET_SIMD)
#define HAVE_lfrintnuv4sfv4si2 (TARGET_SIMD)
#define HAVE_lbtruncv2dfv2di2 (TARGET_SIMD)
#define HAVE_lceilv2dfv2di2 (TARGET_SIMD)
#define HAVE_lfloorv2dfv2di2 (TARGET_SIMD)
#define HAVE_lroundv2dfv2di2 (TARGET_SIMD)
#define HAVE_lfrintnv2dfv2di2 (TARGET_SIMD)
#define HAVE_lbtruncuv2dfv2di2 (TARGET_SIMD)
#define HAVE_lceiluv2dfv2di2 (TARGET_SIMD)
#define HAVE_lflooruv2dfv2di2 (TARGET_SIMD)
#define HAVE_lrounduv2dfv2di2 (TARGET_SIMD)
#define HAVE_lfrintnuv2dfv2di2 (TARGET_SIMD)
#define HAVE_lbtrunchfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lceilhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lfloorhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lroundhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lfrintnhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lbtruncuhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lceiluhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lflooruhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lrounduhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_lfrintnuhfhi2 (TARGET_SIMD_F16INST)
#define HAVE_fix_trunchfhi2 (TARGET_SIMD_F16INST)
#define HAVE_fixuns_trunchfhi2 (TARGET_SIMD_F16INST)
#define HAVE_floathihf2 (TARGET_SIMD_F16INST)
#define HAVE_floatunshihf2 (TARGET_SIMD_F16INST)
#define HAVE_floatv4hiv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floatunsv4hiv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floatv8hiv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floatunsv8hiv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_floatv2siv2sf2 (TARGET_SIMD)
#define HAVE_floatunsv2siv2sf2 (TARGET_SIMD)
#define HAVE_floatv4siv4sf2 (TARGET_SIMD)
#define HAVE_floatunsv4siv4sf2 (TARGET_SIMD)
#define HAVE_floatv2div2df2 (TARGET_SIMD)
#define HAVE_floatunsv2div2df2 (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_lo_v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_lo_v4sf (TARGET_SIMD)
#define HAVE_fcvtzsv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fcvtzuv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fcvtzsv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fcvtzuv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fcvtzsv2sf3 (TARGET_SIMD)
#define HAVE_fcvtzuv2sf3 (TARGET_SIMD)
#define HAVE_fcvtzsv4sf3 (TARGET_SIMD)
#define HAVE_fcvtzuv4sf3 (TARGET_SIMD)
#define HAVE_fcvtzsv2df3 (TARGET_SIMD)
#define HAVE_fcvtzuv2df3 (TARGET_SIMD)
#define HAVE_scvtfv4hi3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ucvtfv4hi3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_scvtfv8hi3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ucvtfv8hi3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_scvtfv2si3 (TARGET_SIMD)
#define HAVE_ucvtfv2si3 (TARGET_SIMD)
#define HAVE_scvtfv4si3 (TARGET_SIMD)
#define HAVE_ucvtfv4si3 (TARGET_SIMD)
#define HAVE_scvtfv2di3 (TARGET_SIMD)
#define HAVE_ucvtfv2di3 (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_hi_v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_vec_unpacks_hi_v4sf (TARGET_SIMD)
#define HAVE_aarch64_float_extend_lo_v2df (TARGET_SIMD)
#define HAVE_aarch64_float_extend_lo_v4sf (TARGET_SIMD)
#define HAVE_aarch64_float_truncate_lo_v2sf (TARGET_SIMD)
#define HAVE_aarch64_float_truncate_lo_v4hf (TARGET_SIMD)
#define HAVE_aarch64_float_truncate_hi_v4sf_le (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_float_truncate_hi_v8hf_le (TARGET_SIMD && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_float_truncate_hi_v4sf_be (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_float_truncate_hi_v8hf_be (TARGET_SIMD && BYTES_BIG_ENDIAN)
#define HAVE_smaxv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_sminv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smaxv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_sminv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smaxv2sf3 (TARGET_SIMD)
#define HAVE_sminv2sf3 (TARGET_SIMD)
#define HAVE_smaxv4sf3 (TARGET_SIMD)
#define HAVE_sminv4sf3 (TARGET_SIMD)
#define HAVE_smaxv2df3 (TARGET_SIMD)
#define HAVE_sminv2df3 (TARGET_SIMD)
#define HAVE_smax_nanv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smin_nanv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fmaxv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fminv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smax_nanv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smin_nanv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fmaxv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fminv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_smax_nanv2sf3 (TARGET_SIMD)
#define HAVE_smin_nanv2sf3 (TARGET_SIMD)
#define HAVE_fmaxv2sf3 (TARGET_SIMD)
#define HAVE_fminv2sf3 (TARGET_SIMD)
#define HAVE_smax_nanv4sf3 (TARGET_SIMD)
#define HAVE_smin_nanv4sf3 (TARGET_SIMD)
#define HAVE_fmaxv4sf3 (TARGET_SIMD)
#define HAVE_fminv4sf3 (TARGET_SIMD)
#define HAVE_smax_nanv2df3 (TARGET_SIMD)
#define HAVE_smin_nanv2df3 (TARGET_SIMD)
#define HAVE_fmaxv2df3 (TARGET_SIMD)
#define HAVE_fminv2df3 (TARGET_SIMD)
#define HAVE_aarch64_faddpv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_faddpv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_faddpv2sf (TARGET_SIMD)
#define HAVE_aarch64_faddpv4sf (TARGET_SIMD)
#define HAVE_aarch64_faddpv2df (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv8qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv16qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv4hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv8hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv4si (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv2di (TARGET_SIMD)
#define HAVE_aarch64_reduc_plus_internalv2si (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v2sf (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v2df (TARGET_SIMD)
#define HAVE_clrsbv8qi2 (TARGET_SIMD)
#define HAVE_clrsbv16qi2 (TARGET_SIMD)
#define HAVE_clrsbv4hi2 (TARGET_SIMD)
#define HAVE_clrsbv8hi2 (TARGET_SIMD)
#define HAVE_clrsbv2si2 (TARGET_SIMD)
#define HAVE_clrsbv4si2 (TARGET_SIMD)
#define HAVE_clzv8qi2 (TARGET_SIMD)
#define HAVE_clzv16qi2 (TARGET_SIMD)
#define HAVE_clzv4hi2 (TARGET_SIMD)
#define HAVE_clzv8hi2 (TARGET_SIMD)
#define HAVE_clzv2si2 (TARGET_SIMD)
#define HAVE_clzv4si2 (TARGET_SIMD)
#define HAVE_popcountv8qi2 (TARGET_SIMD)
#define HAVE_popcountv16qi2 (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv8qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv8qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv8qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv8qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv16qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv16qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv16qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv16qi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv4hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv4hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv4hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv4hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv8hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv8hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv8hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv8hi (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv4si (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv4si (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv4si (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv4si (TARGET_SIMD)
#define HAVE_aarch64_reduc_umax_internalv2si (TARGET_SIMD)
#define HAVE_aarch64_reduc_umin_internalv2si (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv2si (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv2si (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_nan_internalv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smin_nan_internalv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smax_internalv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smin_internalv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smax_nan_internalv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smin_nan_internalv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smax_internalv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smin_internalv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_reduc_smax_nan_internalv2sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_nan_internalv2sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv2sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv2sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_nan_internalv4sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_nan_internalv4sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv4sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv4sf (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_nan_internalv2df (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_nan_internalv2df (TARGET_SIMD)
#define HAVE_aarch64_reduc_smax_internalv2df (TARGET_SIMD)
#define HAVE_aarch64_reduc_smin_internalv2df (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv8qi_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4hi_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2si_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2di_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bsldi_internal (TARGET_SIMD)
#define HAVE_aarch64_simd_bsldi_alt (TARGET_SIMD)
#define HAVE_aarch64_get_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_get_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_get_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_get_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_get_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_get_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_get_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_get_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_get_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_get_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_get_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_get_lanev2df (TARGET_SIMD)
#define HAVE_load_pair_lanesv8qi (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V8QImode))))
#define HAVE_load_pair_lanesv4hi (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V4HImode))))
#define HAVE_load_pair_lanesv4hf (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V4HFmode))))
#define HAVE_load_pair_lanesv2si (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V2SImode))))
#define HAVE_load_pair_lanesv2sf (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (V2SFmode))))
#define HAVE_load_pair_lanesdi (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (DImode))))
#define HAVE_load_pair_lanesdf (TARGET_SIMD && !STRICT_ALIGNMENT \
   && rtx_equal_p (XEXP (operands[2], 0), \
		   plus_constant (Pmode, \
				  XEXP (operands[1], 0), \
				  GET_MODE_SIZE (DFmode))))
#define HAVE_store_pair_lanesv8qi (TARGET_SIMD)
#define HAVE_store_pair_lanesv4hi (TARGET_SIMD)
#define HAVE_store_pair_lanesv4hf (TARGET_SIMD)
#define HAVE_store_pair_lanesv2si (TARGET_SIMD)
#define HAVE_store_pair_lanesv2sf (TARGET_SIMD)
#define HAVE_store_pair_lanesdi (TARGET_SIMD)
#define HAVE_store_pair_lanesdf (TARGET_SIMD)
#define HAVE_aarch64_saddlv16qi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv16qi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv16qi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv16qi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv8hi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv8hi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv8hi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv8hi_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv4si_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv4si_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv4si_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv4si_hi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv16qi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv16qi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv16qi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv16qi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv8hi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv8hi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv8hi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv8hi_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv4si_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_ssublv4si_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddlv4si_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_usublv4si_lo_internal (TARGET_SIMD)
#define HAVE_aarch64_saddlv8qi (TARGET_SIMD)
#define HAVE_aarch64_ssublv8qi (TARGET_SIMD)
#define HAVE_aarch64_uaddlv8qi (TARGET_SIMD)
#define HAVE_aarch64_usublv8qi (TARGET_SIMD)
#define HAVE_aarch64_saddlv4hi (TARGET_SIMD)
#define HAVE_aarch64_ssublv4hi (TARGET_SIMD)
#define HAVE_aarch64_uaddlv4hi (TARGET_SIMD)
#define HAVE_aarch64_usublv4hi (TARGET_SIMD)
#define HAVE_aarch64_saddlv2si (TARGET_SIMD)
#define HAVE_aarch64_ssublv2si (TARGET_SIMD)
#define HAVE_aarch64_uaddlv2si (TARGET_SIMD)
#define HAVE_aarch64_usublv2si (TARGET_SIMD)
#define HAVE_aarch64_saddwv8qi (TARGET_SIMD)
#define HAVE_aarch64_ssubwv8qi (TARGET_SIMD)
#define HAVE_aarch64_uaddwv8qi (TARGET_SIMD)
#define HAVE_aarch64_usubwv8qi (TARGET_SIMD)
#define HAVE_aarch64_saddwv4hi (TARGET_SIMD)
#define HAVE_aarch64_ssubwv4hi (TARGET_SIMD)
#define HAVE_aarch64_uaddwv4hi (TARGET_SIMD)
#define HAVE_aarch64_usubwv4hi (TARGET_SIMD)
#define HAVE_aarch64_saddwv2si (TARGET_SIMD)
#define HAVE_aarch64_ssubwv2si (TARGET_SIMD)
#define HAVE_aarch64_uaddwv2si (TARGET_SIMD)
#define HAVE_aarch64_usubwv2si (TARGET_SIMD)
#define HAVE_aarch64_saddwv16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubwv16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddwv16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_usubwv16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddwv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubwv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddwv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_usubwv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddwv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubwv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddwv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_usubwv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_saddw2v16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_usubw2v16qi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddw2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_usubw2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_saddw2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_usubw2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_shaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_uhaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_srhaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_urhaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_shsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_uhsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_srhsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_urhsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_shaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_uhaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_srhaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_urhaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_shsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_uhsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_srhsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_urhsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_shaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_uhaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_srhaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_urhaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_shsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_uhsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_srhsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_urhsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_shaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_uhaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_srhaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_urhaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_shsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_uhsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_srhsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_urhsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_shaddv2si (TARGET_SIMD)
#define HAVE_aarch64_uhaddv2si (TARGET_SIMD)
#define HAVE_aarch64_srhaddv2si (TARGET_SIMD)
#define HAVE_aarch64_urhaddv2si (TARGET_SIMD)
#define HAVE_aarch64_shsubv2si (TARGET_SIMD)
#define HAVE_aarch64_uhsubv2si (TARGET_SIMD)
#define HAVE_aarch64_srhsubv2si (TARGET_SIMD)
#define HAVE_aarch64_urhsubv2si (TARGET_SIMD)
#define HAVE_aarch64_shaddv4si (TARGET_SIMD)
#define HAVE_aarch64_uhaddv4si (TARGET_SIMD)
#define HAVE_aarch64_srhaddv4si (TARGET_SIMD)
#define HAVE_aarch64_urhaddv4si (TARGET_SIMD)
#define HAVE_aarch64_shsubv4si (TARGET_SIMD)
#define HAVE_aarch64_uhsubv4si (TARGET_SIMD)
#define HAVE_aarch64_srhsubv4si (TARGET_SIMD)
#define HAVE_aarch64_urhsubv4si (TARGET_SIMD)
#define HAVE_aarch64_addhnv8hi (TARGET_SIMD)
#define HAVE_aarch64_raddhnv8hi (TARGET_SIMD)
#define HAVE_aarch64_subhnv8hi (TARGET_SIMD)
#define HAVE_aarch64_rsubhnv8hi (TARGET_SIMD)
#define HAVE_aarch64_addhnv4si (TARGET_SIMD)
#define HAVE_aarch64_raddhnv4si (TARGET_SIMD)
#define HAVE_aarch64_subhnv4si (TARGET_SIMD)
#define HAVE_aarch64_rsubhnv4si (TARGET_SIMD)
#define HAVE_aarch64_addhnv2di (TARGET_SIMD)
#define HAVE_aarch64_raddhnv2di (TARGET_SIMD)
#define HAVE_aarch64_subhnv2di (TARGET_SIMD)
#define HAVE_aarch64_rsubhnv2di (TARGET_SIMD)
#define HAVE_aarch64_addhn2v8hi (TARGET_SIMD)
#define HAVE_aarch64_raddhn2v8hi (TARGET_SIMD)
#define HAVE_aarch64_subhn2v8hi (TARGET_SIMD)
#define HAVE_aarch64_rsubhn2v8hi (TARGET_SIMD)
#define HAVE_aarch64_addhn2v4si (TARGET_SIMD)
#define HAVE_aarch64_raddhn2v4si (TARGET_SIMD)
#define HAVE_aarch64_subhn2v4si (TARGET_SIMD)
#define HAVE_aarch64_rsubhn2v4si (TARGET_SIMD)
#define HAVE_aarch64_addhn2v2di (TARGET_SIMD)
#define HAVE_aarch64_raddhn2v2di (TARGET_SIMD)
#define HAVE_aarch64_subhn2v2di (TARGET_SIMD)
#define HAVE_aarch64_rsubhn2v2di (TARGET_SIMD)
#define HAVE_aarch64_pmulv8qi (TARGET_SIMD)
#define HAVE_aarch64_pmulv16qi (TARGET_SIMD)
#define HAVE_aarch64_fmulxv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_fmulxv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_fmulxv2sf (TARGET_SIMD)
#define HAVE_aarch64_fmulxv4sf (TARGET_SIMD)
#define HAVE_aarch64_fmulxv2df (TARGET_SIMD)
#define HAVE_aarch64_fmulxhf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_fmulxsf (TARGET_SIMD)
#define HAVE_aarch64_fmulxdf (TARGET_SIMD)
#define HAVE_aarch64_sqaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_uqaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_uqsubv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_uqaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_uqsubv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_uqaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_uqsubv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqsubv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqaddv2si (TARGET_SIMD)
#define HAVE_aarch64_uqaddv2si (TARGET_SIMD)
#define HAVE_aarch64_sqsubv2si (TARGET_SIMD)
#define HAVE_aarch64_uqsubv2si (TARGET_SIMD)
#define HAVE_aarch64_sqaddv4si (TARGET_SIMD)
#define HAVE_aarch64_uqaddv4si (TARGET_SIMD)
#define HAVE_aarch64_sqsubv4si (TARGET_SIMD)
#define HAVE_aarch64_uqsubv4si (TARGET_SIMD)
#define HAVE_aarch64_sqaddv2di (TARGET_SIMD)
#define HAVE_aarch64_uqaddv2di (TARGET_SIMD)
#define HAVE_aarch64_sqsubv2di (TARGET_SIMD)
#define HAVE_aarch64_uqsubv2di (TARGET_SIMD)
#define HAVE_aarch64_sqaddqi (TARGET_SIMD)
#define HAVE_aarch64_uqaddqi (TARGET_SIMD)
#define HAVE_aarch64_sqsubqi (TARGET_SIMD)
#define HAVE_aarch64_uqsubqi (TARGET_SIMD)
#define HAVE_aarch64_sqaddhi (TARGET_SIMD)
#define HAVE_aarch64_uqaddhi (TARGET_SIMD)
#define HAVE_aarch64_sqsubhi (TARGET_SIMD)
#define HAVE_aarch64_uqsubhi (TARGET_SIMD)
#define HAVE_aarch64_sqaddsi (TARGET_SIMD)
#define HAVE_aarch64_uqaddsi (TARGET_SIMD)
#define HAVE_aarch64_sqsubsi (TARGET_SIMD)
#define HAVE_aarch64_uqsubsi (TARGET_SIMD)
#define HAVE_aarch64_sqadddi (TARGET_SIMD)
#define HAVE_aarch64_uqadddi (TARGET_SIMD)
#define HAVE_aarch64_sqsubdi (TARGET_SIMD)
#define HAVE_aarch64_uqsubdi (TARGET_SIMD)
#define HAVE_aarch64_suqaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_usqaddv8qi (TARGET_SIMD)
#define HAVE_aarch64_suqaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_usqaddv16qi (TARGET_SIMD)
#define HAVE_aarch64_suqaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_usqaddv4hi (TARGET_SIMD)
#define HAVE_aarch64_suqaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_usqaddv8hi (TARGET_SIMD)
#define HAVE_aarch64_suqaddv2si (TARGET_SIMD)
#define HAVE_aarch64_usqaddv2si (TARGET_SIMD)
#define HAVE_aarch64_suqaddv4si (TARGET_SIMD)
#define HAVE_aarch64_usqaddv4si (TARGET_SIMD)
#define HAVE_aarch64_suqaddv2di (TARGET_SIMD)
#define HAVE_aarch64_usqaddv2di (TARGET_SIMD)
#define HAVE_aarch64_suqaddqi (TARGET_SIMD)
#define HAVE_aarch64_usqaddqi (TARGET_SIMD)
#define HAVE_aarch64_suqaddhi (TARGET_SIMD)
#define HAVE_aarch64_usqaddhi (TARGET_SIMD)
#define HAVE_aarch64_suqaddsi (TARGET_SIMD)
#define HAVE_aarch64_usqaddsi (TARGET_SIMD)
#define HAVE_aarch64_suqadddi (TARGET_SIMD)
#define HAVE_aarch64_usqadddi (TARGET_SIMD)
#define HAVE_aarch64_sqmovunv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqmovunv4si (TARGET_SIMD)
#define HAVE_aarch64_sqmovunv2di (TARGET_SIMD)
#define HAVE_aarch64_sqmovunhi (TARGET_SIMD)
#define HAVE_aarch64_sqmovunsi (TARGET_SIMD)
#define HAVE_aarch64_sqmovundi (TARGET_SIMD)
#define HAVE_aarch64_sqmovnv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqmovnv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqmovnv4si (TARGET_SIMD)
#define HAVE_aarch64_uqmovnv4si (TARGET_SIMD)
#define HAVE_aarch64_sqmovnv2di (TARGET_SIMD)
#define HAVE_aarch64_uqmovnv2di (TARGET_SIMD)
#define HAVE_aarch64_sqmovnhi (TARGET_SIMD)
#define HAVE_aarch64_uqmovnhi (TARGET_SIMD)
#define HAVE_aarch64_sqmovnsi (TARGET_SIMD)
#define HAVE_aarch64_uqmovnsi (TARGET_SIMD)
#define HAVE_aarch64_sqmovndi (TARGET_SIMD)
#define HAVE_aarch64_uqmovndi (TARGET_SIMD)
#define HAVE_aarch64_sqnegv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqabsv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqnegv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqabsv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqnegv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqabsv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqnegv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqabsv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqnegv2si (TARGET_SIMD)
#define HAVE_aarch64_sqabsv2si (TARGET_SIMD)
#define HAVE_aarch64_sqnegv4si (TARGET_SIMD)
#define HAVE_aarch64_sqabsv4si (TARGET_SIMD)
#define HAVE_aarch64_sqnegv2di (TARGET_SIMD)
#define HAVE_aarch64_sqabsv2di (TARGET_SIMD)
#define HAVE_aarch64_sqnegqi (TARGET_SIMD)
#define HAVE_aarch64_sqabsqi (TARGET_SIMD)
#define HAVE_aarch64_sqneghi (TARGET_SIMD)
#define HAVE_aarch64_sqabshi (TARGET_SIMD)
#define HAVE_aarch64_sqnegsi (TARGET_SIMD)
#define HAVE_aarch64_sqabssi (TARGET_SIMD)
#define HAVE_aarch64_sqnegdi (TARGET_SIMD)
#define HAVE_aarch64_sqabsdi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhv2si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhv4si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhhi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulhsi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulhsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqv2si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqv4si (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanehi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanehi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_lanesi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_lanesi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqhi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmulh_laneqsi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmulh_laneqsi (TARGET_SIMD)
#define HAVE_aarch64_sqrdmlahv4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshv4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlahv8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshv8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlahv2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshv2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlahv4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshv4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlahhi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshhi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlahsi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlshsi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanev4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanev4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanev8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanev8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanev2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanev2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanev4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanev4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanehi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanehi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_lanesi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_lanesi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqv4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqv4hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqv8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqv8hi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqv2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqv2si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqv4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqv4si (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqhi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqhi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlah_laneqsi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqrdmlsh_laneqsi (TARGET_SIMD_RDMA)
#define HAVE_aarch64_sqdmlalv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlslv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlalv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlslv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlalhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlslhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlalsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlslsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_laneqv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_laneqv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_laneqv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_laneqv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_lanehi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_lanehi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_lanesi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_lanesi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_laneqhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_laneqhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_laneqsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_laneqsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_lanev8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_lanev8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_lanev4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_lanev4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_laneqv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_laneqv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_laneqv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_laneqv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_nv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_nv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_nv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_nv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmullv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmullv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmullhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmullsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_laneqv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_laneqv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_lanehi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_lanesi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_laneqhi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_laneqsi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2v8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2v4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_lanev8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_lanev4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_laneqv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_laneqv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_nv8hi_internal (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_nv4si_internal (TARGET_SIMD)
#define HAVE_aarch64_sshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_ushlv8qi (TARGET_SIMD)
#define HAVE_aarch64_srshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_urshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_sshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_ushlv16qi (TARGET_SIMD)
#define HAVE_aarch64_srshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_urshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_sshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_ushlv4hi (TARGET_SIMD)
#define HAVE_aarch64_srshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_urshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_sshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_ushlv8hi (TARGET_SIMD)
#define HAVE_aarch64_srshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_urshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_sshlv2si (TARGET_SIMD)
#define HAVE_aarch64_ushlv2si (TARGET_SIMD)
#define HAVE_aarch64_srshlv2si (TARGET_SIMD)
#define HAVE_aarch64_urshlv2si (TARGET_SIMD)
#define HAVE_aarch64_sshlv4si (TARGET_SIMD)
#define HAVE_aarch64_ushlv4si (TARGET_SIMD)
#define HAVE_aarch64_srshlv4si (TARGET_SIMD)
#define HAVE_aarch64_urshlv4si (TARGET_SIMD)
#define HAVE_aarch64_sshlv2di (TARGET_SIMD)
#define HAVE_aarch64_ushlv2di (TARGET_SIMD)
#define HAVE_aarch64_srshlv2di (TARGET_SIMD)
#define HAVE_aarch64_urshlv2di (TARGET_SIMD)
#define HAVE_aarch64_sshldi (TARGET_SIMD)
#define HAVE_aarch64_ushldi (TARGET_SIMD)
#define HAVE_aarch64_srshldi (TARGET_SIMD)
#define HAVE_aarch64_urshldi (TARGET_SIMD)
#define HAVE_aarch64_sqshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_uqshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_uqshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_uqshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqshlv2si (TARGET_SIMD)
#define HAVE_aarch64_uqshlv2si (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv2si (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv2si (TARGET_SIMD)
#define HAVE_aarch64_sqshlv4si (TARGET_SIMD)
#define HAVE_aarch64_uqshlv4si (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv4si (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv4si (TARGET_SIMD)
#define HAVE_aarch64_sqshlv2di (TARGET_SIMD)
#define HAVE_aarch64_uqshlv2di (TARGET_SIMD)
#define HAVE_aarch64_sqrshlv2di (TARGET_SIMD)
#define HAVE_aarch64_uqrshlv2di (TARGET_SIMD)
#define HAVE_aarch64_sqshlqi (TARGET_SIMD)
#define HAVE_aarch64_uqshlqi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlqi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlqi (TARGET_SIMD)
#define HAVE_aarch64_sqshlhi (TARGET_SIMD)
#define HAVE_aarch64_uqshlhi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlhi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlhi (TARGET_SIMD)
#define HAVE_aarch64_sqshlsi (TARGET_SIMD)
#define HAVE_aarch64_uqshlsi (TARGET_SIMD)
#define HAVE_aarch64_sqrshlsi (TARGET_SIMD)
#define HAVE_aarch64_uqrshlsi (TARGET_SIMD)
#define HAVE_aarch64_sqshldi (TARGET_SIMD)
#define HAVE_aarch64_uqshldi (TARGET_SIMD)
#define HAVE_aarch64_sqrshldi (TARGET_SIMD)
#define HAVE_aarch64_uqrshldi (TARGET_SIMD)
#define HAVE_aarch64_sshll_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_ushll_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_sshll_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_ushll_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sshll_nv2si (TARGET_SIMD)
#define HAVE_aarch64_ushll_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sshll2_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_ushll2_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_sshll2_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_ushll2_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sshll2_nv4si (TARGET_SIMD)
#define HAVE_aarch64_ushll2_nv4si (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv2si (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv2si (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv4si (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv4si (TARGET_SIMD)
#define HAVE_aarch64_srshr_nv2di (TARGET_SIMD)
#define HAVE_aarch64_urshr_nv2di (TARGET_SIMD)
#define HAVE_aarch64_srshr_ndi (TARGET_SIMD)
#define HAVE_aarch64_urshr_ndi (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_usra_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_usra_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_usra_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_usra_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv2si (TARGET_SIMD)
#define HAVE_aarch64_usra_nv2si (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv2si (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv2si (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv4si (TARGET_SIMD)
#define HAVE_aarch64_usra_nv4si (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv4si (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv4si (TARGET_SIMD)
#define HAVE_aarch64_ssra_nv2di (TARGET_SIMD)
#define HAVE_aarch64_usra_nv2di (TARGET_SIMD)
#define HAVE_aarch64_srsra_nv2di (TARGET_SIMD)
#define HAVE_aarch64_ursra_nv2di (TARGET_SIMD)
#define HAVE_aarch64_ssra_ndi (TARGET_SIMD)
#define HAVE_aarch64_usra_ndi (TARGET_SIMD)
#define HAVE_aarch64_srsra_ndi (TARGET_SIMD)
#define HAVE_aarch64_ursra_ndi (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_usli_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_usri_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_usli_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_usri_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_usli_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_usri_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_usli_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_usri_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv2si (TARGET_SIMD)
#define HAVE_aarch64_usli_nv2si (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv2si (TARGET_SIMD)
#define HAVE_aarch64_usri_nv2si (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv4si (TARGET_SIMD)
#define HAVE_aarch64_usli_nv4si (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv4si (TARGET_SIMD)
#define HAVE_aarch64_usri_nv4si (TARGET_SIMD)
#define HAVE_aarch64_ssli_nv2di (TARGET_SIMD)
#define HAVE_aarch64_usli_nv2di (TARGET_SIMD)
#define HAVE_aarch64_ssri_nv2di (TARGET_SIMD)
#define HAVE_aarch64_usri_nv2di (TARGET_SIMD)
#define HAVE_aarch64_ssli_ndi (TARGET_SIMD)
#define HAVE_aarch64_usli_ndi (TARGET_SIMD)
#define HAVE_aarch64_ssri_ndi (TARGET_SIMD)
#define HAVE_aarch64_usri_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv8qi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv16qi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv4hi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv2si (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv2si (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv4si (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nv2di (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nqi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nqi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nqi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nhi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_nsi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqshlu_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqshl_ndi (TARGET_SIMD)
#define HAVE_aarch64_uqshl_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_nv4si (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_nv4si (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_nv2di (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_nv2di (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_nv2di (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_nhi (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_nhi (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_nhi (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_nsi (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_nsi (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_nsi (TARGET_SIMD)
#define HAVE_aarch64_sqshrun_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrun_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqshrn_ndi (TARGET_SIMD)
#define HAVE_aarch64_uqshrn_ndi (TARGET_SIMD)
#define HAVE_aarch64_sqrshrn_ndi (TARGET_SIMD)
#define HAVE_aarch64_uqrshrn_ndi (TARGET_SIMD)
#define HAVE_aarch64_cmltv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmlev8qi (TARGET_SIMD)
#define HAVE_aarch64_cmeqv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmgev8qi (TARGET_SIMD)
#define HAVE_aarch64_cmgtv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmltv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmlev16qi (TARGET_SIMD)
#define HAVE_aarch64_cmeqv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmgev16qi (TARGET_SIMD)
#define HAVE_aarch64_cmgtv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmltv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmlev4hi (TARGET_SIMD)
#define HAVE_aarch64_cmeqv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmgev4hi (TARGET_SIMD)
#define HAVE_aarch64_cmgtv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmltv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmlev8hi (TARGET_SIMD)
#define HAVE_aarch64_cmeqv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmgev8hi (TARGET_SIMD)
#define HAVE_aarch64_cmgtv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmltv2si (TARGET_SIMD)
#define HAVE_aarch64_cmlev2si (TARGET_SIMD)
#define HAVE_aarch64_cmeqv2si (TARGET_SIMD)
#define HAVE_aarch64_cmgev2si (TARGET_SIMD)
#define HAVE_aarch64_cmgtv2si (TARGET_SIMD)
#define HAVE_aarch64_cmltv4si (TARGET_SIMD)
#define HAVE_aarch64_cmlev4si (TARGET_SIMD)
#define HAVE_aarch64_cmeqv4si (TARGET_SIMD)
#define HAVE_aarch64_cmgev4si (TARGET_SIMD)
#define HAVE_aarch64_cmgtv4si (TARGET_SIMD)
#define HAVE_aarch64_cmltv2di (TARGET_SIMD)
#define HAVE_aarch64_cmlev2di (TARGET_SIMD)
#define HAVE_aarch64_cmeqv2di (TARGET_SIMD)
#define HAVE_aarch64_cmgev2di (TARGET_SIMD)
#define HAVE_aarch64_cmgtv2di (TARGET_SIMD)
#define HAVE_aarch64_cmltdi (TARGET_SIMD)
#define HAVE_aarch64_cmledi (TARGET_SIMD)
#define HAVE_aarch64_cmeqdi (TARGET_SIMD)
#define HAVE_aarch64_cmgedi (TARGET_SIMD)
#define HAVE_aarch64_cmgtdi (TARGET_SIMD)
#define HAVE_aarch64_cmltuv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmleuv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmltuv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmleuv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmltuv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmleuv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmltuv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmleuv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmltuv2si (TARGET_SIMD)
#define HAVE_aarch64_cmleuv2si (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv2si (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv2si (TARGET_SIMD)
#define HAVE_aarch64_cmltuv4si (TARGET_SIMD)
#define HAVE_aarch64_cmleuv4si (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv4si (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv4si (TARGET_SIMD)
#define HAVE_aarch64_cmltuv2di (TARGET_SIMD)
#define HAVE_aarch64_cmleuv2di (TARGET_SIMD)
#define HAVE_aarch64_cmgeuv2di (TARGET_SIMD)
#define HAVE_aarch64_cmgtuv2di (TARGET_SIMD)
#define HAVE_aarch64_cmltudi (TARGET_SIMD)
#define HAVE_aarch64_cmleudi (TARGET_SIMD)
#define HAVE_aarch64_cmgeudi (TARGET_SIMD)
#define HAVE_aarch64_cmgtudi (TARGET_SIMD)
#define HAVE_aarch64_cmtstv8qi (TARGET_SIMD)
#define HAVE_aarch64_cmtstv16qi (TARGET_SIMD)
#define HAVE_aarch64_cmtstv4hi (TARGET_SIMD)
#define HAVE_aarch64_cmtstv8hi (TARGET_SIMD)
#define HAVE_aarch64_cmtstv2si (TARGET_SIMD)
#define HAVE_aarch64_cmtstv4si (TARGET_SIMD)
#define HAVE_aarch64_cmtstv2di (TARGET_SIMD)
#define HAVE_aarch64_cmtstdi (TARGET_SIMD)
#define HAVE_aarch64_cmltv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmlev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmeqv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgtv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmltv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmlev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmeqv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgtv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmltv2sf (TARGET_SIMD)
#define HAVE_aarch64_cmlev2sf (TARGET_SIMD)
#define HAVE_aarch64_cmeqv2sf (TARGET_SIMD)
#define HAVE_aarch64_cmgev2sf (TARGET_SIMD)
#define HAVE_aarch64_cmgtv2sf (TARGET_SIMD)
#define HAVE_aarch64_cmltv4sf (TARGET_SIMD)
#define HAVE_aarch64_cmlev4sf (TARGET_SIMD)
#define HAVE_aarch64_cmeqv4sf (TARGET_SIMD)
#define HAVE_aarch64_cmgev4sf (TARGET_SIMD)
#define HAVE_aarch64_cmgtv4sf (TARGET_SIMD)
#define HAVE_aarch64_cmltv2df (TARGET_SIMD)
#define HAVE_aarch64_cmlev2df (TARGET_SIMD)
#define HAVE_aarch64_cmeqv2df (TARGET_SIMD)
#define HAVE_aarch64_cmgev2df (TARGET_SIMD)
#define HAVE_aarch64_cmgtv2df (TARGET_SIMD)
#define HAVE_aarch64_cmlthf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmlehf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmeqhf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgehf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmgthf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_cmltsf (TARGET_SIMD)
#define HAVE_aarch64_cmlesf (TARGET_SIMD)
#define HAVE_aarch64_cmeqsf (TARGET_SIMD)
#define HAVE_aarch64_cmgesf (TARGET_SIMD)
#define HAVE_aarch64_cmgtsf (TARGET_SIMD)
#define HAVE_aarch64_cmltdf (TARGET_SIMD)
#define HAVE_aarch64_cmledf (TARGET_SIMD)
#define HAVE_aarch64_cmeqdf (TARGET_SIMD)
#define HAVE_aarch64_cmgedf (TARGET_SIMD)
#define HAVE_aarch64_cmgtdf (TARGET_SIMD)
#define HAVE_aarch64_facltv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_faclev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgtv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facltv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_faclev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgtv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facltv2sf (TARGET_SIMD)
#define HAVE_aarch64_faclev2sf (TARGET_SIMD)
#define HAVE_aarch64_facgev2sf (TARGET_SIMD)
#define HAVE_aarch64_facgtv2sf (TARGET_SIMD)
#define HAVE_aarch64_facltv4sf (TARGET_SIMD)
#define HAVE_aarch64_faclev4sf (TARGET_SIMD)
#define HAVE_aarch64_facgev4sf (TARGET_SIMD)
#define HAVE_aarch64_facgtv4sf (TARGET_SIMD)
#define HAVE_aarch64_facltv2df (TARGET_SIMD)
#define HAVE_aarch64_faclev2df (TARGET_SIMD)
#define HAVE_aarch64_facgev2df (TARGET_SIMD)
#define HAVE_aarch64_facgtv2df (TARGET_SIMD)
#define HAVE_aarch64_faclthf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_faclehf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgehf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facgthf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_facltsf (TARGET_SIMD)
#define HAVE_aarch64_faclesf (TARGET_SIMD)
#define HAVE_aarch64_facgesf (TARGET_SIMD)
#define HAVE_aarch64_facgtsf (TARGET_SIMD)
#define HAVE_aarch64_facltdf (TARGET_SIMD)
#define HAVE_aarch64_facledf (TARGET_SIMD)
#define HAVE_aarch64_facgedf (TARGET_SIMD)
#define HAVE_aarch64_facgtdf (TARGET_SIMD)
#define HAVE_aarch64_addpv8qi (TARGET_SIMD)
#define HAVE_aarch64_addpv4hi (TARGET_SIMD)
#define HAVE_aarch64_addpv2si (TARGET_SIMD)
#define HAVE_aarch64_addpdi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2v2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rv2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rdi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld2rdf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesoi_lanedf (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_st2v2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesoi_lanedf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3v2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rv2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rdi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld3rdf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesci_lanedf (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_st3v2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesci_lanedf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4v2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rv2df (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rdi (TARGET_SIMD)
#define HAVE_aarch64_simd_ld4rdf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_load_lanesxi_lanedf (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v2di (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_st4v2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanedi (TARGET_SIMD)
#define HAVE_aarch64_vec_store_lanesxi_lanedf (TARGET_SIMD)
#define HAVE_aarch64_rev_reglistoi (TARGET_SIMD)
#define HAVE_aarch64_rev_reglistci (TARGET_SIMD)
#define HAVE_aarch64_rev_reglistxi (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v8qi (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v16qi (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v4hi (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v8hi (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v2si (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v4si (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v2di (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v4hf (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v8hf (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v2sf (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v4sf (TARGET_SIMD)
#define HAVE_aarch64_be_ld1v2df (TARGET_SIMD)
#define HAVE_aarch64_be_ld1di (TARGET_SIMD)
#define HAVE_aarch64_be_st1v8qi (TARGET_SIMD)
#define HAVE_aarch64_be_st1v16qi (TARGET_SIMD)
#define HAVE_aarch64_be_st1v4hi (TARGET_SIMD)
#define HAVE_aarch64_be_st1v8hi (TARGET_SIMD)
#define HAVE_aarch64_be_st1v2si (TARGET_SIMD)
#define HAVE_aarch64_be_st1v4si (TARGET_SIMD)
#define HAVE_aarch64_be_st1v2di (TARGET_SIMD)
#define HAVE_aarch64_be_st1v4hf (TARGET_SIMD)
#define HAVE_aarch64_be_st1v8hf (TARGET_SIMD)
#define HAVE_aarch64_be_st1v2sf (TARGET_SIMD)
#define HAVE_aarch64_be_st1v4sf (TARGET_SIMD)
#define HAVE_aarch64_be_st1v2df (TARGET_SIMD)
#define HAVE_aarch64_be_st1di (TARGET_SIMD)
#define HAVE_aarch64_ld2v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2di_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld2df_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3di_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld3df_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4di_dreg (TARGET_SIMD)
#define HAVE_aarch64_ld4df_dreg (TARGET_SIMD)
#define HAVE_aarch64_tbl1v8qi (TARGET_SIMD)
#define HAVE_aarch64_tbl1v16qi (TARGET_SIMD)
#define HAVE_aarch64_tbl2v16qi (TARGET_SIMD)
#define HAVE_aarch64_tbl3v8qi (TARGET_SIMD)
#define HAVE_aarch64_tbl3v16qi (TARGET_SIMD)
#define HAVE_aarch64_tbx4v8qi (TARGET_SIMD)
#define HAVE_aarch64_tbx4v16qi (TARGET_SIMD)
#define HAVE_aarch64_qtbl3v8qi (TARGET_SIMD)
#define HAVE_aarch64_qtbl3v16qi (TARGET_SIMD)
#define HAVE_aarch64_qtbx3v8qi (TARGET_SIMD)
#define HAVE_aarch64_qtbx3v16qi (TARGET_SIMD)
#define HAVE_aarch64_qtbl4v8qi (TARGET_SIMD)
#define HAVE_aarch64_qtbl4v16qi (TARGET_SIMD)
#define HAVE_aarch64_qtbx4v8qi (TARGET_SIMD)
#define HAVE_aarch64_qtbx4v16qi (TARGET_SIMD)
#define HAVE_aarch64_combinev16qi (TARGET_SIMD)
#define HAVE_aarch64_zip1v8qi (TARGET_SIMD)
#define HAVE_aarch64_zip2v8qi (TARGET_SIMD)
#define HAVE_aarch64_trn1v8qi (TARGET_SIMD)
#define HAVE_aarch64_trn2v8qi (TARGET_SIMD)
#define HAVE_aarch64_uzp1v8qi (TARGET_SIMD)
#define HAVE_aarch64_uzp2v8qi (TARGET_SIMD)
#define HAVE_aarch64_zip1v16qi (TARGET_SIMD)
#define HAVE_aarch64_zip2v16qi (TARGET_SIMD)
#define HAVE_aarch64_trn1v16qi (TARGET_SIMD)
#define HAVE_aarch64_trn2v16qi (TARGET_SIMD)
#define HAVE_aarch64_uzp1v16qi (TARGET_SIMD)
#define HAVE_aarch64_uzp2v16qi (TARGET_SIMD)
#define HAVE_aarch64_zip1v4hi (TARGET_SIMD)
#define HAVE_aarch64_zip2v4hi (TARGET_SIMD)
#define HAVE_aarch64_trn1v4hi (TARGET_SIMD)
#define HAVE_aarch64_trn2v4hi (TARGET_SIMD)
#define HAVE_aarch64_uzp1v4hi (TARGET_SIMD)
#define HAVE_aarch64_uzp2v4hi (TARGET_SIMD)
#define HAVE_aarch64_zip1v8hi (TARGET_SIMD)
#define HAVE_aarch64_zip2v8hi (TARGET_SIMD)
#define HAVE_aarch64_trn1v8hi (TARGET_SIMD)
#define HAVE_aarch64_trn2v8hi (TARGET_SIMD)
#define HAVE_aarch64_uzp1v8hi (TARGET_SIMD)
#define HAVE_aarch64_uzp2v8hi (TARGET_SIMD)
#define HAVE_aarch64_zip1v2si (TARGET_SIMD)
#define HAVE_aarch64_zip2v2si (TARGET_SIMD)
#define HAVE_aarch64_trn1v2si (TARGET_SIMD)
#define HAVE_aarch64_trn2v2si (TARGET_SIMD)
#define HAVE_aarch64_uzp1v2si (TARGET_SIMD)
#define HAVE_aarch64_uzp2v2si (TARGET_SIMD)
#define HAVE_aarch64_zip1v4si (TARGET_SIMD)
#define HAVE_aarch64_zip2v4si (TARGET_SIMD)
#define HAVE_aarch64_trn1v4si (TARGET_SIMD)
#define HAVE_aarch64_trn2v4si (TARGET_SIMD)
#define HAVE_aarch64_uzp1v4si (TARGET_SIMD)
#define HAVE_aarch64_uzp2v4si (TARGET_SIMD)
#define HAVE_aarch64_zip1v2di (TARGET_SIMD)
#define HAVE_aarch64_zip2v2di (TARGET_SIMD)
#define HAVE_aarch64_trn1v2di (TARGET_SIMD)
#define HAVE_aarch64_trn2v2di (TARGET_SIMD)
#define HAVE_aarch64_uzp1v2di (TARGET_SIMD)
#define HAVE_aarch64_uzp2v2di (TARGET_SIMD)
#define HAVE_aarch64_zip1v4hf (TARGET_SIMD)
#define HAVE_aarch64_zip2v4hf (TARGET_SIMD)
#define HAVE_aarch64_trn1v4hf (TARGET_SIMD)
#define HAVE_aarch64_trn2v4hf (TARGET_SIMD)
#define HAVE_aarch64_uzp1v4hf (TARGET_SIMD)
#define HAVE_aarch64_uzp2v4hf (TARGET_SIMD)
#define HAVE_aarch64_zip1v8hf (TARGET_SIMD)
#define HAVE_aarch64_zip2v8hf (TARGET_SIMD)
#define HAVE_aarch64_trn1v8hf (TARGET_SIMD)
#define HAVE_aarch64_trn2v8hf (TARGET_SIMD)
#define HAVE_aarch64_uzp1v8hf (TARGET_SIMD)
#define HAVE_aarch64_uzp2v8hf (TARGET_SIMD)
#define HAVE_aarch64_zip1v2sf (TARGET_SIMD)
#define HAVE_aarch64_zip2v2sf (TARGET_SIMD)
#define HAVE_aarch64_trn1v2sf (TARGET_SIMD)
#define HAVE_aarch64_trn2v2sf (TARGET_SIMD)
#define HAVE_aarch64_uzp1v2sf (TARGET_SIMD)
#define HAVE_aarch64_uzp2v2sf (TARGET_SIMD)
#define HAVE_aarch64_zip1v4sf (TARGET_SIMD)
#define HAVE_aarch64_zip2v4sf (TARGET_SIMD)
#define HAVE_aarch64_trn1v4sf (TARGET_SIMD)
#define HAVE_aarch64_trn2v4sf (TARGET_SIMD)
#define HAVE_aarch64_uzp1v4sf (TARGET_SIMD)
#define HAVE_aarch64_uzp2v4sf (TARGET_SIMD)
#define HAVE_aarch64_zip1v2df (TARGET_SIMD)
#define HAVE_aarch64_zip2v2df (TARGET_SIMD)
#define HAVE_aarch64_trn1v2df (TARGET_SIMD)
#define HAVE_aarch64_trn2v2df (TARGET_SIMD)
#define HAVE_aarch64_uzp1v2df (TARGET_SIMD)
#define HAVE_aarch64_uzp2v2df (TARGET_SIMD)
#define HAVE_aarch64_extv8qi (TARGET_SIMD)
#define HAVE_aarch64_extv16qi (TARGET_SIMD)
#define HAVE_aarch64_extv4hi (TARGET_SIMD)
#define HAVE_aarch64_extv8hi (TARGET_SIMD)
#define HAVE_aarch64_extv2si (TARGET_SIMD)
#define HAVE_aarch64_extv4si (TARGET_SIMD)
#define HAVE_aarch64_extv2di (TARGET_SIMD)
#define HAVE_aarch64_extv4hf (TARGET_SIMD)
#define HAVE_aarch64_extv8hf (TARGET_SIMD)
#define HAVE_aarch64_extv2sf (TARGET_SIMD)
#define HAVE_aarch64_extv4sf (TARGET_SIMD)
#define HAVE_aarch64_extv2df (TARGET_SIMD)
#define HAVE_aarch64_rev64v8qi (TARGET_SIMD)
#define HAVE_aarch64_rev32v8qi (TARGET_SIMD)
#define HAVE_aarch64_rev16v8qi (TARGET_SIMD)
#define HAVE_aarch64_rev64v16qi (TARGET_SIMD)
#define HAVE_aarch64_rev32v16qi (TARGET_SIMD)
#define HAVE_aarch64_rev16v16qi (TARGET_SIMD)
#define HAVE_aarch64_rev64v4hi (TARGET_SIMD)
#define HAVE_aarch64_rev32v4hi (TARGET_SIMD)
#define HAVE_aarch64_rev16v4hi (TARGET_SIMD)
#define HAVE_aarch64_rev64v8hi (TARGET_SIMD)
#define HAVE_aarch64_rev32v8hi (TARGET_SIMD)
#define HAVE_aarch64_rev16v8hi (TARGET_SIMD)
#define HAVE_aarch64_rev64v2si (TARGET_SIMD)
#define HAVE_aarch64_rev32v2si (TARGET_SIMD)
#define HAVE_aarch64_rev16v2si (TARGET_SIMD)
#define HAVE_aarch64_rev64v4si (TARGET_SIMD)
#define HAVE_aarch64_rev32v4si (TARGET_SIMD)
#define HAVE_aarch64_rev16v4si (TARGET_SIMD)
#define HAVE_aarch64_rev64v2di (TARGET_SIMD)
#define HAVE_aarch64_rev32v2di (TARGET_SIMD)
#define HAVE_aarch64_rev16v2di (TARGET_SIMD)
#define HAVE_aarch64_rev64v4hf (TARGET_SIMD)
#define HAVE_aarch64_rev32v4hf (TARGET_SIMD)
#define HAVE_aarch64_rev16v4hf (TARGET_SIMD)
#define HAVE_aarch64_rev64v8hf (TARGET_SIMD)
#define HAVE_aarch64_rev32v8hf (TARGET_SIMD)
#define HAVE_aarch64_rev16v8hf (TARGET_SIMD)
#define HAVE_aarch64_rev64v2sf (TARGET_SIMD)
#define HAVE_aarch64_rev32v2sf (TARGET_SIMD)
#define HAVE_aarch64_rev16v2sf (TARGET_SIMD)
#define HAVE_aarch64_rev64v4sf (TARGET_SIMD)
#define HAVE_aarch64_rev32v4sf (TARGET_SIMD)
#define HAVE_aarch64_rev16v4sf (TARGET_SIMD)
#define HAVE_aarch64_rev64v2df (TARGET_SIMD)
#define HAVE_aarch64_rev32v2df (TARGET_SIMD)
#define HAVE_aarch64_rev16v2df (TARGET_SIMD)
#define HAVE_aarch64_st2v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2di_dreg (TARGET_SIMD)
#define HAVE_aarch64_st2df_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3di_dreg (TARGET_SIMD)
#define HAVE_aarch64_st3df_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4v8qi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4v4hi_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4v4hf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4v2si_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4v2sf_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4di_dreg (TARGET_SIMD)
#define HAVE_aarch64_st4df_dreg (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v16qi_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v8hi_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v4si_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v2di_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v8hf_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v4sf_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v2df_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v8qi_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v4hi_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v4hf_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v2si_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1v2sf_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1di_x2 (TARGET_SIMD)
#define HAVE_aarch64_simd_ld1df_x2 (TARGET_SIMD)
#define HAVE_aarch64_frecpev4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_frecpev8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_frecpev2sf (TARGET_SIMD)
#define HAVE_aarch64_frecpev4sf (TARGET_SIMD)
#define HAVE_aarch64_frecpev2df (TARGET_SIMD)
#define HAVE_aarch64_frecpehf ((TARGET_SIMD) && (AARCH64_ISA_F16))
#define HAVE_aarch64_frecpxhf ((TARGET_SIMD) && (AARCH64_ISA_F16))
#define HAVE_aarch64_frecpesf (TARGET_SIMD)
#define HAVE_aarch64_frecpxsf (TARGET_SIMD)
#define HAVE_aarch64_frecpedf (TARGET_SIMD)
#define HAVE_aarch64_frecpxdf (TARGET_SIMD)
#define HAVE_aarch64_frecpsv4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_frecpsv8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_frecpsv2sf (TARGET_SIMD)
#define HAVE_aarch64_frecpsv4sf (TARGET_SIMD)
#define HAVE_aarch64_frecpsv2df (TARGET_SIMD)
#define HAVE_aarch64_frecpshf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_aarch64_frecpssf (TARGET_SIMD)
#define HAVE_aarch64_frecpsdf (TARGET_SIMD)
#define HAVE_aarch64_urecpev2si (TARGET_SIMD)
#define HAVE_aarch64_urecpev4si (TARGET_SIMD)
#define HAVE_aarch64_crypto_aesev16qi (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_crypto_aesdv16qi (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_crypto_aesmcv16qi (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_crypto_aesimcv16qi (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_crypto_sha1hsi (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha1hv4si (TARGET_SIMD && TARGET_SHA2 && !BYTES_BIG_ENDIAN)
#define HAVE_aarch64_be_crypto_sha1hv4si (TARGET_SIMD && TARGET_SHA2 && BYTES_BIG_ENDIAN)
#define HAVE_aarch64_crypto_sha1su1v4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha1cv4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha1mv4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha1pv4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha1su0v4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha256hv4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha256h2v4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha256su0v4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha256su1v4si (TARGET_SIMD && TARGET_SHA2)
#define HAVE_aarch64_crypto_sha512hqv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_crypto_sha512h2qv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_crypto_sha512su0qv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_crypto_sha512su1qv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_eor3qv8hi (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_rax1qv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_xarqv2di (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_bcaxqv8hi (TARGET_SIMD && TARGET_SHA3)
#define HAVE_aarch64_sm3ss1qv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3tt1aqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3tt1bqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3tt2aqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3tt2bqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3partw1qv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm3partw2qv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm4eqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_sm4ekeyqv4si (TARGET_SIMD && TARGET_SM4)
#define HAVE_aarch64_simd_fmlal_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlal_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlal_lane_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_lane_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlal_lane_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_lane_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_laneq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_laneq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_laneq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_laneq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlal_laneq_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_laneq_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlal_laneq_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlsl_laneq_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_lane_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_lane_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlalq_lane_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_simd_fmlslq_lane_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_crypto_pmulldi (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_crypto_pmullv2di (TARGET_SIMD && TARGET_AES)
#define HAVE_aarch64_compare_and_swapqi 1
#define HAVE_aarch64_compare_and_swaphi 1
#define HAVE_aarch64_compare_and_swapsi 1
#define HAVE_aarch64_compare_and_swapdi 1
#define HAVE_aarch64_compare_and_swapqi_lse (TARGET_LSE)
#define HAVE_aarch64_compare_and_swaphi_lse (TARGET_LSE)
#define HAVE_aarch64_compare_and_swapsi_lse (TARGET_LSE)
#define HAVE_aarch64_compare_and_swapdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_exchangeqi 1
#define HAVE_aarch64_atomic_exchangehi 1
#define HAVE_aarch64_atomic_exchangesi 1
#define HAVE_aarch64_atomic_exchangedi 1
#define HAVE_aarch64_atomic_exchangeqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_exchangehi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_exchangesi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_exchangedi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_addqi 1
#define HAVE_aarch64_atomic_subqi 1
#define HAVE_aarch64_atomic_orqi 1
#define HAVE_aarch64_atomic_xorqi 1
#define HAVE_aarch64_atomic_andqi 1
#define HAVE_aarch64_atomic_addhi 1
#define HAVE_aarch64_atomic_subhi 1
#define HAVE_aarch64_atomic_orhi 1
#define HAVE_aarch64_atomic_xorhi 1
#define HAVE_aarch64_atomic_andhi 1
#define HAVE_aarch64_atomic_addsi 1
#define HAVE_aarch64_atomic_subsi 1
#define HAVE_aarch64_atomic_orsi 1
#define HAVE_aarch64_atomic_xorsi 1
#define HAVE_aarch64_atomic_andsi 1
#define HAVE_aarch64_atomic_adddi 1
#define HAVE_aarch64_atomic_subdi 1
#define HAVE_aarch64_atomic_ordi 1
#define HAVE_aarch64_atomic_xordi 1
#define HAVE_aarch64_atomic_anddi 1
#define HAVE_aarch64_atomic_addqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_subqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_orqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xorqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_andqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_addhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_subhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_orhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xorhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_andhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_addsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_subsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_orsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xorsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_andsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_adddi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_subdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_ordi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xordi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_anddi_lse (TARGET_LSE)
#define HAVE_atomic_nandqi 1
#define HAVE_atomic_nandhi 1
#define HAVE_atomic_nandsi 1
#define HAVE_atomic_nanddi 1
#define HAVE_aarch64_atomic_fetch_addqi 1
#define HAVE_aarch64_atomic_fetch_subqi 1
#define HAVE_aarch64_atomic_fetch_orqi 1
#define HAVE_aarch64_atomic_fetch_xorqi 1
#define HAVE_aarch64_atomic_fetch_andqi 1
#define HAVE_aarch64_atomic_fetch_addhi 1
#define HAVE_aarch64_atomic_fetch_subhi 1
#define HAVE_aarch64_atomic_fetch_orhi 1
#define HAVE_aarch64_atomic_fetch_xorhi 1
#define HAVE_aarch64_atomic_fetch_andhi 1
#define HAVE_aarch64_atomic_fetch_addsi 1
#define HAVE_aarch64_atomic_fetch_subsi 1
#define HAVE_aarch64_atomic_fetch_orsi 1
#define HAVE_aarch64_atomic_fetch_xorsi 1
#define HAVE_aarch64_atomic_fetch_andsi 1
#define HAVE_aarch64_atomic_fetch_adddi 1
#define HAVE_aarch64_atomic_fetch_subdi 1
#define HAVE_aarch64_atomic_fetch_ordi 1
#define HAVE_aarch64_atomic_fetch_xordi 1
#define HAVE_aarch64_atomic_fetch_anddi 1
#define HAVE_aarch64_atomic_fetch_addqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_subqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_orqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_xorqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_andqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_addhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_subhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_orhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_xorhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_andhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_addsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_subsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_orsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_xorsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_andsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_adddi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_subdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_ordi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_xordi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_fetch_anddi_lse (TARGET_LSE)
#define HAVE_atomic_fetch_nandqi 1
#define HAVE_atomic_fetch_nandhi 1
#define HAVE_atomic_fetch_nandsi 1
#define HAVE_atomic_fetch_nanddi 1
#define HAVE_aarch64_atomic_add_fetchqi 1
#define HAVE_aarch64_atomic_sub_fetchqi 1
#define HAVE_aarch64_atomic_or_fetchqi 1
#define HAVE_aarch64_atomic_xor_fetchqi 1
#define HAVE_aarch64_atomic_and_fetchqi 1
#define HAVE_aarch64_atomic_add_fetchhi 1
#define HAVE_aarch64_atomic_sub_fetchhi 1
#define HAVE_aarch64_atomic_or_fetchhi 1
#define HAVE_aarch64_atomic_xor_fetchhi 1
#define HAVE_aarch64_atomic_and_fetchhi 1
#define HAVE_aarch64_atomic_add_fetchsi 1
#define HAVE_aarch64_atomic_sub_fetchsi 1
#define HAVE_aarch64_atomic_or_fetchsi 1
#define HAVE_aarch64_atomic_xor_fetchsi 1
#define HAVE_aarch64_atomic_and_fetchsi 1
#define HAVE_aarch64_atomic_add_fetchdi 1
#define HAVE_aarch64_atomic_sub_fetchdi 1
#define HAVE_aarch64_atomic_or_fetchdi 1
#define HAVE_aarch64_atomic_xor_fetchdi 1
#define HAVE_aarch64_atomic_and_fetchdi 1
#define HAVE_aarch64_atomic_add_fetchqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_sub_fetchqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_or_fetchqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xor_fetchqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_and_fetchqi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_add_fetchhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_sub_fetchhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_or_fetchhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xor_fetchhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_and_fetchhi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_add_fetchsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_sub_fetchsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_or_fetchsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xor_fetchsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_and_fetchsi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_add_fetchdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_sub_fetchdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_or_fetchdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_xor_fetchdi_lse (TARGET_LSE)
#define HAVE_aarch64_atomic_and_fetchdi_lse (TARGET_LSE)
#define HAVE_atomic_nand_fetchqi 1
#define HAVE_atomic_nand_fetchhi 1
#define HAVE_atomic_nand_fetchsi 1
#define HAVE_atomic_nand_fetchdi 1
#define HAVE_atomic_loadqi 1
#define HAVE_atomic_loadhi 1
#define HAVE_atomic_loadsi 1
#define HAVE_atomic_loaddi 1
#define HAVE_atomic_storeqi 1
#define HAVE_atomic_storehi 1
#define HAVE_atomic_storesi 1
#define HAVE_atomic_storedi 1
#define HAVE_aarch64_load_exclusiveqi 1
#define HAVE_aarch64_load_exclusivehi 1
#define HAVE_aarch64_load_exclusivesi 1
#define HAVE_aarch64_load_exclusivedi 1
#define HAVE_aarch64_store_exclusiveqi 1
#define HAVE_aarch64_store_exclusivehi 1
#define HAVE_aarch64_store_exclusivesi 1
#define HAVE_aarch64_store_exclusivedi 1
#define HAVE_aarch64_atomic_swpqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_swphi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_swpsi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_swpdi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_casqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_cashi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_cassi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_casdi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadsetqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadclrqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadeorqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadaddqi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadsethi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadclrhi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadeorhi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadaddhi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadsetsi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadclrsi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadeorsi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadaddsi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadsetdi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadclrdi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadeordi (TARGET_LSE && reload_completed)
#define HAVE_aarch64_atomic_loadadddi (TARGET_LSE && reload_completed)
#define HAVE_maskloadvnx16qivnx16bi (TARGET_SVE)
#define HAVE_maskloadvnx8hivnx8bi (TARGET_SVE)
#define HAVE_maskloadvnx4sivnx4bi (TARGET_SVE)
#define HAVE_maskloadvnx2divnx2bi (TARGET_SVE)
#define HAVE_maskloadvnx8hfvnx8bi (TARGET_SVE)
#define HAVE_maskloadvnx4sfvnx4bi (TARGET_SVE)
#define HAVE_maskloadvnx2dfvnx2bi (TARGET_SVE)
#define HAVE_maskstorevnx16qivnx16bi (TARGET_SVE)
#define HAVE_maskstorevnx8hivnx8bi (TARGET_SVE)
#define HAVE_maskstorevnx4sivnx4bi (TARGET_SVE)
#define HAVE_maskstorevnx2divnx2bi (TARGET_SVE)
#define HAVE_maskstorevnx8hfvnx8bi (TARGET_SVE)
#define HAVE_maskstorevnx4sfvnx4bi (TARGET_SVE)
#define HAVE_maskstorevnx2dfvnx2bi (TARGET_SVE)
#define HAVE_mask_gather_loadvnx4si (TARGET_SVE)
#define HAVE_mask_gather_loadvnx4sf (TARGET_SVE)
#define HAVE_mask_gather_loadvnx2di (TARGET_SVE)
#define HAVE_mask_gather_loadvnx2df (TARGET_SVE)
#define HAVE_mask_scatter_storevnx4si (TARGET_SVE)
#define HAVE_mask_scatter_storevnx4sf (TARGET_SVE)
#define HAVE_mask_scatter_storevnx2di (TARGET_SVE)
#define HAVE_mask_scatter_storevnx2df (TARGET_SVE)
#define HAVE_pred_movvnx32qi (TARGET_SVE \
   && (register_operand (operands[0], VNx32QImode) \
       || register_operand (operands[2], VNx32QImode)))
#define HAVE_pred_movvnx16hi (TARGET_SVE \
   && (register_operand (operands[0], VNx16HImode) \
       || register_operand (operands[2], VNx16HImode)))
#define HAVE_pred_movvnx8si (TARGET_SVE \
   && (register_operand (operands[0], VNx8SImode) \
       || register_operand (operands[2], VNx8SImode)))
#define HAVE_pred_movvnx4di (TARGET_SVE \
   && (register_operand (operands[0], VNx4DImode) \
       || register_operand (operands[2], VNx4DImode)))
#define HAVE_pred_movvnx16hf (TARGET_SVE \
   && (register_operand (operands[0], VNx16HFmode) \
       || register_operand (operands[2], VNx16HFmode)))
#define HAVE_pred_movvnx8sf (TARGET_SVE \
   && (register_operand (operands[0], VNx8SFmode) \
       || register_operand (operands[2], VNx8SFmode)))
#define HAVE_pred_movvnx4df (TARGET_SVE \
   && (register_operand (operands[0], VNx4DFmode) \
       || register_operand (operands[2], VNx4DFmode)))
#define HAVE_pred_movvnx48qi (TARGET_SVE \
   && (register_operand (operands[0], VNx48QImode) \
       || register_operand (operands[2], VNx48QImode)))
#define HAVE_pred_movvnx24hi (TARGET_SVE \
   && (register_operand (operands[0], VNx24HImode) \
       || register_operand (operands[2], VNx24HImode)))
#define HAVE_pred_movvnx12si (TARGET_SVE \
   && (register_operand (operands[0], VNx12SImode) \
       || register_operand (operands[2], VNx12SImode)))
#define HAVE_pred_movvnx6di (TARGET_SVE \
   && (register_operand (operands[0], VNx6DImode) \
       || register_operand (operands[2], VNx6DImode)))
#define HAVE_pred_movvnx24hf (TARGET_SVE \
   && (register_operand (operands[0], VNx24HFmode) \
       || register_operand (operands[2], VNx24HFmode)))
#define HAVE_pred_movvnx12sf (TARGET_SVE \
   && (register_operand (operands[0], VNx12SFmode) \
       || register_operand (operands[2], VNx12SFmode)))
#define HAVE_pred_movvnx6df (TARGET_SVE \
   && (register_operand (operands[0], VNx6DFmode) \
       || register_operand (operands[2], VNx6DFmode)))
#define HAVE_pred_movvnx64qi (TARGET_SVE \
   && (register_operand (operands[0], VNx64QImode) \
       || register_operand (operands[2], VNx64QImode)))
#define HAVE_pred_movvnx32hi (TARGET_SVE \
   && (register_operand (operands[0], VNx32HImode) \
       || register_operand (operands[2], VNx32HImode)))
#define HAVE_pred_movvnx16si (TARGET_SVE \
   && (register_operand (operands[0], VNx16SImode) \
       || register_operand (operands[2], VNx16SImode)))
#define HAVE_pred_movvnx8di (TARGET_SVE \
   && (register_operand (operands[0], VNx8DImode) \
       || register_operand (operands[2], VNx8DImode)))
#define HAVE_pred_movvnx32hf (TARGET_SVE \
   && (register_operand (operands[0], VNx32HFmode) \
       || register_operand (operands[2], VNx32HFmode)))
#define HAVE_pred_movvnx16sf (TARGET_SVE \
   && (register_operand (operands[0], VNx16SFmode) \
       || register_operand (operands[2], VNx16SFmode)))
#define HAVE_pred_movvnx8df (TARGET_SVE \
   && (register_operand (operands[0], VNx8DFmode) \
       || register_operand (operands[2], VNx8DFmode)))
#define HAVE_extract_last_vnx16qi (TARGET_SVE)
#define HAVE_extract_last_vnx8hi (TARGET_SVE)
#define HAVE_extract_last_vnx4si (TARGET_SVE)
#define HAVE_extract_last_vnx2di (TARGET_SVE)
#define HAVE_extract_last_vnx8hf (TARGET_SVE)
#define HAVE_extract_last_vnx4sf (TARGET_SVE)
#define HAVE_extract_last_vnx2df (TARGET_SVE)
#define HAVE_sve_ld1rvnx16qi (TARGET_SVE)
#define HAVE_sve_ld1rvnx8hi (TARGET_SVE)
#define HAVE_sve_ld1rvnx4si (TARGET_SVE)
#define HAVE_sve_ld1rvnx2di (TARGET_SVE)
#define HAVE_sve_ld1rvnx8hf (TARGET_SVE)
#define HAVE_sve_ld1rvnx4sf (TARGET_SVE)
#define HAVE_sve_ld1rvnx2df (TARGET_SVE)
#define HAVE_vec_seriesvnx16qi (TARGET_SVE)
#define HAVE_vec_seriesvnx8hi (TARGET_SVE)
#define HAVE_vec_seriesvnx4si (TARGET_SVE)
#define HAVE_vec_seriesvnx2di (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx32qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx16hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx8sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx4divnx2di (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx16hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx8sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx4dfvnx2df (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx48qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx24hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx12sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx6divnx2di (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx24hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx12sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx6dfvnx2df (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx64qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx32hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx16sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx8divnx2di (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx32hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx16sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_load_lanesvnx8dfvnx2df (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx32qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx16hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx8sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx4divnx2di (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx16hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx8sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx4dfvnx2df (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx48qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx24hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx12sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx6divnx2di (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx24hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx12sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx6dfvnx2df (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx64qivnx16qi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx32hivnx8hi (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx16sivnx4si (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx8divnx2di (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx32hfvnx8hf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx16sfvnx4sf (TARGET_SVE)
#define HAVE_vec_mask_store_lanesvnx8dfvnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx2di (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx8hf (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx4sf (TARGET_SVE)
#define HAVE_aarch64_sve_zip1vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_zip2vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_trn1vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_trn2vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_uzp1vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_uzp2vnx2df (TARGET_SVE)
#define HAVE_addvnx16qi3 (TARGET_SVE)
#define HAVE_addvnx8hi3 (TARGET_SVE)
#define HAVE_addvnx4si3 (TARGET_SVE)
#define HAVE_addvnx2di3 (TARGET_SVE)
#define HAVE_subvnx16qi3 (TARGET_SVE)
#define HAVE_subvnx8hi3 (TARGET_SVE)
#define HAVE_subvnx4si3 (TARGET_SVE)
#define HAVE_subvnx2di3 (TARGET_SVE)
#define HAVE_andvnx16qi3 (TARGET_SVE)
#define HAVE_iorvnx16qi3 (TARGET_SVE)
#define HAVE_xorvnx16qi3 (TARGET_SVE)
#define HAVE_andvnx8hi3 (TARGET_SVE)
#define HAVE_iorvnx8hi3 (TARGET_SVE)
#define HAVE_xorvnx8hi3 (TARGET_SVE)
#define HAVE_andvnx4si3 (TARGET_SVE)
#define HAVE_iorvnx4si3 (TARGET_SVE)
#define HAVE_xorvnx4si3 (TARGET_SVE)
#define HAVE_andvnx2di3 (TARGET_SVE)
#define HAVE_iorvnx2di3 (TARGET_SVE)
#define HAVE_xorvnx2di3 (TARGET_SVE)
#define HAVE_bicvnx16qi3 (TARGET_SVE)
#define HAVE_bicvnx8hi3 (TARGET_SVE)
#define HAVE_bicvnx4si3 (TARGET_SVE)
#define HAVE_bicvnx2di3 (TARGET_SVE)
#define HAVE_andvnx16bi3 (TARGET_SVE)
#define HAVE_andvnx8bi3 (TARGET_SVE)
#define HAVE_andvnx4bi3 (TARGET_SVE)
#define HAVE_andvnx2bi3 (TARGET_SVE)
#define HAVE_pred_andvnx16bi3 (TARGET_SVE)
#define HAVE_pred_iorvnx16bi3 (TARGET_SVE)
#define HAVE_pred_xorvnx16bi3 (TARGET_SVE)
#define HAVE_pred_andvnx8bi3 (TARGET_SVE)
#define HAVE_pred_iorvnx8bi3 (TARGET_SVE)
#define HAVE_pred_xorvnx8bi3 (TARGET_SVE)
#define HAVE_pred_andvnx4bi3 (TARGET_SVE)
#define HAVE_pred_iorvnx4bi3 (TARGET_SVE)
#define HAVE_pred_xorvnx4bi3 (TARGET_SVE)
#define HAVE_pred_andvnx2bi3 (TARGET_SVE)
#define HAVE_pred_iorvnx2bi3 (TARGET_SVE)
#define HAVE_pred_xorvnx2bi3 (TARGET_SVE)
#define HAVE_ptest_ptruevnx16bi (TARGET_SVE)
#define HAVE_ptest_ptruevnx8bi (TARGET_SVE)
#define HAVE_ptest_ptruevnx4bi (TARGET_SVE)
#define HAVE_ptest_ptruevnx2bi (TARGET_SVE)
#define HAVE_while_ultsivnx16bi (TARGET_SVE)
#define HAVE_while_ultdivnx16bi (TARGET_SVE)
#define HAVE_while_ultsivnx8bi (TARGET_SVE)
#define HAVE_while_ultdivnx8bi (TARGET_SVE)
#define HAVE_while_ultsivnx4bi (TARGET_SVE)
#define HAVE_while_ultdivnx4bi (TARGET_SVE)
#define HAVE_while_ultsivnx2bi (TARGET_SVE)
#define HAVE_while_ultdivnx2bi (TARGET_SVE)
#define HAVE_while_ultsivnx16bi_cc (TARGET_SVE)
#define HAVE_while_ultdivnx16bi_cc (TARGET_SVE)
#define HAVE_while_ultsivnx8bi_cc (TARGET_SVE)
#define HAVE_while_ultdivnx8bi_cc (TARGET_SVE)
#define HAVE_while_ultsivnx4bi_cc (TARGET_SVE)
#define HAVE_while_ultdivnx4bi_cc (TARGET_SVE)
#define HAVE_while_ultsivnx2bi_cc (TARGET_SVE)
#define HAVE_while_ultdivnx2bi_cc (TARGET_SVE)
#define HAVE_vcond_mask_vnx16qivnx16bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx8hivnx8bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx4sivnx4bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx2divnx2bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx8hfvnx8bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx4sfvnx4bi (TARGET_SVE)
#define HAVE_vcond_mask_vnx2dfvnx2bi (TARGET_SVE)
#define HAVE_aarch64_sve_dupvnx16qi_const (TARGET_SVE)
#define HAVE_aarch64_sve_dupvnx8hi_const (TARGET_SVE)
#define HAVE_aarch64_sve_dupvnx4si_const (TARGET_SVE)
#define HAVE_aarch64_sve_dupvnx2di_const (TARGET_SVE)
#define HAVE_cond_addvnx16qi (TARGET_SVE)
#define HAVE_cond_subvnx16qi (TARGET_SVE)
#define HAVE_cond_smaxvnx16qi (TARGET_SVE)
#define HAVE_cond_umaxvnx16qi (TARGET_SVE)
#define HAVE_cond_sminvnx16qi (TARGET_SVE)
#define HAVE_cond_uminvnx16qi (TARGET_SVE)
#define HAVE_cond_andvnx16qi (TARGET_SVE)
#define HAVE_cond_iorvnx16qi (TARGET_SVE)
#define HAVE_cond_xorvnx16qi (TARGET_SVE)
#define HAVE_cond_addvnx8hi (TARGET_SVE)
#define HAVE_cond_subvnx8hi (TARGET_SVE)
#define HAVE_cond_smaxvnx8hi (TARGET_SVE)
#define HAVE_cond_umaxvnx8hi (TARGET_SVE)
#define HAVE_cond_sminvnx8hi (TARGET_SVE)
#define HAVE_cond_uminvnx8hi (TARGET_SVE)
#define HAVE_cond_andvnx8hi (TARGET_SVE)
#define HAVE_cond_iorvnx8hi (TARGET_SVE)
#define HAVE_cond_xorvnx8hi (TARGET_SVE)
#define HAVE_cond_addvnx4si (TARGET_SVE)
#define HAVE_cond_subvnx4si (TARGET_SVE)
#define HAVE_cond_smaxvnx4si (TARGET_SVE)
#define HAVE_cond_umaxvnx4si (TARGET_SVE)
#define HAVE_cond_sminvnx4si (TARGET_SVE)
#define HAVE_cond_uminvnx4si (TARGET_SVE)
#define HAVE_cond_andvnx4si (TARGET_SVE)
#define HAVE_cond_iorvnx4si (TARGET_SVE)
#define HAVE_cond_xorvnx4si (TARGET_SVE)
#define HAVE_cond_addvnx2di (TARGET_SVE)
#define HAVE_cond_subvnx2di (TARGET_SVE)
#define HAVE_cond_smaxvnx2di (TARGET_SVE)
#define HAVE_cond_umaxvnx2di (TARGET_SVE)
#define HAVE_cond_sminvnx2di (TARGET_SVE)
#define HAVE_cond_uminvnx2di (TARGET_SVE)
#define HAVE_cond_andvnx2di (TARGET_SVE)
#define HAVE_cond_iorvnx2di (TARGET_SVE)
#define HAVE_cond_xorvnx2di (TARGET_SVE)
#define HAVE_fold_extract_last_vnx16qi (TARGET_SVE)
#define HAVE_fold_extract_last_vnx8hi (TARGET_SVE)
#define HAVE_fold_extract_last_vnx4si (TARGET_SVE)
#define HAVE_fold_extract_last_vnx2di (TARGET_SVE)
#define HAVE_fold_extract_last_vnx8hf (TARGET_SVE)
#define HAVE_fold_extract_last_vnx4sf (TARGET_SVE)
#define HAVE_fold_extract_last_vnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_floatvnx4sivnx2df2 (TARGET_SVE)
#define HAVE_aarch64_sve_floatunsvnx4sivnx2df2 (TARGET_SVE)
#define HAVE_aarch64_sve_floatvnx2divnx2df2 (TARGET_SVE)
#define HAVE_aarch64_sve_floatunsvnx2divnx2df2 (TARGET_SVE)
#define HAVE_aarch64_sve_extendvnx8hfvnx4sf2 (TARGET_SVE)
#define HAVE_aarch64_sve_extendvnx4sfvnx2df2 (TARGET_SVE)
#define HAVE_aarch64_sve_punpklo_vnx16bi (TARGET_SVE)
#define HAVE_aarch64_sve_punpkhi_vnx16bi (TARGET_SVE)
#define HAVE_aarch64_sve_punpklo_vnx8bi (TARGET_SVE)
#define HAVE_aarch64_sve_punpkhi_vnx8bi (TARGET_SVE)
#define HAVE_aarch64_sve_punpklo_vnx4bi (TARGET_SVE)
#define HAVE_aarch64_sve_punpkhi_vnx4bi (TARGET_SVE)
#define HAVE_aarch64_sve_sunpkhi_vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_uunpkhi_vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_sunpklo_vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_uunpklo_vnx16qi (TARGET_SVE)
#define HAVE_aarch64_sve_sunpkhi_vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_uunpkhi_vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_sunpklo_vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_uunpklo_vnx8hi (TARGET_SVE)
#define HAVE_aarch64_sve_sunpkhi_vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_uunpkhi_vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_sunpklo_vnx4si (TARGET_SVE)
#define HAVE_aarch64_sve_uunpklo_vnx4si (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx8bi (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx4bi (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx2bi (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx8hi (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx4si (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx2di (TARGET_SVE)
#define HAVE_cond_addvnx8hf (TARGET_SVE)
#define HAVE_cond_subvnx8hf (TARGET_SVE)
#define HAVE_cond_addvnx4sf (TARGET_SVE)
#define HAVE_cond_subvnx4sf (TARGET_SVE)
#define HAVE_cond_addvnx2df (TARGET_SVE)
#define HAVE_cond_subvnx2df (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx16qi (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx8hi (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx4si (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx2di (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx8hf (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx4sf (TARGET_SVE)
#define HAVE_vec_shl_insert_vnx2df (TARGET_SVE)
#define HAVE_cbranchsi4 1
#define HAVE_cbranchdi4 1
#define HAVE_cbranchsf4 1
#define HAVE_cbranchdf4 1
#define HAVE_cbranchcc4 1
#define HAVE_modsi3 1
#define HAVE_moddi3 1
#define HAVE_casesi 1
#define HAVE_prologue 1
#define HAVE_epilogue 1
#define HAVE_sibcall_epilogue 1
#define HAVE_return (aarch64_use_return_insn_p ())
#define HAVE_call 1
#define HAVE_call_value 1
#define HAVE_sibcall 1
#define HAVE_sibcall_value 1
#define HAVE_untyped_call 1
#define HAVE_movqi 1
#define HAVE_movhi 1
#define HAVE_movsi 1
#define HAVE_movdi 1
#define HAVE_movti 1
#define HAVE_movhf 1
#define HAVE_movsf 1
#define HAVE_movdf 1
#define HAVE_movtf 1
#define HAVE_movmemdi (!STRICT_ALIGNMENT)
#define HAVE_extendsidi2 1
#define HAVE_zero_extendsidi2 1
#define HAVE_extendqisi2 1
#define HAVE_zero_extendqisi2 1
#define HAVE_extendqidi2 1
#define HAVE_zero_extendqidi2 1
#define HAVE_extendhisi2 1
#define HAVE_zero_extendhisi2 1
#define HAVE_extendhidi2 1
#define HAVE_zero_extendhidi2 1
#define HAVE_extendqihi2 1
#define HAVE_zero_extendqihi2 1
#define HAVE_addsi3 1
#define HAVE_adddi3 1
#define HAVE_addti3 1
#define HAVE_addsi3_carryin 1
#define HAVE_adddi3_carryin 1
#define HAVE_subti3 1
#define HAVE_subsi3_carryin 1
#define HAVE_subdi3_carryin 1
#define HAVE_abssi2 1
#define HAVE_absdi2 1
#define HAVE_mulditi3 1
#define HAVE_umulditi3 1
#define HAVE_multi3 1
#define HAVE_cstoresi4 1
#define HAVE_cstoredi4 1
#define HAVE_cstorecc4 1
#define HAVE_cstoresf4 1
#define HAVE_cstoredf4 1
#define HAVE_cmovsi6 1
#define HAVE_cmovdi6 1
#define HAVE_cmovsf6 1
#define HAVE_cmovdf6 1
#define HAVE_movqicc 1
#define HAVE_movhicc 1
#define HAVE_movsicc 1
#define HAVE_movdicc 1
#define HAVE_movsfsicc 1
#define HAVE_movsfdicc 1
#define HAVE_movdfsicc 1
#define HAVE_movdfdicc 1
#define HAVE_movsfcc 1
#define HAVE_movdfcc 1
#define HAVE_negsicc 1
#define HAVE_notsicc 1
#define HAVE_negdicc 1
#define HAVE_notdicc 1
#define HAVE_umaxsi3 (TARGET_SVE)
#define HAVE_umaxdi3 (TARGET_SVE)
#define HAVE_ffssi2 1
#define HAVE_ffsdi2 1
#define HAVE_popcountsi2 (TARGET_SIMD)
#define HAVE_popcountdi2 (TARGET_SIMD)
#define HAVE_ashlsi3 1
#define HAVE_ashrsi3 1
#define HAVE_lshrsi3 1
#define HAVE_ashldi3 1
#define HAVE_ashrdi3 1
#define HAVE_lshrdi3 1
#define HAVE_ashlqi3 1
#define HAVE_ashlhi3 1
#define HAVE_rotrsi3 1
#define HAVE_rotrdi3 1
#define HAVE_rotlsi3 1
#define HAVE_rotldi3 1
#define HAVE_extv 1
#define HAVE_extzv 1
#define HAVE_insvsi 1
#define HAVE_insvdi 1
#define HAVE_floatsihf2 (TARGET_FLOAT)
#define HAVE_floatunssihf2 (TARGET_FLOAT)
#define HAVE_floatdihf2 (TARGET_FLOAT && (TARGET_FP_F16INST || TARGET_SIMD))
#define HAVE_floatunsdihf2 (TARGET_FLOAT && (TARGET_FP_F16INST || TARGET_SIMD))
#define HAVE_divhf3 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_divsf3 (TARGET_FLOAT)
#define HAVE_divdf3 (TARGET_FLOAT)
#define HAVE_sqrthf2 ((TARGET_FLOAT) && (AARCH64_ISA_F16))
#define HAVE_sqrtsf2 (TARGET_FLOAT)
#define HAVE_sqrtdf2 (TARGET_FLOAT)
#define HAVE_lrintsfsi2 (TARGET_FLOAT \
   && ((GET_MODE_SIZE (SFmode) <= GET_MODE_SIZE (SImode)) \
   || !flag_trapping_math || flag_fp_int_builtin_inexact))
#define HAVE_lrintsfdi2 (TARGET_FLOAT \
   && ((GET_MODE_SIZE (SFmode) <= GET_MODE_SIZE (DImode)) \
   || !flag_trapping_math || flag_fp_int_builtin_inexact))
#define HAVE_lrintdfsi2 (TARGET_FLOAT \
   && ((GET_MODE_SIZE (DFmode) <= GET_MODE_SIZE (SImode)) \
   || !flag_trapping_math || flag_fp_int_builtin_inexact))
#define HAVE_lrintdfdi2 (TARGET_FLOAT \
   && ((GET_MODE_SIZE (DFmode) <= GET_MODE_SIZE (DImode)) \
   || !flag_trapping_math || flag_fp_int_builtin_inexact))
#define HAVE_copysigndf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_copysignsf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_xorsignsf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_xorsigndf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_aarch64_reload_movcpsfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpsfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpdfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpdfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcptfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcptfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv8qisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv16qisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv4hisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv8hisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv2sisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv4sisi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv2disi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv2sfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv4sfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv2dfsi ((TARGET_FLOAT) && (ptr_mode == SImode || Pmode == SImode))
#define HAVE_aarch64_reload_movcpv8qidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv16qidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv4hidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv8hidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv2sidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv4sidi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv2didi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv2sfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv4sfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movcpv2dfdi ((TARGET_FLOAT) && (ptr_mode == DImode || Pmode == DImode))
#define HAVE_aarch64_reload_movti (TARGET_FLOAT)
#define HAVE_aarch64_reload_movtf (TARGET_FLOAT)
#define HAVE_add_losym 1
#define HAVE_tlsgd_small_si (ptr_mode == SImode)
#define HAVE_tlsgd_small_di (ptr_mode == DImode)
#define HAVE_tlsdesc_small_si ((TARGET_TLS_DESC) && (ptr_mode == SImode))
#define HAVE_tlsdesc_small_di ((TARGET_TLS_DESC) && (ptr_mode == DImode))
#define HAVE_get_thread_pointerdi 1
#define HAVE_stack_protect_set 1
#define HAVE_stack_protect_test 1
#define HAVE_doloop_end (optimize > 0 && flag_modulo_sched)
#define HAVE_set_clobber_cc 1
#define HAVE_despeculate_copyqi 1
#define HAVE_despeculate_copyhi 1
#define HAVE_despeculate_copysi 1
#define HAVE_despeculate_copydi 1
#define HAVE_despeculate_copyti 1
#define HAVE_movv8qi (TARGET_SIMD)
#define HAVE_movv16qi (TARGET_SIMD)
#define HAVE_movv4hi (TARGET_SIMD)
#define HAVE_movv8hi (TARGET_SIMD)
#define HAVE_movv2si (TARGET_SIMD)
#define HAVE_movv4si (TARGET_SIMD)
#define HAVE_movv2di (TARGET_SIMD)
#define HAVE_movv4hf (TARGET_SIMD)
#define HAVE_movv8hf (TARGET_SIMD)
#define HAVE_movv2sf (TARGET_SIMD)
#define HAVE_movv4sf (TARGET_SIMD)
#define HAVE_movv2df (TARGET_SIMD)
#define HAVE_movmisalignv8qi (TARGET_SIMD)
#define HAVE_movmisalignv16qi (TARGET_SIMD)
#define HAVE_movmisalignv4hi (TARGET_SIMD)
#define HAVE_movmisalignv8hi (TARGET_SIMD)
#define HAVE_movmisalignv2si (TARGET_SIMD)
#define HAVE_movmisalignv4si (TARGET_SIMD)
#define HAVE_movmisalignv2di (TARGET_SIMD)
#define HAVE_movmisalignv2sf (TARGET_SIMD)
#define HAVE_movmisalignv4sf (TARGET_SIMD)
#define HAVE_movmisalignv2df (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv16qi (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv8hi (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv4si (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv2di (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv8hf (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv4sf (TARGET_SIMD)
#define HAVE_aarch64_split_simd_movv2df (TARGET_SIMD)
#define HAVE_ctzv2si2 (TARGET_SIMD)
#define HAVE_ctzv4si2 (TARGET_SIMD)
#define HAVE_xorsignv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_xorsignv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_xorsignv2sf3 (TARGET_SIMD)
#define HAVE_xorsignv4sf3 (TARGET_SIMD)
#define HAVE_xorsignv2df3 (TARGET_SIMD)
#define HAVE_sdot_prodv8qi (TARGET_DOTPROD)
#define HAVE_udot_prodv8qi (TARGET_DOTPROD)
#define HAVE_sdot_prodv16qi (TARGET_DOTPROD)
#define HAVE_udot_prodv16qi (TARGET_DOTPROD)
#define HAVE_copysignv4hf3 ((TARGET_FLOAT && TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_copysignv8hf3 ((TARGET_FLOAT && TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_copysignv2sf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_copysignv4sf3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_copysignv2df3 (TARGET_FLOAT && TARGET_SIMD)
#define HAVE_rsqrtv2sf2 (TARGET_SIMD)
#define HAVE_rsqrtv4sf2 (TARGET_SIMD)
#define HAVE_rsqrtv2df2 (TARGET_SIMD)
#define HAVE_rsqrtsf2 (TARGET_SIMD)
#define HAVE_rsqrtdf2 (TARGET_SIMD)
#define HAVE_ashlv8qi3 (TARGET_SIMD)
#define HAVE_ashlv16qi3 (TARGET_SIMD)
#define HAVE_ashlv4hi3 (TARGET_SIMD)
#define HAVE_ashlv8hi3 (TARGET_SIMD)
#define HAVE_ashlv2si3 (TARGET_SIMD)
#define HAVE_ashlv4si3 (TARGET_SIMD)
#define HAVE_ashlv2di3 (TARGET_SIMD)
#define HAVE_lshrv8qi3 (TARGET_SIMD)
#define HAVE_lshrv16qi3 (TARGET_SIMD)
#define HAVE_lshrv4hi3 (TARGET_SIMD)
#define HAVE_lshrv8hi3 (TARGET_SIMD)
#define HAVE_lshrv2si3 (TARGET_SIMD)
#define HAVE_lshrv4si3 (TARGET_SIMD)
#define HAVE_lshrv2di3 (TARGET_SIMD)
#define HAVE_ashrv8qi3 (TARGET_SIMD)
#define HAVE_ashrv16qi3 (TARGET_SIMD)
#define HAVE_ashrv4hi3 (TARGET_SIMD)
#define HAVE_ashrv8hi3 (TARGET_SIMD)
#define HAVE_ashrv2si3 (TARGET_SIMD)
#define HAVE_ashrv4si3 (TARGET_SIMD)
#define HAVE_ashrv2di3 (TARGET_SIMD)
#define HAVE_vashlv8qi3 (TARGET_SIMD)
#define HAVE_vashlv16qi3 (TARGET_SIMD)
#define HAVE_vashlv4hi3 (TARGET_SIMD)
#define HAVE_vashlv8hi3 (TARGET_SIMD)
#define HAVE_vashlv2si3 (TARGET_SIMD)
#define HAVE_vashlv4si3 (TARGET_SIMD)
#define HAVE_vashlv2di3 (TARGET_SIMD)
#define HAVE_vashrv8qi3 (TARGET_SIMD)
#define HAVE_vashrv16qi3 (TARGET_SIMD)
#define HAVE_vashrv4hi3 (TARGET_SIMD)
#define HAVE_vashrv8hi3 (TARGET_SIMD)
#define HAVE_vashrv2si3 (TARGET_SIMD)
#define HAVE_vashrv4si3 (TARGET_SIMD)
#define HAVE_aarch64_ashr_simddi (TARGET_SIMD)
#define HAVE_vlshrv8qi3 (TARGET_SIMD)
#define HAVE_vlshrv16qi3 (TARGET_SIMD)
#define HAVE_vlshrv4hi3 (TARGET_SIMD)
#define HAVE_vlshrv8hi3 (TARGET_SIMD)
#define HAVE_vlshrv2si3 (TARGET_SIMD)
#define HAVE_vlshrv4si3 (TARGET_SIMD)
#define HAVE_aarch64_lshr_simddi (TARGET_SIMD)
#define HAVE_vec_setv8qi (TARGET_SIMD)
#define HAVE_vec_setv16qi (TARGET_SIMD)
#define HAVE_vec_setv4hi (TARGET_SIMD)
#define HAVE_vec_setv8hi (TARGET_SIMD)
#define HAVE_vec_setv2si (TARGET_SIMD)
#define HAVE_vec_setv4si (TARGET_SIMD)
#define HAVE_vec_setv2di (TARGET_SIMD)
#define HAVE_vec_setv4hf (TARGET_SIMD)
#define HAVE_vec_setv8hf (TARGET_SIMD)
#define HAVE_vec_setv2sf (TARGET_SIMD)
#define HAVE_vec_setv4sf (TARGET_SIMD)
#define HAVE_vec_setv2df (TARGET_SIMD)
#define HAVE_smaxv2di3 (TARGET_SIMD)
#define HAVE_sminv2di3 (TARGET_SIMD)
#define HAVE_umaxv2di3 (TARGET_SIMD)
#define HAVE_uminv2di3 (TARGET_SIMD)
#define HAVE_move_lo_quad_v16qi (TARGET_SIMD)
#define HAVE_move_lo_quad_v8hi (TARGET_SIMD)
#define HAVE_move_lo_quad_v4si (TARGET_SIMD)
#define HAVE_move_lo_quad_v2di (TARGET_SIMD)
#define HAVE_move_lo_quad_v8hf (TARGET_SIMD)
#define HAVE_move_lo_quad_v4sf (TARGET_SIMD)
#define HAVE_move_lo_quad_v2df (TARGET_SIMD)
#define HAVE_move_hi_quad_v16qi (TARGET_SIMD)
#define HAVE_move_hi_quad_v8hi (TARGET_SIMD)
#define HAVE_move_hi_quad_v4si (TARGET_SIMD)
#define HAVE_move_hi_quad_v2di (TARGET_SIMD)
#define HAVE_move_hi_quad_v8hf (TARGET_SIMD)
#define HAVE_move_hi_quad_v4sf (TARGET_SIMD)
#define HAVE_move_hi_quad_v2df (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v4hi (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v2si (TARGET_SIMD)
#define HAVE_vec_pack_trunc_di (TARGET_SIMD)
#define HAVE_vec_unpacks_hi_v16qi (TARGET_SIMD)
#define HAVE_vec_unpacku_hi_v16qi (TARGET_SIMD)
#define HAVE_vec_unpacks_hi_v8hi (TARGET_SIMD)
#define HAVE_vec_unpacku_hi_v8hi (TARGET_SIMD)
#define HAVE_vec_unpacks_hi_v4si (TARGET_SIMD)
#define HAVE_vec_unpacku_hi_v4si (TARGET_SIMD)
#define HAVE_vec_unpacks_lo_v16qi (TARGET_SIMD)
#define HAVE_vec_unpacku_lo_v16qi (TARGET_SIMD)
#define HAVE_vec_unpacks_lo_v8hi (TARGET_SIMD)
#define HAVE_vec_unpacku_lo_v8hi (TARGET_SIMD)
#define HAVE_vec_unpacks_lo_v4si (TARGET_SIMD)
#define HAVE_vec_unpacku_lo_v4si (TARGET_SIMD)
#define HAVE_vec_widen_smult_lo_v16qi (TARGET_SIMD)
#define HAVE_vec_widen_umult_lo_v16qi (TARGET_SIMD)
#define HAVE_vec_widen_smult_lo_v8hi (TARGET_SIMD)
#define HAVE_vec_widen_umult_lo_v8hi (TARGET_SIMD)
#define HAVE_vec_widen_smult_lo_v4si (TARGET_SIMD)
#define HAVE_vec_widen_umult_lo_v4si (TARGET_SIMD)
#define HAVE_vec_widen_smult_hi_v16qi (TARGET_SIMD)
#define HAVE_vec_widen_umult_hi_v16qi (TARGET_SIMD)
#define HAVE_vec_widen_smult_hi_v8hi (TARGET_SIMD)
#define HAVE_vec_widen_umult_hi_v8hi (TARGET_SIMD)
#define HAVE_vec_widen_smult_hi_v4si (TARGET_SIMD)
#define HAVE_vec_widen_umult_hi_v4si (TARGET_SIMD)
#define HAVE_divv4hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_divv8hf3 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_divv2sf3 (TARGET_SIMD)
#define HAVE_divv4sf3 (TARGET_SIMD)
#define HAVE_divv2df3 (TARGET_SIMD)
#define HAVE_fixv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixunsv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixunsv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixv2sfv2si2 (TARGET_SIMD)
#define HAVE_fixunsv2sfv2si2 (TARGET_SIMD)
#define HAVE_fixv4sfv4si2 (TARGET_SIMD)
#define HAVE_fixunsv4sfv4si2 (TARGET_SIMD)
#define HAVE_fixv2dfv2di2 (TARGET_SIMD)
#define HAVE_fixunsv2dfv2di2 (TARGET_SIMD)
#define HAVE_fix_truncv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixuns_truncv4hfv4hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fix_truncv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fixuns_truncv8hfv8hi2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_fix_truncv2sfv2si2 (TARGET_SIMD)
#define HAVE_fixuns_truncv2sfv2si2 (TARGET_SIMD)
#define HAVE_fix_truncv4sfv4si2 (TARGET_SIMD)
#define HAVE_fixuns_truncv4sfv4si2 (TARGET_SIMD)
#define HAVE_fix_truncv2dfv2di2 (TARGET_SIMD)
#define HAVE_fixuns_truncv2dfv2di2 (TARGET_SIMD)
#define HAVE_ftruncv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ftruncv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_ftruncv2sf2 (TARGET_SIMD)
#define HAVE_ftruncv4sf2 (TARGET_SIMD)
#define HAVE_ftruncv2df2 (TARGET_SIMD)
#define HAVE_vec_unpacks_lo_v8hf (TARGET_SIMD)
#define HAVE_vec_unpacks_lo_v4sf (TARGET_SIMD)
#define HAVE_vec_unpacks_hi_v8hf (TARGET_SIMD)
#define HAVE_vec_unpacks_hi_v4sf (TARGET_SIMD)
#define HAVE_aarch64_float_truncate_hi_v4sf (TARGET_SIMD)
#define HAVE_aarch64_float_truncate_hi_v8hf (TARGET_SIMD)
#define HAVE_vec_pack_trunc_v2df (TARGET_SIMD)
#define HAVE_vec_pack_trunc_df (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v8qi (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v16qi (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v4hi (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v8hi (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v2si (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v4si (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v2di (TARGET_SIMD)
#define HAVE_reduc_plus_scal_v4sf (TARGET_SIMD)
#define HAVE_reduc_smax_nan_scal_v4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smin_nan_scal_v4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smax_scal_v4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smin_scal_v4hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smax_nan_scal_v8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smin_nan_scal_v8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smax_scal_v8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smin_scal_v8hf ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_reduc_smax_nan_scal_v2sf (TARGET_SIMD)
#define HAVE_reduc_smin_nan_scal_v2sf (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v2sf (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v2sf (TARGET_SIMD)
#define HAVE_reduc_smax_nan_scal_v4sf (TARGET_SIMD)
#define HAVE_reduc_smin_nan_scal_v4sf (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v4sf (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v4sf (TARGET_SIMD)
#define HAVE_reduc_smax_nan_scal_v2df (TARGET_SIMD)
#define HAVE_reduc_smin_nan_scal_v2df (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v2df (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v2df (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v8qi (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v8qi (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v8qi (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v8qi (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v16qi (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v16qi (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v16qi (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v16qi (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v4hi (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v4hi (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v4hi (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v4hi (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v8hi (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v8hi (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v8hi (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v8hi (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v2si (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v2si (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v2si (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v2si (TARGET_SIMD)
#define HAVE_reduc_umax_scal_v4si (TARGET_SIMD)
#define HAVE_reduc_umin_scal_v4si (TARGET_SIMD)
#define HAVE_reduc_smax_scal_v4si (TARGET_SIMD)
#define HAVE_reduc_smin_scal_v4si (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv16qi (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv8hi (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2si (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4si (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2di (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv8hf (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv4sf (TARGET_SIMD)
#define HAVE_aarch64_simd_bslv2df (TARGET_SIMD)
#define HAVE_aarch64_simd_bsldi (TARGET_SIMD)
#define HAVE_aarch64_simd_bsldf (TARGET_SIMD)
#define HAVE_vcond_mask_v8qiv8qi (TARGET_SIMD)
#define HAVE_vcond_mask_v16qiv16qi (TARGET_SIMD)
#define HAVE_vcond_mask_v4hiv4hi (TARGET_SIMD)
#define HAVE_vcond_mask_v8hiv8hi (TARGET_SIMD)
#define HAVE_vcond_mask_v2siv2si (TARGET_SIMD)
#define HAVE_vcond_mask_v4siv4si (TARGET_SIMD)
#define HAVE_vcond_mask_v2div2di (TARGET_SIMD)
#define HAVE_vcond_mask_v2sfv2si (TARGET_SIMD)
#define HAVE_vcond_mask_v4sfv4si (TARGET_SIMD)
#define HAVE_vcond_mask_v2dfv2di (TARGET_SIMD)
#define HAVE_vcond_mask_didi (TARGET_SIMD)
#define HAVE_vec_cmpv8qiv8qi (TARGET_SIMD)
#define HAVE_vec_cmpv16qiv16qi (TARGET_SIMD)
#define HAVE_vec_cmpv4hiv4hi (TARGET_SIMD)
#define HAVE_vec_cmpv8hiv8hi (TARGET_SIMD)
#define HAVE_vec_cmpv2siv2si (TARGET_SIMD)
#define HAVE_vec_cmpv4siv4si (TARGET_SIMD)
#define HAVE_vec_cmpv2div2di (TARGET_SIMD)
#define HAVE_vec_cmpdidi (TARGET_SIMD)
#define HAVE_vec_cmpv2sfv2si (TARGET_SIMD)
#define HAVE_vec_cmpv4sfv4si (TARGET_SIMD)
#define HAVE_vec_cmpv2dfv2di (TARGET_SIMD)
#define HAVE_vec_cmpuv8qiv8qi (TARGET_SIMD)
#define HAVE_vec_cmpuv16qiv16qi (TARGET_SIMD)
#define HAVE_vec_cmpuv4hiv4hi (TARGET_SIMD)
#define HAVE_vec_cmpuv8hiv8hi (TARGET_SIMD)
#define HAVE_vec_cmpuv2siv2si (TARGET_SIMD)
#define HAVE_vec_cmpuv4siv4si (TARGET_SIMD)
#define HAVE_vec_cmpuv2div2di (TARGET_SIMD)
#define HAVE_vec_cmpudidi (TARGET_SIMD)
#define HAVE_vcondv8qiv8qi (TARGET_SIMD)
#define HAVE_vcondv16qiv16qi (TARGET_SIMD)
#define HAVE_vcondv4hiv4hi (TARGET_SIMD)
#define HAVE_vcondv8hiv8hi (TARGET_SIMD)
#define HAVE_vcondv2siv2si (TARGET_SIMD)
#define HAVE_vcondv4siv4si (TARGET_SIMD)
#define HAVE_vcondv2div2di (TARGET_SIMD)
#define HAVE_vcondv2sfv2sf (TARGET_SIMD)
#define HAVE_vcondv4sfv4sf (TARGET_SIMD)
#define HAVE_vcondv2dfv2df (TARGET_SIMD)
#define HAVE_vconddidi (TARGET_SIMD)
#define HAVE_vcondv2siv2sf (TARGET_SIMD)
#define HAVE_vcondv2sfv2si (TARGET_SIMD)
#define HAVE_vcondv4siv4sf (TARGET_SIMD)
#define HAVE_vcondv4sfv4si (TARGET_SIMD)
#define HAVE_vcondv2div2df (TARGET_SIMD)
#define HAVE_vcondv2dfv2di (TARGET_SIMD)
#define HAVE_vconduv8qiv8qi (TARGET_SIMD)
#define HAVE_vconduv16qiv16qi (TARGET_SIMD)
#define HAVE_vconduv4hiv4hi (TARGET_SIMD)
#define HAVE_vconduv8hiv8hi (TARGET_SIMD)
#define HAVE_vconduv2siv2si (TARGET_SIMD)
#define HAVE_vconduv4siv4si (TARGET_SIMD)
#define HAVE_vconduv2div2di (TARGET_SIMD)
#define HAVE_vcondudidi (TARGET_SIMD)
#define HAVE_vconduv2sfv2si (TARGET_SIMD)
#define HAVE_vconduv4sfv4si (TARGET_SIMD)
#define HAVE_vconduv2dfv2di (TARGET_SIMD)
#define HAVE_aarch64_combinev8qi (TARGET_SIMD)
#define HAVE_aarch64_combinev4hi (TARGET_SIMD)
#define HAVE_aarch64_combinev4hf (TARGET_SIMD)
#define HAVE_aarch64_combinev2si (TARGET_SIMD)
#define HAVE_aarch64_combinev2sf (TARGET_SIMD)
#define HAVE_aarch64_combinedi (TARGET_SIMD)
#define HAVE_aarch64_combinedf (TARGET_SIMD)
#define HAVE_aarch64_simd_combinev8qi (TARGET_SIMD)
#define HAVE_aarch64_simd_combinev4hi (TARGET_SIMD)
#define HAVE_aarch64_simd_combinev4hf (TARGET_SIMD)
#define HAVE_aarch64_simd_combinev2si (TARGET_SIMD)
#define HAVE_aarch64_simd_combinev2sf (TARGET_SIMD)
#define HAVE_aarch64_simd_combinedi (TARGET_SIMD)
#define HAVE_aarch64_simd_combinedf (TARGET_SIMD)
#define HAVE_aarch64_saddl2v16qi (TARGET_SIMD)
#define HAVE_aarch64_saddl2v8hi (TARGET_SIMD)
#define HAVE_aarch64_saddl2v4si (TARGET_SIMD)
#define HAVE_aarch64_uaddl2v16qi (TARGET_SIMD)
#define HAVE_aarch64_uaddl2v8hi (TARGET_SIMD)
#define HAVE_aarch64_uaddl2v4si (TARGET_SIMD)
#define HAVE_aarch64_ssubl2v16qi (TARGET_SIMD)
#define HAVE_aarch64_ssubl2v8hi (TARGET_SIMD)
#define HAVE_aarch64_ssubl2v4si (TARGET_SIMD)
#define HAVE_aarch64_usubl2v16qi (TARGET_SIMD)
#define HAVE_aarch64_usubl2v8hi (TARGET_SIMD)
#define HAVE_aarch64_usubl2v4si (TARGET_SIMD)
#define HAVE_widen_ssumv16qi3 (TARGET_SIMD)
#define HAVE_widen_ssumv8hi3 (TARGET_SIMD)
#define HAVE_widen_ssumv4si3 (TARGET_SIMD)
#define HAVE_widen_ssumv8qi3 (TARGET_SIMD)
#define HAVE_widen_ssumv4hi3 (TARGET_SIMD)
#define HAVE_widen_ssumv2si3 (TARGET_SIMD)
#define HAVE_widen_usumv16qi3 (TARGET_SIMD)
#define HAVE_widen_usumv8hi3 (TARGET_SIMD)
#define HAVE_widen_usumv4si3 (TARGET_SIMD)
#define HAVE_widen_usumv8qi3 (TARGET_SIMD)
#define HAVE_widen_usumv4hi3 (TARGET_SIMD)
#define HAVE_widen_usumv2si3 (TARGET_SIMD)
#define HAVE_aarch64_saddw2v16qi (TARGET_SIMD)
#define HAVE_aarch64_saddw2v8hi (TARGET_SIMD)
#define HAVE_aarch64_saddw2v4si (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v16qi (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v8hi (TARGET_SIMD)
#define HAVE_aarch64_uaddw2v4si (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v16qi (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v8hi (TARGET_SIMD)
#define HAVE_aarch64_ssubw2v4si (TARGET_SIMD)
#define HAVE_aarch64_usubw2v16qi (TARGET_SIMD)
#define HAVE_aarch64_usubw2v8hi (TARGET_SIMD)
#define HAVE_aarch64_usubw2v4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2v8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2v4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2v8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2v4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_laneqv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_laneqv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_laneqv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_laneqv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlal2_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmlsl2_nv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2v8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2v4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_laneqv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_laneqv4si (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_nv8hi (TARGET_SIMD)
#define HAVE_aarch64_sqdmull2_nv4si (TARGET_SIMD)
#define HAVE_sqrtv4hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_sqrtv8hf2 ((TARGET_SIMD) && (TARGET_SIMD_F16INST))
#define HAVE_sqrtv2sf2 (TARGET_SIMD)
#define HAVE_sqrtv4sf2 (TARGET_SIMD)
#define HAVE_sqrtv2df2 (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv16qi (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv8hi (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv4si (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv2di (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv8hf (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv4sf (TARGET_SIMD)
#define HAVE_vec_load_lanesoiv2df (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv16qi (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv8hi (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv4si (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv2di (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv8hf (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv4sf (TARGET_SIMD)
#define HAVE_vec_store_lanesoiv2df (TARGET_SIMD)
#define HAVE_vec_load_lanesciv16qi (TARGET_SIMD)
#define HAVE_vec_load_lanesciv8hi (TARGET_SIMD)
#define HAVE_vec_load_lanesciv4si (TARGET_SIMD)
#define HAVE_vec_load_lanesciv2di (TARGET_SIMD)
#define HAVE_vec_load_lanesciv8hf (TARGET_SIMD)
#define HAVE_vec_load_lanesciv4sf (TARGET_SIMD)
#define HAVE_vec_load_lanesciv2df (TARGET_SIMD)
#define HAVE_vec_store_lanesciv16qi (TARGET_SIMD)
#define HAVE_vec_store_lanesciv8hi (TARGET_SIMD)
#define HAVE_vec_store_lanesciv4si (TARGET_SIMD)
#define HAVE_vec_store_lanesciv2di (TARGET_SIMD)
#define HAVE_vec_store_lanesciv8hf (TARGET_SIMD)
#define HAVE_vec_store_lanesciv4sf (TARGET_SIMD)
#define HAVE_vec_store_lanesciv2df (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv16qi (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv8hi (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv4si (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv2di (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv8hf (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv4sf (TARGET_SIMD)
#define HAVE_vec_load_lanesxiv2df (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv16qi (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv8hi (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv4si (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv2di (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv8hf (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv4sf (TARGET_SIMD)
#define HAVE_vec_store_lanesxiv2df (TARGET_SIMD)
#define HAVE_movoi (TARGET_SIMD)
#define HAVE_movci (TARGET_SIMD)
#define HAVE_movxi (TARGET_SIMD)
#define HAVE_aarch64_ld2rv8qi (TARGET_SIMD)
#define HAVE_aarch64_ld3rv8qi (TARGET_SIMD)
#define HAVE_aarch64_ld4rv8qi (TARGET_SIMD)
#define HAVE_aarch64_ld2rv16qi (TARGET_SIMD)
#define HAVE_aarch64_ld3rv16qi (TARGET_SIMD)
#define HAVE_aarch64_ld4rv16qi (TARGET_SIMD)
#define HAVE_aarch64_ld2rv4hi (TARGET_SIMD)
#define HAVE_aarch64_ld3rv4hi (TARGET_SIMD)
#define HAVE_aarch64_ld4rv4hi (TARGET_SIMD)
#define HAVE_aarch64_ld2rv8hi (TARGET_SIMD)
#define HAVE_aarch64_ld3rv8hi (TARGET_SIMD)
#define HAVE_aarch64_ld4rv8hi (TARGET_SIMD)
#define HAVE_aarch64_ld2rv2si (TARGET_SIMD)
#define HAVE_aarch64_ld3rv2si (TARGET_SIMD)
#define HAVE_aarch64_ld4rv2si (TARGET_SIMD)
#define HAVE_aarch64_ld2rv4si (TARGET_SIMD)
#define HAVE_aarch64_ld3rv4si (TARGET_SIMD)
#define HAVE_aarch64_ld4rv4si (TARGET_SIMD)
#define HAVE_aarch64_ld2rv2di (TARGET_SIMD)
#define HAVE_aarch64_ld3rv2di (TARGET_SIMD)
#define HAVE_aarch64_ld4rv2di (TARGET_SIMD)
#define HAVE_aarch64_ld2rv4hf (TARGET_SIMD)
#define HAVE_aarch64_ld3rv4hf (TARGET_SIMD)
#define HAVE_aarch64_ld4rv4hf (TARGET_SIMD)
#define HAVE_aarch64_ld2rv8hf (TARGET_SIMD)
#define HAVE_aarch64_ld3rv8hf (TARGET_SIMD)
#define HAVE_aarch64_ld4rv8hf (TARGET_SIMD)
#define HAVE_aarch64_ld2rv2sf (TARGET_SIMD)
#define HAVE_aarch64_ld3rv2sf (TARGET_SIMD)
#define HAVE_aarch64_ld4rv2sf (TARGET_SIMD)
#define HAVE_aarch64_ld2rv4sf (TARGET_SIMD)
#define HAVE_aarch64_ld3rv4sf (TARGET_SIMD)
#define HAVE_aarch64_ld4rv4sf (TARGET_SIMD)
#define HAVE_aarch64_ld2rv2df (TARGET_SIMD)
#define HAVE_aarch64_ld3rv2df (TARGET_SIMD)
#define HAVE_aarch64_ld4rv2df (TARGET_SIMD)
#define HAVE_aarch64_ld2rdi (TARGET_SIMD)
#define HAVE_aarch64_ld3rdi (TARGET_SIMD)
#define HAVE_aarch64_ld4rdi (TARGET_SIMD)
#define HAVE_aarch64_ld2rdf (TARGET_SIMD)
#define HAVE_aarch64_ld3rdf (TARGET_SIMD)
#define HAVE_aarch64_ld4rdf (TARGET_SIMD)
#define HAVE_aarch64_ld2v8qi (TARGET_SIMD)
#define HAVE_aarch64_ld2v4hi (TARGET_SIMD)
#define HAVE_aarch64_ld2v4hf (TARGET_SIMD)
#define HAVE_aarch64_ld2v2si (TARGET_SIMD)
#define HAVE_aarch64_ld2v2sf (TARGET_SIMD)
#define HAVE_aarch64_ld2di (TARGET_SIMD)
#define HAVE_aarch64_ld2df (TARGET_SIMD)
#define HAVE_aarch64_ld3v8qi (TARGET_SIMD)
#define HAVE_aarch64_ld3v4hi (TARGET_SIMD)
#define HAVE_aarch64_ld3v4hf (TARGET_SIMD)
#define HAVE_aarch64_ld3v2si (TARGET_SIMD)
#define HAVE_aarch64_ld3v2sf (TARGET_SIMD)
#define HAVE_aarch64_ld3di (TARGET_SIMD)
#define HAVE_aarch64_ld3df (TARGET_SIMD)
#define HAVE_aarch64_ld4v8qi (TARGET_SIMD)
#define HAVE_aarch64_ld4v4hi (TARGET_SIMD)
#define HAVE_aarch64_ld4v4hf (TARGET_SIMD)
#define HAVE_aarch64_ld4v2si (TARGET_SIMD)
#define HAVE_aarch64_ld4v2sf (TARGET_SIMD)
#define HAVE_aarch64_ld4di (TARGET_SIMD)
#define HAVE_aarch64_ld4df (TARGET_SIMD)
#define HAVE_aarch64_ld1v8qi (TARGET_SIMD)
#define HAVE_aarch64_ld1v16qi (TARGET_SIMD)
#define HAVE_aarch64_ld1v4hi (TARGET_SIMD)
#define HAVE_aarch64_ld1v8hi (TARGET_SIMD)
#define HAVE_aarch64_ld1v2si (TARGET_SIMD)
#define HAVE_aarch64_ld1v4si (TARGET_SIMD)
#define HAVE_aarch64_ld1v2di (TARGET_SIMD)
#define HAVE_aarch64_ld1v4hf (TARGET_SIMD)
#define HAVE_aarch64_ld1v8hf (TARGET_SIMD)
#define HAVE_aarch64_ld1v2sf (TARGET_SIMD)
#define HAVE_aarch64_ld1v4sf (TARGET_SIMD)
#define HAVE_aarch64_ld1v2df (TARGET_SIMD)
#define HAVE_aarch64_ld2v16qi (TARGET_SIMD)
#define HAVE_aarch64_ld2v8hi (TARGET_SIMD)
#define HAVE_aarch64_ld2v4si (TARGET_SIMD)
#define HAVE_aarch64_ld2v2di (TARGET_SIMD)
#define HAVE_aarch64_ld2v8hf (TARGET_SIMD)
#define HAVE_aarch64_ld2v4sf (TARGET_SIMD)
#define HAVE_aarch64_ld2v2df (TARGET_SIMD)
#define HAVE_aarch64_ld3v16qi (TARGET_SIMD)
#define HAVE_aarch64_ld3v8hi (TARGET_SIMD)
#define HAVE_aarch64_ld3v4si (TARGET_SIMD)
#define HAVE_aarch64_ld3v2di (TARGET_SIMD)
#define HAVE_aarch64_ld3v8hf (TARGET_SIMD)
#define HAVE_aarch64_ld3v4sf (TARGET_SIMD)
#define HAVE_aarch64_ld3v2df (TARGET_SIMD)
#define HAVE_aarch64_ld4v16qi (TARGET_SIMD)
#define HAVE_aarch64_ld4v8hi (TARGET_SIMD)
#define HAVE_aarch64_ld4v4si (TARGET_SIMD)
#define HAVE_aarch64_ld4v2di (TARGET_SIMD)
#define HAVE_aarch64_ld4v8hf (TARGET_SIMD)
#define HAVE_aarch64_ld4v4sf (TARGET_SIMD)
#define HAVE_aarch64_ld4v2df (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v16qi (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v8hi (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v4si (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v2di (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v8hf (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v4sf (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v2df (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v8qi (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v4hi (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v4hf (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v2si (TARGET_SIMD)
#define HAVE_aarch64_ld1x2v2sf (TARGET_SIMD)
#define HAVE_aarch64_ld1x2di (TARGET_SIMD)
#define HAVE_aarch64_ld1x2df (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanedi (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanedi (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanedi (TARGET_SIMD)
#define HAVE_aarch64_ld2_lanedf (TARGET_SIMD)
#define HAVE_aarch64_ld3_lanedf (TARGET_SIMD)
#define HAVE_aarch64_ld4_lanedf (TARGET_SIMD)
#define HAVE_aarch64_get_dregoiv8qi (TARGET_SIMD)
#define HAVE_aarch64_get_dregoiv4hi (TARGET_SIMD)
#define HAVE_aarch64_get_dregoiv4hf (TARGET_SIMD)
#define HAVE_aarch64_get_dregoiv2si (TARGET_SIMD)
#define HAVE_aarch64_get_dregoiv2sf (TARGET_SIMD)
#define HAVE_aarch64_get_dregoidi (TARGET_SIMD)
#define HAVE_aarch64_get_dregoidf (TARGET_SIMD)
#define HAVE_aarch64_get_dregciv8qi (TARGET_SIMD)
#define HAVE_aarch64_get_dregciv4hi (TARGET_SIMD)
#define HAVE_aarch64_get_dregciv4hf (TARGET_SIMD)
#define HAVE_aarch64_get_dregciv2si (TARGET_SIMD)
#define HAVE_aarch64_get_dregciv2sf (TARGET_SIMD)
#define HAVE_aarch64_get_dregcidi (TARGET_SIMD)
#define HAVE_aarch64_get_dregcidf (TARGET_SIMD)
#define HAVE_aarch64_get_dregxiv8qi (TARGET_SIMD)
#define HAVE_aarch64_get_dregxiv4hi (TARGET_SIMD)
#define HAVE_aarch64_get_dregxiv4hf (TARGET_SIMD)
#define HAVE_aarch64_get_dregxiv2si (TARGET_SIMD)
#define HAVE_aarch64_get_dregxiv2sf (TARGET_SIMD)
#define HAVE_aarch64_get_dregxidi (TARGET_SIMD)
#define HAVE_aarch64_get_dregxidf (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv16qi (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv8hi (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv4si (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv2di (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv8hf (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv4sf (TARGET_SIMD)
#define HAVE_aarch64_get_qregoiv2df (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv16qi (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv8hi (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv4si (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv2di (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv8hf (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv4sf (TARGET_SIMD)
#define HAVE_aarch64_get_qregciv2df (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv16qi (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv8hi (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv4si (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv2di (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv8hf (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv4sf (TARGET_SIMD)
#define HAVE_aarch64_get_qregxiv2df (TARGET_SIMD)
#define HAVE_vec_permv8qi (TARGET_SIMD)
#define HAVE_vec_permv16qi (TARGET_SIMD)
#define HAVE_aarch64_st2v8qi (TARGET_SIMD)
#define HAVE_aarch64_st2v4hi (TARGET_SIMD)
#define HAVE_aarch64_st2v4hf (TARGET_SIMD)
#define HAVE_aarch64_st2v2si (TARGET_SIMD)
#define HAVE_aarch64_st2v2sf (TARGET_SIMD)
#define HAVE_aarch64_st2di (TARGET_SIMD)
#define HAVE_aarch64_st2df (TARGET_SIMD)
#define HAVE_aarch64_st3v8qi (TARGET_SIMD)
#define HAVE_aarch64_st3v4hi (TARGET_SIMD)
#define HAVE_aarch64_st3v4hf (TARGET_SIMD)
#define HAVE_aarch64_st3v2si (TARGET_SIMD)
#define HAVE_aarch64_st3v2sf (TARGET_SIMD)
#define HAVE_aarch64_st3di (TARGET_SIMD)
#define HAVE_aarch64_st3df (TARGET_SIMD)
#define HAVE_aarch64_st4v8qi (TARGET_SIMD)
#define HAVE_aarch64_st4v4hi (TARGET_SIMD)
#define HAVE_aarch64_st4v4hf (TARGET_SIMD)
#define HAVE_aarch64_st4v2si (TARGET_SIMD)
#define HAVE_aarch64_st4v2sf (TARGET_SIMD)
#define HAVE_aarch64_st4di (TARGET_SIMD)
#define HAVE_aarch64_st4df (TARGET_SIMD)
#define HAVE_aarch64_st2v16qi (TARGET_SIMD)
#define HAVE_aarch64_st2v8hi (TARGET_SIMD)
#define HAVE_aarch64_st2v4si (TARGET_SIMD)
#define HAVE_aarch64_st2v2di (TARGET_SIMD)
#define HAVE_aarch64_st2v8hf (TARGET_SIMD)
#define HAVE_aarch64_st2v4sf (TARGET_SIMD)
#define HAVE_aarch64_st2v2df (TARGET_SIMD)
#define HAVE_aarch64_st3v16qi (TARGET_SIMD)
#define HAVE_aarch64_st3v8hi (TARGET_SIMD)
#define HAVE_aarch64_st3v4si (TARGET_SIMD)
#define HAVE_aarch64_st3v2di (TARGET_SIMD)
#define HAVE_aarch64_st3v8hf (TARGET_SIMD)
#define HAVE_aarch64_st3v4sf (TARGET_SIMD)
#define HAVE_aarch64_st3v2df (TARGET_SIMD)
#define HAVE_aarch64_st4v16qi (TARGET_SIMD)
#define HAVE_aarch64_st4v8hi (TARGET_SIMD)
#define HAVE_aarch64_st4v4si (TARGET_SIMD)
#define HAVE_aarch64_st4v2di (TARGET_SIMD)
#define HAVE_aarch64_st4v8hf (TARGET_SIMD)
#define HAVE_aarch64_st4v4sf (TARGET_SIMD)
#define HAVE_aarch64_st4v2df (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev8qi (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev16qi (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev4hi (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev8hi (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev2si (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev4si (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev2di (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev4hf (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev8hf (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev2sf (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev4sf (TARGET_SIMD)
#define HAVE_aarch64_st2_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_st3_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_st4_lanev2df (TARGET_SIMD)
#define HAVE_aarch64_st2_lanedi (TARGET_SIMD)
#define HAVE_aarch64_st3_lanedi (TARGET_SIMD)
#define HAVE_aarch64_st4_lanedi (TARGET_SIMD)
#define HAVE_aarch64_st2_lanedf (TARGET_SIMD)
#define HAVE_aarch64_st3_lanedf (TARGET_SIMD)
#define HAVE_aarch64_st4_lanedf (TARGET_SIMD)
#define HAVE_aarch64_st1v8qi (TARGET_SIMD)
#define HAVE_aarch64_st1v16qi (TARGET_SIMD)
#define HAVE_aarch64_st1v4hi (TARGET_SIMD)
#define HAVE_aarch64_st1v8hi (TARGET_SIMD)
#define HAVE_aarch64_st1v2si (TARGET_SIMD)
#define HAVE_aarch64_st1v4si (TARGET_SIMD)
#define HAVE_aarch64_st1v2di (TARGET_SIMD)
#define HAVE_aarch64_st1v4hf (TARGET_SIMD)
#define HAVE_aarch64_st1v8hf (TARGET_SIMD)
#define HAVE_aarch64_st1v2sf (TARGET_SIMD)
#define HAVE_aarch64_st1v4sf (TARGET_SIMD)
#define HAVE_aarch64_st1v2df (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv16qi (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv8hi (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv4si (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv2di (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv8hf (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv4sf (TARGET_SIMD)
#define HAVE_aarch64_set_qregoiv2df (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv16qi (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv8hi (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv4si (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv2di (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv8hf (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv4sf (TARGET_SIMD)
#define HAVE_aarch64_set_qregciv2df (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv16qi (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv8hi (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv4si (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv2di (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv8hf (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv4sf (TARGET_SIMD)
#define HAVE_aarch64_set_qregxiv2df (TARGET_SIMD)
#define HAVE_vec_initv8qiqi (TARGET_SIMD)
#define HAVE_vec_initv16qiqi (TARGET_SIMD)
#define HAVE_vec_initv4hihi (TARGET_SIMD)
#define HAVE_vec_initv8hihi (TARGET_SIMD)
#define HAVE_vec_initv2sisi (TARGET_SIMD)
#define HAVE_vec_initv4sisi (TARGET_SIMD)
#define HAVE_vec_initv2didi (TARGET_SIMD)
#define HAVE_vec_initv4hfhf (TARGET_SIMD)
#define HAVE_vec_initv8hfhf (TARGET_SIMD)
#define HAVE_vec_initv2sfsf (TARGET_SIMD)
#define HAVE_vec_initv4sfsf (TARGET_SIMD)
#define HAVE_vec_initv2dfdf (TARGET_SIMD)
#define HAVE_vec_extractv8qiqi (TARGET_SIMD)
#define HAVE_vec_extractv16qiqi (TARGET_SIMD)
#define HAVE_vec_extractv4hihi (TARGET_SIMD)
#define HAVE_vec_extractv8hihi (TARGET_SIMD)
#define HAVE_vec_extractv2sisi (TARGET_SIMD)
#define HAVE_vec_extractv4sisi (TARGET_SIMD)
#define HAVE_vec_extractv2didi (TARGET_SIMD)
#define HAVE_vec_extractv4hfhf (TARGET_SIMD)
#define HAVE_vec_extractv8hfhf (TARGET_SIMD)
#define HAVE_vec_extractv2sfsf (TARGET_SIMD)
#define HAVE_vec_extractv4sfsf (TARGET_SIMD)
#define HAVE_vec_extractv2dfdf (TARGET_SIMD)
#define HAVE_aarch64_fmlal_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlal_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlal_lane_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_lane_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlal_lane_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_lane_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_laneq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_laneq_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_laneq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_laneq_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlal_laneq_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_laneq_lowv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlal_laneq_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlsl_laneq_highv2sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_lane_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_lane_lowv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlalq_lane_highv4sf (TARGET_F16FML)
#define HAVE_aarch64_fmlslq_lane_highv4sf (TARGET_F16FML)
#define HAVE_atomic_compare_and_swapqi 1
#define HAVE_atomic_compare_and_swaphi 1
#define HAVE_atomic_compare_and_swapsi 1
#define HAVE_atomic_compare_and_swapdi 1
#define HAVE_atomic_exchangeqi 1
#define HAVE_atomic_exchangehi 1
#define HAVE_atomic_exchangesi 1
#define HAVE_atomic_exchangedi 1
#define HAVE_atomic_addqi 1
#define HAVE_atomic_subqi 1
#define HAVE_atomic_orqi 1
#define HAVE_atomic_xorqi 1
#define HAVE_atomic_andqi 1
#define HAVE_atomic_addhi 1
#define HAVE_atomic_subhi 1
#define HAVE_atomic_orhi 1
#define HAVE_atomic_xorhi 1
#define HAVE_atomic_andhi 1
#define HAVE_atomic_addsi 1
#define HAVE_atomic_subsi 1
#define HAVE_atomic_orsi 1
#define HAVE_atomic_xorsi 1
#define HAVE_atomic_andsi 1
#define HAVE_atomic_adddi 1
#define HAVE_atomic_subdi 1
#define HAVE_atomic_ordi 1
#define HAVE_atomic_xordi 1
#define HAVE_atomic_anddi 1
#define HAVE_atomic_fetch_addqi 1
#define HAVE_atomic_fetch_subqi 1
#define HAVE_atomic_fetch_orqi 1
#define HAVE_atomic_fetch_xorqi 1
#define HAVE_atomic_fetch_andqi 1
#define HAVE_atomic_fetch_addhi 1
#define HAVE_atomic_fetch_subhi 1
#define HAVE_atomic_fetch_orhi 1
#define HAVE_atomic_fetch_xorhi 1
#define HAVE_atomic_fetch_andhi 1
#define HAVE_atomic_fetch_addsi 1
#define HAVE_atomic_fetch_subsi 1
#define HAVE_atomic_fetch_orsi 1
#define HAVE_atomic_fetch_xorsi 1
#define HAVE_atomic_fetch_andsi 1
#define HAVE_atomic_fetch_adddi 1
#define HAVE_atomic_fetch_subdi 1
#define HAVE_atomic_fetch_ordi 1
#define HAVE_atomic_fetch_xordi 1
#define HAVE_atomic_fetch_anddi 1
#define HAVE_atomic_add_fetchqi 1
#define HAVE_atomic_sub_fetchqi 1
#define HAVE_atomic_or_fetchqi 1
#define HAVE_atomic_xor_fetchqi 1
#define HAVE_atomic_and_fetchqi 1
#define HAVE_atomic_add_fetchhi 1
#define HAVE_atomic_sub_fetchhi 1
#define HAVE_atomic_or_fetchhi 1
#define HAVE_atomic_xor_fetchhi 1
#define HAVE_atomic_and_fetchhi 1
#define HAVE_atomic_add_fetchsi 1
#define HAVE_atomic_sub_fetchsi 1
#define HAVE_atomic_or_fetchsi 1
#define HAVE_atomic_xor_fetchsi 1
#define HAVE_atomic_and_fetchsi 1
#define HAVE_atomic_add_fetchdi 1
#define HAVE_atomic_sub_fetchdi 1
#define HAVE_atomic_or_fetchdi 1
#define HAVE_atomic_xor_fetchdi 1
#define HAVE_atomic_and_fetchdi 1
#define HAVE_mem_thread_fence 1
#define HAVE_dmb 1
#define HAVE_movvnx16qi (TARGET_SVE)
#define HAVE_movvnx8hi (TARGET_SVE)
#define HAVE_movvnx4si (TARGET_SVE)
#define HAVE_movvnx2di (TARGET_SVE)
#define HAVE_movvnx8hf (TARGET_SVE)
#define HAVE_movvnx4sf (TARGET_SVE)
#define HAVE_movvnx2df (TARGET_SVE)
#define HAVE_aarch64_sve_reload_be (TARGET_SVE && BYTES_BIG_ENDIAN)
#define HAVE_movmisalignvnx16qi (TARGET_SVE)
#define HAVE_movmisalignvnx8hi (TARGET_SVE)
#define HAVE_movmisalignvnx4si (TARGET_SVE)
#define HAVE_movmisalignvnx2di (TARGET_SVE)
#define HAVE_movmisalignvnx8hf (TARGET_SVE)
#define HAVE_movmisalignvnx4sf (TARGET_SVE)
#define HAVE_movmisalignvnx2df (TARGET_SVE)
#define HAVE_gather_loadvnx4si (TARGET_SVE)
#define HAVE_gather_loadvnx2di (TARGET_SVE)
#define HAVE_gather_loadvnx4sf (TARGET_SVE)
#define HAVE_gather_loadvnx2df (TARGET_SVE)
#define HAVE_scatter_storevnx4si (TARGET_SVE)
#define HAVE_scatter_storevnx2di (TARGET_SVE)
#define HAVE_scatter_storevnx4sf (TARGET_SVE)
#define HAVE_scatter_storevnx2df (TARGET_SVE)
#define HAVE_movvnx32qi (TARGET_SVE)
#define HAVE_movvnx16hi (TARGET_SVE)
#define HAVE_movvnx8si (TARGET_SVE)
#define HAVE_movvnx4di (TARGET_SVE)
#define HAVE_movvnx16hf (TARGET_SVE)
#define HAVE_movvnx8sf (TARGET_SVE)
#define HAVE_movvnx4df (TARGET_SVE)
#define HAVE_movvnx48qi (TARGET_SVE)
#define HAVE_movvnx24hi (TARGET_SVE)
#define HAVE_movvnx12si (TARGET_SVE)
#define HAVE_movvnx6di (TARGET_SVE)
#define HAVE_movvnx24hf (TARGET_SVE)
#define HAVE_movvnx12sf (TARGET_SVE)
#define HAVE_movvnx6df (TARGET_SVE)
#define HAVE_movvnx64qi (TARGET_SVE)
#define HAVE_movvnx32hi (TARGET_SVE)
#define HAVE_movvnx16si (TARGET_SVE)
#define HAVE_movvnx8di (TARGET_SVE)
#define HAVE_movvnx32hf (TARGET_SVE)
#define HAVE_movvnx16sf (TARGET_SVE)
#define HAVE_movvnx8df (TARGET_SVE)
#define HAVE_movvnx16bi (TARGET_SVE)
#define HAVE_movvnx8bi (TARGET_SVE)
#define HAVE_movvnx4bi (TARGET_SVE)
#define HAVE_movvnx2bi (TARGET_SVE)
#define HAVE_vec_extractvnx16biqi (TARGET_SVE)
#define HAVE_vec_extractvnx8bihi (TARGET_SVE)
#define HAVE_vec_extractvnx4bisi (TARGET_SVE)
#define HAVE_vec_extractvnx2bidi (TARGET_SVE)
#define HAVE_vec_extractvnx16qiqi (TARGET_SVE)
#define HAVE_vec_extractvnx8hihi (TARGET_SVE)
#define HAVE_vec_extractvnx4sisi (TARGET_SVE)
#define HAVE_vec_extractvnx2didi (TARGET_SVE)
#define HAVE_vec_extractvnx8hfhf (TARGET_SVE)
#define HAVE_vec_extractvnx4sfsf (TARGET_SVE)
#define HAVE_vec_extractvnx2dfdf (TARGET_SVE)
#define HAVE_vec_duplicatevnx16qi (TARGET_SVE)
#define HAVE_vec_duplicatevnx8hi (TARGET_SVE)
#define HAVE_vec_duplicatevnx4si (TARGET_SVE)
#define HAVE_vec_duplicatevnx2di (TARGET_SVE)
#define HAVE_vec_duplicatevnx8hf (TARGET_SVE)
#define HAVE_vec_duplicatevnx4sf (TARGET_SVE)
#define HAVE_vec_duplicatevnx2df (TARGET_SVE)
#define HAVE_vec_duplicatevnx16bi (TARGET_SVE)
#define HAVE_vec_duplicatevnx8bi (TARGET_SVE)
#define HAVE_vec_duplicatevnx4bi (TARGET_SVE)
#define HAVE_vec_duplicatevnx2bi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx32qivnx16qi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx16hivnx8hi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx8sivnx4si (TARGET_SVE)
#define HAVE_vec_load_lanesvnx4divnx2di (TARGET_SVE)
#define HAVE_vec_load_lanesvnx16hfvnx8hf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx8sfvnx4sf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx4dfvnx2df (TARGET_SVE)
#define HAVE_vec_load_lanesvnx48qivnx16qi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx24hivnx8hi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx12sivnx4si (TARGET_SVE)
#define HAVE_vec_load_lanesvnx6divnx2di (TARGET_SVE)
#define HAVE_vec_load_lanesvnx24hfvnx8hf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx12sfvnx4sf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx6dfvnx2df (TARGET_SVE)
#define HAVE_vec_load_lanesvnx64qivnx16qi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx32hivnx8hi (TARGET_SVE)
#define HAVE_vec_load_lanesvnx16sivnx4si (TARGET_SVE)
#define HAVE_vec_load_lanesvnx8divnx2di (TARGET_SVE)
#define HAVE_vec_load_lanesvnx32hfvnx8hf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx16sfvnx4sf (TARGET_SVE)
#define HAVE_vec_load_lanesvnx8dfvnx2df (TARGET_SVE)
#define HAVE_vec_store_lanesvnx32qivnx16qi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx16hivnx8hi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx8sivnx4si (TARGET_SVE)
#define HAVE_vec_store_lanesvnx4divnx2di (TARGET_SVE)
#define HAVE_vec_store_lanesvnx16hfvnx8hf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx8sfvnx4sf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx4dfvnx2df (TARGET_SVE)
#define HAVE_vec_store_lanesvnx48qivnx16qi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx24hivnx8hi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx12sivnx4si (TARGET_SVE)
#define HAVE_vec_store_lanesvnx6divnx2di (TARGET_SVE)
#define HAVE_vec_store_lanesvnx24hfvnx8hf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx12sfvnx4sf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx6dfvnx2df (TARGET_SVE)
#define HAVE_vec_store_lanesvnx64qivnx16qi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx32hivnx8hi (TARGET_SVE)
#define HAVE_vec_store_lanesvnx16sivnx4si (TARGET_SVE)
#define HAVE_vec_store_lanesvnx8divnx2di (TARGET_SVE)
#define HAVE_vec_store_lanesvnx32hfvnx8hf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx16sfvnx4sf (TARGET_SVE)
#define HAVE_vec_store_lanesvnx8dfvnx2df (TARGET_SVE)
#define HAVE_vec_permvnx16qi (TARGET_SVE && GET_MODE_NUNITS (VNx16QImode).is_constant ())
#define HAVE_vec_permvnx8hi (TARGET_SVE && GET_MODE_NUNITS (VNx8HImode).is_constant ())
#define HAVE_vec_permvnx4si (TARGET_SVE && GET_MODE_NUNITS (VNx4SImode).is_constant ())
#define HAVE_vec_permvnx2di (TARGET_SVE && GET_MODE_NUNITS (VNx2DImode).is_constant ())
#define HAVE_vec_permvnx8hf (TARGET_SVE && GET_MODE_NUNITS (VNx8HFmode).is_constant ())
#define HAVE_vec_permvnx4sf (TARGET_SVE && GET_MODE_NUNITS (VNx4SFmode).is_constant ())
#define HAVE_vec_permvnx2df (TARGET_SVE && GET_MODE_NUNITS (VNx2DFmode).is_constant ())
#define HAVE_mulvnx16qi3 (TARGET_SVE)
#define HAVE_mulvnx8hi3 (TARGET_SVE)
#define HAVE_mulvnx4si3 (TARGET_SVE)
#define HAVE_mulvnx2di3 (TARGET_SVE)
#define HAVE_smulvnx16qi3_highpart (TARGET_SVE)
#define HAVE_umulvnx16qi3_highpart (TARGET_SVE)
#define HAVE_smulvnx8hi3_highpart (TARGET_SVE)
#define HAVE_umulvnx8hi3_highpart (TARGET_SVE)
#define HAVE_smulvnx4si3_highpart (TARGET_SVE)
#define HAVE_umulvnx4si3_highpart (TARGET_SVE)
#define HAVE_smulvnx2di3_highpart (TARGET_SVE)
#define HAVE_umulvnx2di3_highpart (TARGET_SVE)
#define HAVE_negvnx16qi2 (TARGET_SVE)
#define HAVE_one_cmplvnx16qi2 (TARGET_SVE)
#define HAVE_popcountvnx16qi2 (TARGET_SVE)
#define HAVE_negvnx8hi2 (TARGET_SVE)
#define HAVE_one_cmplvnx8hi2 (TARGET_SVE)
#define HAVE_popcountvnx8hi2 (TARGET_SVE)
#define HAVE_negvnx4si2 (TARGET_SVE)
#define HAVE_one_cmplvnx4si2 (TARGET_SVE)
#define HAVE_popcountvnx4si2 (TARGET_SVE)
#define HAVE_negvnx2di2 (TARGET_SVE)
#define HAVE_one_cmplvnx2di2 (TARGET_SVE)
#define HAVE_popcountvnx2di2 (TARGET_SVE)
#define HAVE_iorvnx16bi3 (TARGET_SVE)
#define HAVE_xorvnx16bi3 (TARGET_SVE)
#define HAVE_iorvnx8bi3 (TARGET_SVE)
#define HAVE_xorvnx8bi3 (TARGET_SVE)
#define HAVE_iorvnx4bi3 (TARGET_SVE)
#define HAVE_xorvnx4bi3 (TARGET_SVE)
#define HAVE_iorvnx2bi3 (TARGET_SVE)
#define HAVE_xorvnx2bi3 (TARGET_SVE)
#define HAVE_one_cmplvnx16bi2 (TARGET_SVE)
#define HAVE_one_cmplvnx8bi2 (TARGET_SVE)
#define HAVE_one_cmplvnx4bi2 (TARGET_SVE)
#define HAVE_one_cmplvnx2bi2 (TARGET_SVE)
#define HAVE_vashlvnx16qi3 (TARGET_SVE)
#define HAVE_vashrvnx16qi3 (TARGET_SVE)
#define HAVE_vlshrvnx16qi3 (TARGET_SVE)
#define HAVE_vashlvnx8hi3 (TARGET_SVE)
#define HAVE_vashrvnx8hi3 (TARGET_SVE)
#define HAVE_vlshrvnx8hi3 (TARGET_SVE)
#define HAVE_vashlvnx4si3 (TARGET_SVE)
#define HAVE_vashrvnx4si3 (TARGET_SVE)
#define HAVE_vlshrvnx4si3 (TARGET_SVE)
#define HAVE_vashlvnx2di3 (TARGET_SVE)
#define HAVE_vashrvnx2di3 (TARGET_SVE)
#define HAVE_vlshrvnx2di3 (TARGET_SVE)
#define HAVE_ashlvnx16qi3 (TARGET_SVE)
#define HAVE_ashrvnx16qi3 (TARGET_SVE)
#define HAVE_lshrvnx16qi3 (TARGET_SVE)
#define HAVE_ashlvnx8hi3 (TARGET_SVE)
#define HAVE_ashrvnx8hi3 (TARGET_SVE)
#define HAVE_lshrvnx8hi3 (TARGET_SVE)
#define HAVE_ashlvnx4si3 (TARGET_SVE)
#define HAVE_ashrvnx4si3 (TARGET_SVE)
#define HAVE_lshrvnx4si3 (TARGET_SVE)
#define HAVE_ashlvnx2di3 (TARGET_SVE)
#define HAVE_ashrvnx2di3 (TARGET_SVE)
#define HAVE_lshrvnx2di3 (TARGET_SVE)
#define HAVE_vcondvnx16qivnx16qi (TARGET_SVE)
#define HAVE_vcondvnx8hivnx8hi (TARGET_SVE)
#define HAVE_vcondvnx4sivnx4si (TARGET_SVE)
#define HAVE_vcondvnx2divnx2di (TARGET_SVE)
#define HAVE_vcondvnx8hfvnx8hi (TARGET_SVE)
#define HAVE_vcondvnx4sfvnx4si (TARGET_SVE)
#define HAVE_vcondvnx2dfvnx2di (TARGET_SVE)
#define HAVE_vconduvnx16qivnx16qi (TARGET_SVE)
#define HAVE_vconduvnx8hivnx8hi (TARGET_SVE)
#define HAVE_vconduvnx4sivnx4si (TARGET_SVE)
#define HAVE_vconduvnx2divnx2di (TARGET_SVE)
#define HAVE_vconduvnx8hfvnx8hi (TARGET_SVE)
#define HAVE_vconduvnx4sfvnx4si (TARGET_SVE)
#define HAVE_vconduvnx2dfvnx2di (TARGET_SVE)
#define HAVE_vcondvnx4sivnx4sf (TARGET_SVE)
#define HAVE_vcondvnx2divnx2df (TARGET_SVE)
#define HAVE_vcondvnx4sfvnx4sf (TARGET_SVE)
#define HAVE_vcondvnx2dfvnx2df (TARGET_SVE)
#define HAVE_vec_cmpvnx16qivnx16bi (TARGET_SVE)
#define HAVE_vec_cmpvnx8hivnx8bi (TARGET_SVE)
#define HAVE_vec_cmpvnx4sivnx4bi (TARGET_SVE)
#define HAVE_vec_cmpvnx2divnx2bi (TARGET_SVE)
#define HAVE_vec_cmpuvnx16qivnx16bi (TARGET_SVE)
#define HAVE_vec_cmpuvnx8hivnx8bi (TARGET_SVE)
#define HAVE_vec_cmpuvnx4sivnx4bi (TARGET_SVE)
#define HAVE_vec_cmpuvnx2divnx2bi (TARGET_SVE)
#define HAVE_vec_cmpvnx8hfvnx8bi (TARGET_SVE)
#define HAVE_vec_cmpvnx4sfvnx4bi (TARGET_SVE)
#define HAVE_vec_cmpvnx2dfvnx2bi (TARGET_SVE)
#define HAVE_cbranchvnx16bi4 1
#define HAVE_cbranchvnx8bi4 1
#define HAVE_cbranchvnx4bi4 1
#define HAVE_cbranchvnx2bi4 1
#define HAVE_smaxvnx16qi3 (TARGET_SVE)
#define HAVE_sminvnx16qi3 (TARGET_SVE)
#define HAVE_umaxvnx16qi3 (TARGET_SVE)
#define HAVE_uminvnx16qi3 (TARGET_SVE)
#define HAVE_smaxvnx8hi3 (TARGET_SVE)
#define HAVE_sminvnx8hi3 (TARGET_SVE)
#define HAVE_umaxvnx8hi3 (TARGET_SVE)
#define HAVE_uminvnx8hi3 (TARGET_SVE)
#define HAVE_smaxvnx4si3 (TARGET_SVE)
#define HAVE_sminvnx4si3 (TARGET_SVE)
#define HAVE_umaxvnx4si3 (TARGET_SVE)
#define HAVE_uminvnx4si3 (TARGET_SVE)
#define HAVE_smaxvnx2di3 (TARGET_SVE)
#define HAVE_sminvnx2di3 (TARGET_SVE)
#define HAVE_umaxvnx2di3 (TARGET_SVE)
#define HAVE_uminvnx2di3 (TARGET_SVE)
#define HAVE_smaxvnx8hf3 (TARGET_SVE)
#define HAVE_sminvnx8hf3 (TARGET_SVE)
#define HAVE_smaxvnx4sf3 (TARGET_SVE)
#define HAVE_sminvnx4sf3 (TARGET_SVE)
#define HAVE_smaxvnx2df3 (TARGET_SVE)
#define HAVE_sminvnx2df3 (TARGET_SVE)
#define HAVE_smax_nanvnx8hf3 (TARGET_SVE)
#define HAVE_smin_nanvnx8hf3 (TARGET_SVE)
#define HAVE_fmaxvnx8hf3 (TARGET_SVE)
#define HAVE_fminvnx8hf3 (TARGET_SVE)
#define HAVE_smax_nanvnx4sf3 (TARGET_SVE)
#define HAVE_smin_nanvnx4sf3 (TARGET_SVE)
#define HAVE_fmaxvnx4sf3 (TARGET_SVE)
#define HAVE_fminvnx4sf3 (TARGET_SVE)
#define HAVE_smax_nanvnx2df3 (TARGET_SVE)
#define HAVE_smin_nanvnx2df3 (TARGET_SVE)
#define HAVE_fmaxvnx2df3 (TARGET_SVE)
#define HAVE_fminvnx2df3 (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx8hf (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx4sf (TARGET_SVE)
#define HAVE_reduc_plus_scal_vnx2df (TARGET_SVE)
#define HAVE_reduc_umax_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_umin_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_umax_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_umin_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_umax_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_umin_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_umax_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_umin_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_smax_nan_scal_vnx8hf (TARGET_SVE)
#define HAVE_reduc_smin_nan_scal_vnx8hf (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx8hf (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx8hf (TARGET_SVE)
#define HAVE_reduc_smax_nan_scal_vnx4sf (TARGET_SVE)
#define HAVE_reduc_smin_nan_scal_vnx4sf (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx4sf (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx4sf (TARGET_SVE)
#define HAVE_reduc_smax_nan_scal_vnx2df (TARGET_SVE)
#define HAVE_reduc_smin_nan_scal_vnx2df (TARGET_SVE)
#define HAVE_reduc_smax_scal_vnx2df (TARGET_SVE)
#define HAVE_reduc_smin_scal_vnx2df (TARGET_SVE)
#define HAVE_reduc_and_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_ior_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_xor_scal_vnx16qi (TARGET_SVE)
#define HAVE_reduc_and_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_ior_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_xor_scal_vnx8hi (TARGET_SVE)
#define HAVE_reduc_and_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_ior_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_xor_scal_vnx4si (TARGET_SVE)
#define HAVE_reduc_and_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_ior_scal_vnx2di (TARGET_SVE)
#define HAVE_reduc_xor_scal_vnx2di (TARGET_SVE)
#define HAVE_fold_left_plus_vnx8hf (TARGET_SVE)
#define HAVE_fold_left_plus_vnx4sf (TARGET_SVE)
#define HAVE_fold_left_plus_vnx2df (TARGET_SVE)
#define HAVE_addvnx8hf3 (TARGET_SVE)
#define HAVE_addvnx4sf3 (TARGET_SVE)
#define HAVE_addvnx2df3 (TARGET_SVE)
#define HAVE_subvnx8hf3 (TARGET_SVE)
#define HAVE_subvnx4sf3 (TARGET_SVE)
#define HAVE_subvnx2df3 (TARGET_SVE)
#define HAVE_mulvnx8hf3 (TARGET_SVE)
#define HAVE_mulvnx4sf3 (TARGET_SVE)
#define HAVE_mulvnx2df3 (TARGET_SVE)
#define HAVE_fmavnx8hf4 (TARGET_SVE)
#define HAVE_fmavnx4sf4 (TARGET_SVE)
#define HAVE_fmavnx2df4 (TARGET_SVE)
#define HAVE_fnmavnx8hf4 (TARGET_SVE)
#define HAVE_fnmavnx4sf4 (TARGET_SVE)
#define HAVE_fnmavnx2df4 (TARGET_SVE)
#define HAVE_fmsvnx8hf4 (TARGET_SVE)
#define HAVE_fmsvnx4sf4 (TARGET_SVE)
#define HAVE_fmsvnx2df4 (TARGET_SVE)
#define HAVE_fnmsvnx8hf4 (TARGET_SVE)
#define HAVE_fnmsvnx4sf4 (TARGET_SVE)
#define HAVE_fnmsvnx2df4 (TARGET_SVE)
#define HAVE_divvnx8hf3 (TARGET_SVE)
#define HAVE_divvnx4sf3 (TARGET_SVE)
#define HAVE_divvnx2df3 (TARGET_SVE)
#define HAVE_negvnx8hf2 (TARGET_SVE)
#define HAVE_absvnx8hf2 (TARGET_SVE)
#define HAVE_sqrtvnx8hf2 (TARGET_SVE)
#define HAVE_negvnx4sf2 (TARGET_SVE)
#define HAVE_absvnx4sf2 (TARGET_SVE)
#define HAVE_sqrtvnx4sf2 (TARGET_SVE)
#define HAVE_negvnx2df2 (TARGET_SVE)
#define HAVE_absvnx2df2 (TARGET_SVE)
#define HAVE_sqrtvnx2df2 (TARGET_SVE)
#define HAVE_btruncvnx8hf2 (TARGET_SVE)
#define HAVE_ceilvnx8hf2 (TARGET_SVE)
#define HAVE_floorvnx8hf2 (TARGET_SVE)
#define HAVE_frintnvnx8hf2 (TARGET_SVE)
#define HAVE_nearbyintvnx8hf2 (TARGET_SVE)
#define HAVE_rintvnx8hf2 (TARGET_SVE)
#define HAVE_roundvnx8hf2 (TARGET_SVE)
#define HAVE_btruncvnx4sf2 (TARGET_SVE)
#define HAVE_ceilvnx4sf2 (TARGET_SVE)
#define HAVE_floorvnx4sf2 (TARGET_SVE)
#define HAVE_frintnvnx4sf2 (TARGET_SVE)
#define HAVE_nearbyintvnx4sf2 (TARGET_SVE)
#define HAVE_rintvnx4sf2 (TARGET_SVE)
#define HAVE_roundvnx4sf2 (TARGET_SVE)
#define HAVE_btruncvnx2df2 (TARGET_SVE)
#define HAVE_ceilvnx2df2 (TARGET_SVE)
#define HAVE_floorvnx2df2 (TARGET_SVE)
#define HAVE_frintnvnx2df2 (TARGET_SVE)
#define HAVE_nearbyintvnx2df2 (TARGET_SVE)
#define HAVE_rintvnx2df2 (TARGET_SVE)
#define HAVE_roundvnx2df2 (TARGET_SVE)
#define HAVE_fix_truncvnx8hfvnx8hi2 (TARGET_SVE)
#define HAVE_fixuns_truncvnx8hfvnx8hi2 (TARGET_SVE)
#define HAVE_fix_truncvnx4sfvnx4si2 (TARGET_SVE)
#define HAVE_fixuns_truncvnx4sfvnx4si2 (TARGET_SVE)
#define HAVE_fix_truncvnx2dfvnx2di2 (TARGET_SVE)
#define HAVE_fixuns_truncvnx2dfvnx2di2 (TARGET_SVE)
#define HAVE_floatvnx8hivnx8hf2 (TARGET_SVE)
#define HAVE_floatunsvnx8hivnx8hf2 (TARGET_SVE)
#define HAVE_floatvnx4sivnx4sf2 (TARGET_SVE)
#define HAVE_floatunsvnx4sivnx4sf2 (TARGET_SVE)
#define HAVE_floatvnx2divnx2df2 (TARGET_SVE)
#define HAVE_floatunsvnx2divnx2df2 (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx16bi (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx16bi (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx16bi (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx16bi (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx8bi (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx8bi (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx8bi (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx8bi (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx4bi (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx4bi (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx4bi (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx4bi (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx16qi (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx16qi (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx16qi (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx16qi (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx8hi (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx8hi (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx8hi (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx8hi (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacku_hi_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacku_lo_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx8hf (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx8hf (TARGET_SVE)
#define HAVE_vec_unpacks_lo_vnx4sf (TARGET_SVE)
#define HAVE_vec_unpacks_hi_vnx4sf (TARGET_SVE)
#define HAVE_vec_unpacks_float_lo_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacks_float_hi_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacku_float_lo_vnx4si (TARGET_SVE)
#define HAVE_vec_unpacku_float_hi_vnx4si (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx4sf (TARGET_SVE)
#define HAVE_vec_pack_trunc_vnx2df (TARGET_SVE)
#define HAVE_vec_pack_sfix_trunc_vnx2df (TARGET_SVE)
#define HAVE_vec_pack_ufix_trunc_vnx2df (TARGET_SVE)
extern rtx        gen_indirect_jump                       (rtx);
extern rtx        gen_jump                                (rtx);
extern rtx        gen_ccmpsi                              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_ccmpdi                              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_fccmpsf                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_fccmpdf                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_fccmpesf                            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_fccmpedf                            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_condjump                            (rtx, rtx, rtx);
extern rtx        gen_casesi_dispatch                     (rtx, rtx, rtx);
extern rtx        gen_nop                                 (void);
extern rtx        gen_prefetch                            (rtx, rtx, rtx);
extern rtx        gen_trap                                (void);
extern rtx        gen_simple_return                       (void);
extern rtx        gen_insv_immsi                          (rtx, rtx, rtx);
extern rtx        gen_insv_immdi                          (rtx, rtx, rtx);
extern rtx        gen_load_pairsi                         (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairdi                         (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairsi                        (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairdi                        (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairsf                         (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairdf                         (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairsf                        (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairdf                        (rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairsi_si                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairsi_di                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairdi_si                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairdi_di                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairsf_si                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairsf_di                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairdf_si                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_loadwb_pairdf_di                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairsi_si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairsi_di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairdi_si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairdi_di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairsf_si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairsf_di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairdf_si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_storewb_pairdf_di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_addsi3_compare0                     (rtx, rtx, rtx);
extern rtx        gen_adddi3_compare0                     (rtx, rtx, rtx);
extern rtx        gen_addsi3_compareC                     (rtx, rtx, rtx);
extern rtx        gen_adddi3_compareC                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_subsi_compare0              (rtx, rtx);
extern rtx        gen_aarch64_subdi_compare0              (rtx, rtx);
extern rtx        gen_subsi3                              (rtx, rtx, rtx);
extern rtx        gen_subdi3                              (rtx, rtx, rtx);
extern rtx        gen_subsi3_compare1                     (rtx, rtx, rtx);
extern rtx        gen_subdi3_compare1                     (rtx, rtx, rtx);
extern rtx        gen_subsi3_compare1_imm                 (rtx, rtx, rtx, rtx);
extern rtx        gen_subdi3_compare1_imm                 (rtx, rtx, rtx, rtx);
extern rtx        gen_negsi2                              (rtx, rtx);
extern rtx        gen_negdi2                              (rtx, rtx);
extern rtx        gen_negsi2_compare0                     (rtx, rtx);
extern rtx        gen_negdi2_compare0                     (rtx, rtx);
extern rtx        gen_mulsi3                              (rtx, rtx, rtx);
extern rtx        gen_muldi3                              (rtx, rtx, rtx);
extern rtx        gen_maddsi                              (rtx, rtx, rtx, rtx);
extern rtx        gen_madddi                              (rtx, rtx, rtx, rtx);
extern rtx        gen_mulsidi3                            (rtx, rtx, rtx);
extern rtx        gen_umulsidi3                           (rtx, rtx, rtx);
extern rtx        gen_maddsidi4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_umaddsidi4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_msubsidi4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_umsubsidi4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_smuldi3_highpart                    (rtx, rtx, rtx);
extern rtx        gen_umuldi3_highpart                    (rtx, rtx, rtx);
extern rtx        gen_divsi3                              (rtx, rtx, rtx);
extern rtx        gen_udivsi3                             (rtx, rtx, rtx);
extern rtx        gen_divdi3                              (rtx, rtx, rtx);
extern rtx        gen_udivdi3                             (rtx, rtx, rtx);
extern rtx        gen_cmpsi                               (rtx, rtx);
extern rtx        gen_cmpdi                               (rtx, rtx);
extern rtx        gen_fcmpsf                              (rtx, rtx);
extern rtx        gen_fcmpdf                              (rtx, rtx);
extern rtx        gen_fcmpesf                             (rtx, rtx);
extern rtx        gen_fcmpedf                             (rtx, rtx);
extern rtx        gen_aarch64_cstoreqi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cstorehi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cstoresi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cstoredi                    (rtx, rtx, rtx);
extern rtx        gen_cstoreqi_neg                        (rtx, rtx, rtx);
extern rtx        gen_cstorehi_neg                        (rtx, rtx, rtx);
extern rtx        gen_cstoresi_neg                        (rtx, rtx, rtx);
extern rtx        gen_cstoredi_neg                        (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32b                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32h                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32w                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32x                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32cb                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32ch                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32cw                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_crc32cx                     (rtx, rtx, rtx);
extern rtx        gen_csinc3si_insn                       (rtx, rtx, rtx, rtx);
extern rtx        gen_csinc3di_insn                       (rtx, rtx, rtx, rtx);
extern rtx        gen_csneg3_uxtw_insn                    (rtx, rtx, rtx, rtx);
extern rtx        gen_csneg3si_insn                       (rtx, rtx, rtx, rtx);
extern rtx        gen_csneg3di_insn                       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uqdecsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqdecdi                     (rtx, rtx, rtx);
extern rtx        gen_andsi3                              (rtx, rtx, rtx);
extern rtx        gen_iorsi3                              (rtx, rtx, rtx);
extern rtx        gen_xorsi3                              (rtx, rtx, rtx);
extern rtx        gen_anddi3                              (rtx, rtx, rtx);
extern rtx        gen_iordi3                              (rtx, rtx, rtx);
extern rtx        gen_xordi3                              (rtx, rtx, rtx);
extern rtx        gen_one_cmplsi2                         (rtx, rtx);
extern rtx        gen_one_cmpldi2                         (rtx, rtx);
extern rtx        gen_and_one_cmpl_ashlsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_ashlsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_ashlsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_ashrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_ashrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_ashrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_lshrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_lshrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_lshrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_rotrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_rotrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_rotrsi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_ashldi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_ashldi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_ashldi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_ashrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_ashrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_ashrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_lshrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_lshrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_lshrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_and_one_cmpl_rotrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_ior_one_cmpl_rotrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_xor_one_cmpl_rotrdi3                (rtx, rtx, rtx, rtx);
extern rtx        gen_clzsi2                              (rtx, rtx);
extern rtx        gen_clzdi2                              (rtx, rtx);
extern rtx        gen_clrsbsi2                            (rtx, rtx);
extern rtx        gen_clrsbdi2                            (rtx, rtx);
extern rtx        gen_rbitsi2                             (rtx, rtx);
extern rtx        gen_rbitdi2                             (rtx, rtx);
extern rtx        gen_ctzsi2                              (rtx, rtx);
extern rtx        gen_ctzdi2                              (rtx, rtx);
extern rtx        gen_bswapsi2                            (rtx, rtx);
extern rtx        gen_bswapdi2                            (rtx, rtx);
extern rtx        gen_bswaphi2                            (rtx, rtx);
extern rtx        gen_rev16si2                            (rtx, rtx, rtx, rtx);
extern rtx        gen_rev16di2                            (rtx, rtx, rtx, rtx);
extern rtx        gen_rev16si2_alt                        (rtx, rtx, rtx, rtx);
extern rtx        gen_rev16di2_alt                        (rtx, rtx, rtx, rtx);
extern rtx        gen_btrunchf2                           (rtx, rtx);
extern rtx        gen_ceilhf2                             (rtx, rtx);
extern rtx        gen_floorhf2                            (rtx, rtx);
extern rtx        gen_frintnhf2                           (rtx, rtx);
extern rtx        gen_nearbyinthf2                        (rtx, rtx);
extern rtx        gen_rinthf2                             (rtx, rtx);
extern rtx        gen_roundhf2                            (rtx, rtx);
extern rtx        gen_btruncsf2                           (rtx, rtx);
extern rtx        gen_ceilsf2                             (rtx, rtx);
extern rtx        gen_floorsf2                            (rtx, rtx);
extern rtx        gen_frintnsf2                           (rtx, rtx);
extern rtx        gen_nearbyintsf2                        (rtx, rtx);
extern rtx        gen_rintsf2                             (rtx, rtx);
extern rtx        gen_roundsf2                            (rtx, rtx);
extern rtx        gen_btruncdf2                           (rtx, rtx);
extern rtx        gen_ceildf2                             (rtx, rtx);
extern rtx        gen_floordf2                            (rtx, rtx);
extern rtx        gen_frintndf2                           (rtx, rtx);
extern rtx        gen_nearbyintdf2                        (rtx, rtx);
extern rtx        gen_rintdf2                             (rtx, rtx);
extern rtx        gen_rounddf2                            (rtx, rtx);
extern rtx        gen_lbtrunchfsi2                        (rtx, rtx);
extern rtx        gen_lceilhfsi2                          (rtx, rtx);
extern rtx        gen_lfloorhfsi2                         (rtx, rtx);
extern rtx        gen_lroundhfsi2                         (rtx, rtx);
extern rtx        gen_lfrintnhfsi2                        (rtx, rtx);
extern rtx        gen_lbtruncuhfsi2                       (rtx, rtx);
extern rtx        gen_lceiluhfsi2                         (rtx, rtx);
extern rtx        gen_lflooruhfsi2                        (rtx, rtx);
extern rtx        gen_lrounduhfsi2                        (rtx, rtx);
extern rtx        gen_lfrintnuhfsi2                       (rtx, rtx);
extern rtx        gen_lbtrunchfdi2                        (rtx, rtx);
extern rtx        gen_lceilhfdi2                          (rtx, rtx);
extern rtx        gen_lfloorhfdi2                         (rtx, rtx);
extern rtx        gen_lroundhfdi2                         (rtx, rtx);
extern rtx        gen_lfrintnhfdi2                        (rtx, rtx);
extern rtx        gen_lbtruncuhfdi2                       (rtx, rtx);
extern rtx        gen_lceiluhfdi2                         (rtx, rtx);
extern rtx        gen_lflooruhfdi2                        (rtx, rtx);
extern rtx        gen_lrounduhfdi2                        (rtx, rtx);
extern rtx        gen_lfrintnuhfdi2                       (rtx, rtx);
extern rtx        gen_lbtruncsfsi2                        (rtx, rtx);
extern rtx        gen_lceilsfsi2                          (rtx, rtx);
extern rtx        gen_lfloorsfsi2                         (rtx, rtx);
extern rtx        gen_lroundsfsi2                         (rtx, rtx);
extern rtx        gen_lfrintnsfsi2                        (rtx, rtx);
extern rtx        gen_lbtruncusfsi2                       (rtx, rtx);
extern rtx        gen_lceilusfsi2                         (rtx, rtx);
extern rtx        gen_lfloorusfsi2                        (rtx, rtx);
extern rtx        gen_lroundusfsi2                        (rtx, rtx);
extern rtx        gen_lfrintnusfsi2                       (rtx, rtx);
extern rtx        gen_lbtruncsfdi2                        (rtx, rtx);
extern rtx        gen_lceilsfdi2                          (rtx, rtx);
extern rtx        gen_lfloorsfdi2                         (rtx, rtx);
extern rtx        gen_lroundsfdi2                         (rtx, rtx);
extern rtx        gen_lfrintnsfdi2                        (rtx, rtx);
extern rtx        gen_lbtruncusfdi2                       (rtx, rtx);
extern rtx        gen_lceilusfdi2                         (rtx, rtx);
extern rtx        gen_lfloorusfdi2                        (rtx, rtx);
extern rtx        gen_lroundusfdi2                        (rtx, rtx);
extern rtx        gen_lfrintnusfdi2                       (rtx, rtx);
extern rtx        gen_lbtruncdfsi2                        (rtx, rtx);
extern rtx        gen_lceildfsi2                          (rtx, rtx);
extern rtx        gen_lfloordfsi2                         (rtx, rtx);
extern rtx        gen_lrounddfsi2                         (rtx, rtx);
extern rtx        gen_lfrintndfsi2                        (rtx, rtx);
extern rtx        gen_lbtruncudfsi2                       (rtx, rtx);
extern rtx        gen_lceiludfsi2                         (rtx, rtx);
extern rtx        gen_lfloorudfsi2                        (rtx, rtx);
extern rtx        gen_lroundudfsi2                        (rtx, rtx);
extern rtx        gen_lfrintnudfsi2                       (rtx, rtx);
extern rtx        gen_lbtruncdfdi2                        (rtx, rtx);
extern rtx        gen_lceildfdi2                          (rtx, rtx);
extern rtx        gen_lfloordfdi2                         (rtx, rtx);
extern rtx        gen_lrounddfdi2                         (rtx, rtx);
extern rtx        gen_lfrintndfdi2                        (rtx, rtx);
extern rtx        gen_lbtruncudfdi2                       (rtx, rtx);
extern rtx        gen_lceiludfdi2                         (rtx, rtx);
extern rtx        gen_lfloorudfdi2                        (rtx, rtx);
extern rtx        gen_lroundudfdi2                        (rtx, rtx);
extern rtx        gen_lfrintnudfdi2                       (rtx, rtx);
extern rtx        gen_fmahf4                              (rtx, rtx, rtx, rtx);
extern rtx        gen_fmasf4                              (rtx, rtx, rtx, rtx);
extern rtx        gen_fmadf4                              (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmahf4                             (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmasf4                             (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmadf4                             (rtx, rtx, rtx, rtx);
extern rtx        gen_fmssf4                              (rtx, rtx, rtx, rtx);
extern rtx        gen_fmsdf4                              (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmssf4                             (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmsdf4                             (rtx, rtx, rtx, rtx);
extern rtx        gen_extendsfdf2                         (rtx, rtx);
extern rtx        gen_extendhfsf2                         (rtx, rtx);
extern rtx        gen_extendhfdf2                         (rtx, rtx);
extern rtx        gen_truncdfsf2                          (rtx, rtx);
extern rtx        gen_truncsfhf2                          (rtx, rtx);
extern rtx        gen_truncdfhf2                          (rtx, rtx);
extern rtx        gen_fix_truncsfsi2                      (rtx, rtx);
extern rtx        gen_fixuns_truncsfsi2                   (rtx, rtx);
extern rtx        gen_fix_truncdfdi2                      (rtx, rtx);
extern rtx        gen_fixuns_truncdfdi2                   (rtx, rtx);
extern rtx        gen_fix_trunchfsi2                      (rtx, rtx);
extern rtx        gen_fixuns_trunchfsi2                   (rtx, rtx);
extern rtx        gen_fix_trunchfdi2                      (rtx, rtx);
extern rtx        gen_fixuns_trunchfdi2                   (rtx, rtx);
extern rtx        gen_fix_truncdfsi2                      (rtx, rtx);
extern rtx        gen_fixuns_truncdfsi2                   (rtx, rtx);
extern rtx        gen_fix_truncsfdi2                      (rtx, rtx);
extern rtx        gen_fixuns_truncsfdi2                   (rtx, rtx);
extern rtx        gen_floatsisf2                          (rtx, rtx);
extern rtx        gen_floatunssisf2                       (rtx, rtx);
extern rtx        gen_floatdidf2                          (rtx, rtx);
extern rtx        gen_floatunsdidf2                       (rtx, rtx);
extern rtx        gen_floatdisf2                          (rtx, rtx);
extern rtx        gen_floatunsdisf2                       (rtx, rtx);
extern rtx        gen_floatsidf2                          (rtx, rtx);
extern rtx        gen_floatunssidf2                       (rtx, rtx);
extern rtx        gen_aarch64_fp16_floatsihf2             (rtx, rtx);
extern rtx        gen_aarch64_fp16_floatunssihf2          (rtx, rtx);
extern rtx        gen_aarch64_fp16_floatdihf2             (rtx, rtx);
extern rtx        gen_aarch64_fp16_floatunsdihf2          (rtx, rtx);
extern rtx        gen_fcvtzssf3                           (rtx, rtx, rtx);
extern rtx        gen_fcvtzusf3                           (rtx, rtx, rtx);
extern rtx        gen_fcvtzsdf3                           (rtx, rtx, rtx);
extern rtx        gen_fcvtzudf3                           (rtx, rtx, rtx);
extern rtx        gen_scvtfsi3                            (rtx, rtx, rtx);
extern rtx        gen_ucvtfsi3                            (rtx, rtx, rtx);
extern rtx        gen_scvtfdi3                            (rtx, rtx, rtx);
extern rtx        gen_ucvtfdi3                            (rtx, rtx, rtx);
extern rtx        gen_fcvtzshfsi3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuhfsi3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzshfdi3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuhfdi3                         (rtx, rtx, rtx);
extern rtx        gen_scvtfsihf3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfsihf3                          (rtx, rtx, rtx);
extern rtx        gen_scvtfdihf3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfdihf3                          (rtx, rtx, rtx);
extern rtx        gen_fcvtzshf3                           (rtx, rtx, rtx);
extern rtx        gen_fcvtzuhf3                           (rtx, rtx, rtx);
extern rtx        gen_scvtfhi3                            (rtx, rtx, rtx);
extern rtx        gen_ucvtfhi3                            (rtx, rtx, rtx);
extern rtx        gen_addhf3                              (rtx, rtx, rtx);
extern rtx        gen_addsf3                              (rtx, rtx, rtx);
extern rtx        gen_adddf3                              (rtx, rtx, rtx);
extern rtx        gen_subhf3                              (rtx, rtx, rtx);
extern rtx        gen_subsf3                              (rtx, rtx, rtx);
extern rtx        gen_subdf3                              (rtx, rtx, rtx);
extern rtx        gen_mulhf3                              (rtx, rtx, rtx);
extern rtx        gen_mulsf3                              (rtx, rtx, rtx);
extern rtx        gen_muldf3                              (rtx, rtx, rtx);
extern rtx        gen_neghf2                              (rtx, rtx);
extern rtx        gen_negsf2                              (rtx, rtx);
extern rtx        gen_negdf2                              (rtx, rtx);
extern rtx        gen_abshf2                              (rtx, rtx);
extern rtx        gen_abssf2                              (rtx, rtx);
extern rtx        gen_absdf2                              (rtx, rtx);
extern rtx        gen_smaxsf3                             (rtx, rtx, rtx);
extern rtx        gen_smaxdf3                             (rtx, rtx, rtx);
extern rtx        gen_sminsf3                             (rtx, rtx, rtx);
extern rtx        gen_smindf3                             (rtx, rtx, rtx);
extern rtx        gen_smax_nanhf3                         (rtx, rtx, rtx);
extern rtx        gen_smin_nanhf3                         (rtx, rtx, rtx);
extern rtx        gen_fmaxhf3                             (rtx, rtx, rtx);
extern rtx        gen_fminhf3                             (rtx, rtx, rtx);
extern rtx        gen_smax_nansf3                         (rtx, rtx, rtx);
extern rtx        gen_smin_nansf3                         (rtx, rtx, rtx);
extern rtx        gen_fmaxsf3                             (rtx, rtx, rtx);
extern rtx        gen_fminsf3                             (rtx, rtx, rtx);
extern rtx        gen_smax_nandf3                         (rtx, rtx, rtx);
extern rtx        gen_smin_nandf3                         (rtx, rtx, rtx);
extern rtx        gen_fmaxdf3                             (rtx, rtx, rtx);
extern rtx        gen_fmindf3                             (rtx, rtx, rtx);
extern rtx        gen_aarch64_movdi_tilow                 (rtx, rtx);
extern rtx        gen_aarch64_movdi_tflow                 (rtx, rtx);
extern rtx        gen_aarch64_movdi_tihigh                (rtx, rtx);
extern rtx        gen_aarch64_movdi_tfhigh                (rtx, rtx);
extern rtx        gen_aarch64_movtihigh_di                (rtx, rtx);
extern rtx        gen_aarch64_movtfhigh_di                (rtx, rtx);
extern rtx        gen_aarch64_movtilow_di                 (rtx, rtx);
extern rtx        gen_aarch64_movtflow_di                 (rtx, rtx);
extern rtx        gen_aarch64_movtilow_tilow              (rtx, rtx);
extern rtx        gen_add_losym_si                        (rtx, rtx, rtx);
extern rtx        gen_add_losym_di                        (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_si                    (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_di                    (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_sidi                  (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_28k_si                (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_28k_di                (rtx, rtx, rtx);
extern rtx        gen_ldr_got_small_28k_sidi              (rtx, rtx, rtx);
extern rtx        gen_ldr_got_tiny                        (rtx, rtx);
extern rtx        gen_aarch64_load_tp_hard                (rtx);
extern rtx        gen_tlsie_small_si                      (rtx, rtx);
extern rtx        gen_tlsie_small_di                      (rtx, rtx);
extern rtx        gen_tlsie_small_sidi                    (rtx, rtx);
extern rtx        gen_tlsie_tiny_si                       (rtx, rtx, rtx);
extern rtx        gen_tlsie_tiny_di                       (rtx, rtx, rtx);
extern rtx        gen_tlsie_tiny_sidi                     (rtx, rtx, rtx);
extern rtx        gen_tlsle12_si                          (rtx, rtx, rtx);
extern rtx        gen_tlsle12_di                          (rtx, rtx, rtx);
extern rtx        gen_tlsle24_si                          (rtx, rtx, rtx);
extern rtx        gen_tlsle24_di                          (rtx, rtx, rtx);
extern rtx        gen_tlsle32_si                          (rtx, rtx);
extern rtx        gen_tlsle32_di                          (rtx, rtx);
extern rtx        gen_tlsle48_si                          (rtx, rtx);
extern rtx        gen_tlsle48_di                          (rtx, rtx);
extern rtx        gen_tlsdesc_small_advsimd_si            (rtx);
extern rtx        gen_tlsdesc_small_advsimd_di            (rtx);
extern rtx        gen_tlsdesc_small_sve_si                (rtx);
extern rtx        gen_tlsdesc_small_sve_di                (rtx);
extern rtx        gen_stack_tie                           (rtx, rtx);
extern rtx        gen_pacisp                              (void);
extern rtx        gen_autisp                              (void);
extern rtx        gen_paci1716                            (void);
extern rtx        gen_auti1716                            (void);
extern rtx        gen_xpaclri                             (void);
extern rtx        gen_blockage                            (void);
extern rtx        gen_probe_stack_range                   (rtx, rtx, rtx);
extern rtx        gen_stack_protect_set_si                (rtx, rtx);
extern rtx        gen_stack_protect_set_di                (rtx, rtx);
extern rtx        gen_stack_protect_test_si               (rtx, rtx, rtx);
extern rtx        gen_stack_protect_test_di               (rtx, rtx, rtx);
extern rtx        gen_set_fpcr                            (rtx);
extern rtx        gen_get_fpcr                            (rtx);
extern rtx        gen_set_fpsr                            (rtx);
extern rtx        gen_get_fpsr                            (rtx);
extern rtx        gen_speculation_tracker                 (rtx);
extern rtx        gen_speculation_barrier                 (void);
extern rtx        gen_despeculate_simpleqi                (rtx, rtx, rtx);
extern rtx        gen_despeculate_simplehi                (rtx, rtx, rtx);
extern rtx        gen_despeculate_simplesi                (rtx, rtx, rtx);
extern rtx        gen_despeculate_simpledi                (rtx, rtx, rtx);
extern rtx        gen_despeculate_simpleti                (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_dupv8qi                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv4hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv2si                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv4hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv2sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_dupv2df                (rtx, rtx);
extern rtx        gen_aarch64_dup_lanev8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev4hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev8hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev2sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev4sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lanev2df                (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_128v8qi         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_64v16qi         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_128v4hi         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_64v8hi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_128v2si         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_64v4si          (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_128v4hf         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_64v8hf          (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_128v2sf         (rtx, rtx, rtx);
extern rtx        gen_aarch64_dup_lane_to_64v4sf          (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v8qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v16qi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v4hi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v8hi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v2si             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v4si             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v2di             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v4hf             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v8hf             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v2sf             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v4sf             (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_lane0v2df             (rtx, rtx, rtx);
extern rtx        gen_load_pairv8qi                       (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairv4hi                       (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairv4hf                       (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairv2si                       (rtx, rtx, rtx, rtx);
extern rtx        gen_load_pairv2sf                       (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairv8qi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairv4hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairv4hf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairv2si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_store_pairv2sf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v16qilow      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v8hilow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v4silow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v2dilow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v8hflow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v4sflow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v2dflow       (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v16qihigh     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v8hihigh      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v4sihigh      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v2dihigh      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v8hfhigh      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v4sfhigh      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_mov_from_v2dfhigh      (rtx, rtx, rtx);
extern rtx        gen_ornv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_ornv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_ornv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_ornv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_ornv2si3                            (rtx, rtx, rtx);
extern rtx        gen_ornv4si3                            (rtx, rtx, rtx);
extern rtx        gen_ornv2di3                            (rtx, rtx, rtx);
extern rtx        gen_bicv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_bicv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_bicv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_bicv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_bicv2si3                            (rtx, rtx, rtx);
extern rtx        gen_bicv4si3                            (rtx, rtx, rtx);
extern rtx        gen_bicv2di3                            (rtx, rtx, rtx);
extern rtx        gen_addv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_addv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_addv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_addv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_addv2si3                            (rtx, rtx, rtx);
extern rtx        gen_addv4si3                            (rtx, rtx, rtx);
extern rtx        gen_addv2di3                            (rtx, rtx, rtx);
extern rtx        gen_subv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_subv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_subv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_subv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_subv2si3                            (rtx, rtx, rtx);
extern rtx        gen_subv4si3                            (rtx, rtx, rtx);
extern rtx        gen_subv2di3                            (rtx, rtx, rtx);
extern rtx        gen_mulv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_mulv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_mulv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_mulv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_mulv2si3                            (rtx, rtx, rtx);
extern rtx        gen_mulv4si3                            (rtx, rtx, rtx);
extern rtx        gen_bswapv4hi2                          (rtx, rtx);
extern rtx        gen_bswapv8hi2                          (rtx, rtx);
extern rtx        gen_bswapv2si2                          (rtx, rtx);
extern rtx        gen_bswapv4si2                          (rtx, rtx);
extern rtx        gen_bswapv2di2                          (rtx, rtx);
extern rtx        gen_aarch64_rbitv8qi                    (rtx, rtx);
extern rtx        gen_aarch64_rbitv16qi                   (rtx, rtx);
extern rtx        gen_aarch64_sdotv8qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udotv8qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sdotv16qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udotv16qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sdot_lanev8qi               (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udot_lanev8qi               (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sdot_lanev16qi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udot_lanev16qi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sdot_laneqv8qi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udot_laneqv8qi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sdot_laneqv16qi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_udot_laneqv16qi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtev4hf                  (rtx, rtx);
extern rtx        gen_aarch64_rsqrtev8hf                  (rtx, rtx);
extern rtx        gen_aarch64_rsqrtev2sf                  (rtx, rtx);
extern rtx        gen_aarch64_rsqrtev4sf                  (rtx, rtx);
extern rtx        gen_aarch64_rsqrtev2df                  (rtx, rtx);
extern rtx        gen_aarch64_rsqrtehf                    (rtx, rtx);
extern rtx        gen_aarch64_rsqrtesf                    (rtx, rtx);
extern rtx        gen_aarch64_rsqrtedf                    (rtx, rtx);
extern rtx        gen_aarch64_rsqrtsv4hf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtsv8hf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtsv2sf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtsv4sf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtsv2df                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtshf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtssf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsqrtsdf                    (rtx, rtx, rtx);
extern rtx        gen_negv8qi2                            (rtx, rtx);
extern rtx        gen_negv16qi2                           (rtx, rtx);
extern rtx        gen_negv4hi2                            (rtx, rtx);
extern rtx        gen_negv8hi2                            (rtx, rtx);
extern rtx        gen_negv2si2                            (rtx, rtx);
extern rtx        gen_negv4si2                            (rtx, rtx);
extern rtx        gen_negv2di2                            (rtx, rtx);
extern rtx        gen_absv8qi2                            (rtx, rtx);
extern rtx        gen_absv16qi2                           (rtx, rtx);
extern rtx        gen_absv4hi2                            (rtx, rtx);
extern rtx        gen_absv8hi2                            (rtx, rtx);
extern rtx        gen_absv2si2                            (rtx, rtx);
extern rtx        gen_absv4si2                            (rtx, rtx);
extern rtx        gen_absv2di2                            (rtx, rtx);
extern rtx        gen_aarch64_absv8qi                     (rtx, rtx);
extern rtx        gen_aarch64_absv16qi                    (rtx, rtx);
extern rtx        gen_aarch64_absv4hi                     (rtx, rtx);
extern rtx        gen_aarch64_absv8hi                     (rtx, rtx);
extern rtx        gen_aarch64_absv2si                     (rtx, rtx);
extern rtx        gen_aarch64_absv4si                     (rtx, rtx);
extern rtx        gen_aarch64_absv2di                     (rtx, rtx);
extern rtx        gen_aarch64_absdi                       (rtx, rtx);
extern rtx        gen_abdv8qi_3                           (rtx, rtx, rtx);
extern rtx        gen_abdv16qi_3                          (rtx, rtx, rtx);
extern rtx        gen_abdv4hi_3                           (rtx, rtx, rtx);
extern rtx        gen_abdv8hi_3                           (rtx, rtx, rtx);
extern rtx        gen_abdv2si_3                           (rtx, rtx, rtx);
extern rtx        gen_abdv4si_3                           (rtx, rtx, rtx);
extern rtx        gen_abav8qi_3                           (rtx, rtx, rtx, rtx);
extern rtx        gen_abav16qi_3                          (rtx, rtx, rtx, rtx);
extern rtx        gen_abav4hi_3                           (rtx, rtx, rtx, rtx);
extern rtx        gen_abav8hi_3                           (rtx, rtx, rtx, rtx);
extern rtx        gen_abav2si_3                           (rtx, rtx, rtx, rtx);
extern rtx        gen_abav4si_3                           (rtx, rtx, rtx, rtx);
extern rtx        gen_fabdv4hf3                           (rtx, rtx, rtx);
extern rtx        gen_fabdv8hf3                           (rtx, rtx, rtx);
extern rtx        gen_fabdv2sf3                           (rtx, rtx, rtx);
extern rtx        gen_fabdv4sf3                           (rtx, rtx, rtx);
extern rtx        gen_fabdv2df3                           (rtx, rtx, rtx);
extern rtx        gen_fabdhf3                             (rtx, rtx, rtx);
extern rtx        gen_fabdsf3                             (rtx, rtx, rtx);
extern rtx        gen_fabddf3                             (rtx, rtx, rtx);
extern rtx        gen_andv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_andv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_andv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_andv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_andv2si3                            (rtx, rtx, rtx);
extern rtx        gen_andv4si3                            (rtx, rtx, rtx);
extern rtx        gen_andv2di3                            (rtx, rtx, rtx);
extern rtx        gen_iorv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_iorv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_iorv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_iorv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_iorv2si3                            (rtx, rtx, rtx);
extern rtx        gen_iorv4si3                            (rtx, rtx, rtx);
extern rtx        gen_iorv2di3                            (rtx, rtx, rtx);
extern rtx        gen_xorv8qi3                            (rtx, rtx, rtx);
extern rtx        gen_xorv16qi3                           (rtx, rtx, rtx);
extern rtx        gen_xorv4hi3                            (rtx, rtx, rtx);
extern rtx        gen_xorv8hi3                            (rtx, rtx, rtx);
extern rtx        gen_xorv2si3                            (rtx, rtx, rtx);
extern rtx        gen_xorv4si3                            (rtx, rtx, rtx);
extern rtx        gen_xorv2di3                            (rtx, rtx, rtx);
extern rtx        gen_one_cmplv8qi2                       (rtx, rtx);
extern rtx        gen_one_cmplv16qi2                      (rtx, rtx);
extern rtx        gen_one_cmplv4hi2                       (rtx, rtx);
extern rtx        gen_one_cmplv8hi2                       (rtx, rtx);
extern rtx        gen_one_cmplv2si2                       (rtx, rtx);
extern rtx        gen_one_cmplv4si2                       (rtx, rtx);
extern rtx        gen_one_cmplv2di2                       (rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv8qi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv16qi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv4hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv8hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv2si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv8qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv16qi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv4hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv8hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv2si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv4si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_lshrv2di               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv8qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv16qi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv4hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv8hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv2si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv4si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ashrv2di               (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv8qi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv16qi           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv4hi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv8hi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv2si            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv4si            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_imm_shlv2di            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv8qi           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv16qi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv4hi           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv8hi           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv2si           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv4si           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_sshlv2di           (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv8qi_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv16qi_unsigned  (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv4hi_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv8hi_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv2si_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv4si_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv2di_unsigned   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv8qi_signed     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv16qi_signed    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv4hi_signed     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv8hi_signed     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv2si_signed     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv4si_signed     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_reg_shlv2di_signed     (rtx, rtx, rtx);
extern rtx        gen_vec_shr_v8qi                        (rtx, rtx, rtx);
extern rtx        gen_vec_shr_v4hi                        (rtx, rtx, rtx);
extern rtx        gen_vec_shr_v4hf                        (rtx, rtx, rtx);
extern rtx        gen_vec_shr_v2si                        (rtx, rtx, rtx);
extern rtx        gen_vec_shr_v2sf                        (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv2di            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv4hf            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv8hf            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv2sf            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv4sf            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_setv2df            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav8qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav4hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav2si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlav4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv8qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv4hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv2si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_mlsv4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_smaxv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_sminv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_uminv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_sminv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_umaxv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_uminv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_smaxv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_sminv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_uminv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_sminv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_uminv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv2si3                           (rtx, rtx, rtx);
extern rtx        gen_sminv2si3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv2si3                           (rtx, rtx, rtx);
extern rtx        gen_uminv2si3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv4si3                           (rtx, rtx, rtx);
extern rtx        gen_sminv4si3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv4si3                           (rtx, rtx, rtx);
extern rtx        gen_uminv4si3                           (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_umaxpv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uminpv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smax_nanpv4hf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smin_nanpv4hf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smax_nanpv8hf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smin_nanpv8hf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smax_nanpv2sf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smin_nanpv2sf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smax_nanpv4sf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smin_nanpv4sf               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_smax_nanpv2df               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smin_nanpv2df               (rtx, rtx, rtx);
extern rtx        gen_aarch64_smaxpv2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sminpv2df                   (rtx, rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v16qi         (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v8hi          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v4si          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v8hf          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v4sf          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v2di          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_v2df          (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v16qi      (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v8hi       (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v4si       (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v8hf       (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v4sf       (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v2di       (rtx, rtx);
extern rtx        gen_move_lo_quad_internal_be_v2df       (rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v16qi     (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v8hi      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v4si      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v2di      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v8hf      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v4sf      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_v2df      (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v16qi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v8hi   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v4si   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v2di   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v8hf   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v4sf   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_move_hi_quad_be_v2df   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_pack_trunc_v8hi    (rtx, rtx);
extern rtx        gen_aarch64_simd_vec_pack_trunc_v4si    (rtx, rtx);
extern rtx        gen_aarch64_simd_vec_pack_trunc_v2di    (rtx, rtx);
extern rtx        gen_vec_pack_trunc_v8hi                 (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_v4si                 (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_v2di                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_lo_v16qi   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_lo_v16qi   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_lo_v8hi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_lo_v8hi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_lo_v4si    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_lo_v4si    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_hi_v16qi   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_hi_v16qi   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_hi_v8hi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_hi_v8hi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_hi_v4si    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacku_hi_v4si    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_lo_v16qi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_lo_v16qi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_lo_v8hi      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_lo_v8hi      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_lo_v4si      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_lo_v4si      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_hi_v16qi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_hi_v16qi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_hi_v8hi      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_hi_v8hi      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_smult_hi_v4si      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_umult_hi_v4si      (rtx, rtx, rtx, rtx);
extern rtx        gen_addv4hf3                            (rtx, rtx, rtx);
extern rtx        gen_addv8hf3                            (rtx, rtx, rtx);
extern rtx        gen_addv2sf3                            (rtx, rtx, rtx);
extern rtx        gen_addv4sf3                            (rtx, rtx, rtx);
extern rtx        gen_addv2df3                            (rtx, rtx, rtx);
extern rtx        gen_subv4hf3                            (rtx, rtx, rtx);
extern rtx        gen_subv8hf3                            (rtx, rtx, rtx);
extern rtx        gen_subv2sf3                            (rtx, rtx, rtx);
extern rtx        gen_subv4sf3                            (rtx, rtx, rtx);
extern rtx        gen_subv2df3                            (rtx, rtx, rtx);
extern rtx        gen_mulv4hf3                            (rtx, rtx, rtx);
extern rtx        gen_mulv8hf3                            (rtx, rtx, rtx);
extern rtx        gen_mulv2sf3                            (rtx, rtx, rtx);
extern rtx        gen_mulv4sf3                            (rtx, rtx, rtx);
extern rtx        gen_mulv2df3                            (rtx, rtx, rtx);
extern rtx        gen_negv4hf2                            (rtx, rtx);
extern rtx        gen_negv8hf2                            (rtx, rtx);
extern rtx        gen_negv2sf2                            (rtx, rtx);
extern rtx        gen_negv4sf2                            (rtx, rtx);
extern rtx        gen_negv2df2                            (rtx, rtx);
extern rtx        gen_absv4hf2                            (rtx, rtx);
extern rtx        gen_absv8hf2                            (rtx, rtx);
extern rtx        gen_absv2sf2                            (rtx, rtx);
extern rtx        gen_absv4sf2                            (rtx, rtx);
extern rtx        gen_absv2df2                            (rtx, rtx);
extern rtx        gen_fmav4hf4                            (rtx, rtx, rtx, rtx);
extern rtx        gen_fmav8hf4                            (rtx, rtx, rtx, rtx);
extern rtx        gen_fmav2sf4                            (rtx, rtx, rtx, rtx);
extern rtx        gen_fmav4sf4                            (rtx, rtx, rtx, rtx);
extern rtx        gen_fmav2df4                            (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmav4hf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmav8hf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmav2sf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmav4sf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmav2df4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_btruncv4hf2                         (rtx, rtx);
extern rtx        gen_ceilv4hf2                           (rtx, rtx);
extern rtx        gen_floorv4hf2                          (rtx, rtx);
extern rtx        gen_frintnv4hf2                         (rtx, rtx);
extern rtx        gen_nearbyintv4hf2                      (rtx, rtx);
extern rtx        gen_rintv4hf2                           (rtx, rtx);
extern rtx        gen_roundv4hf2                          (rtx, rtx);
extern rtx        gen_btruncv8hf2                         (rtx, rtx);
extern rtx        gen_ceilv8hf2                           (rtx, rtx);
extern rtx        gen_floorv8hf2                          (rtx, rtx);
extern rtx        gen_frintnv8hf2                         (rtx, rtx);
extern rtx        gen_nearbyintv8hf2                      (rtx, rtx);
extern rtx        gen_rintv8hf2                           (rtx, rtx);
extern rtx        gen_roundv8hf2                          (rtx, rtx);
extern rtx        gen_btruncv2sf2                         (rtx, rtx);
extern rtx        gen_ceilv2sf2                           (rtx, rtx);
extern rtx        gen_floorv2sf2                          (rtx, rtx);
extern rtx        gen_frintnv2sf2                         (rtx, rtx);
extern rtx        gen_nearbyintv2sf2                      (rtx, rtx);
extern rtx        gen_rintv2sf2                           (rtx, rtx);
extern rtx        gen_roundv2sf2                          (rtx, rtx);
extern rtx        gen_btruncv4sf2                         (rtx, rtx);
extern rtx        gen_ceilv4sf2                           (rtx, rtx);
extern rtx        gen_floorv4sf2                          (rtx, rtx);
extern rtx        gen_frintnv4sf2                         (rtx, rtx);
extern rtx        gen_nearbyintv4sf2                      (rtx, rtx);
extern rtx        gen_rintv4sf2                           (rtx, rtx);
extern rtx        gen_roundv4sf2                          (rtx, rtx);
extern rtx        gen_btruncv2df2                         (rtx, rtx);
extern rtx        gen_ceilv2df2                           (rtx, rtx);
extern rtx        gen_floorv2df2                          (rtx, rtx);
extern rtx        gen_frintnv2df2                         (rtx, rtx);
extern rtx        gen_nearbyintv2df2                      (rtx, rtx);
extern rtx        gen_rintv2df2                           (rtx, rtx);
extern rtx        gen_roundv2df2                          (rtx, rtx);
extern rtx        gen_lbtruncv4hfv4hi2                    (rtx, rtx);
extern rtx        gen_lceilv4hfv4hi2                      (rtx, rtx);
extern rtx        gen_lfloorv4hfv4hi2                     (rtx, rtx);
extern rtx        gen_lroundv4hfv4hi2                     (rtx, rtx);
extern rtx        gen_lfrintnv4hfv4hi2                    (rtx, rtx);
extern rtx        gen_lbtruncuv4hfv4hi2                   (rtx, rtx);
extern rtx        gen_lceiluv4hfv4hi2                     (rtx, rtx);
extern rtx        gen_lflooruv4hfv4hi2                    (rtx, rtx);
extern rtx        gen_lrounduv4hfv4hi2                    (rtx, rtx);
extern rtx        gen_lfrintnuv4hfv4hi2                   (rtx, rtx);
extern rtx        gen_lbtruncv8hfv8hi2                    (rtx, rtx);
extern rtx        gen_lceilv8hfv8hi2                      (rtx, rtx);
extern rtx        gen_lfloorv8hfv8hi2                     (rtx, rtx);
extern rtx        gen_lroundv8hfv8hi2                     (rtx, rtx);
extern rtx        gen_lfrintnv8hfv8hi2                    (rtx, rtx);
extern rtx        gen_lbtruncuv8hfv8hi2                   (rtx, rtx);
extern rtx        gen_lceiluv8hfv8hi2                     (rtx, rtx);
extern rtx        gen_lflooruv8hfv8hi2                    (rtx, rtx);
extern rtx        gen_lrounduv8hfv8hi2                    (rtx, rtx);
extern rtx        gen_lfrintnuv8hfv8hi2                   (rtx, rtx);
extern rtx        gen_lbtruncv2sfv2si2                    (rtx, rtx);
extern rtx        gen_lceilv2sfv2si2                      (rtx, rtx);
extern rtx        gen_lfloorv2sfv2si2                     (rtx, rtx);
extern rtx        gen_lroundv2sfv2si2                     (rtx, rtx);
extern rtx        gen_lfrintnv2sfv2si2                    (rtx, rtx);
extern rtx        gen_lbtruncuv2sfv2si2                   (rtx, rtx);
extern rtx        gen_lceiluv2sfv2si2                     (rtx, rtx);
extern rtx        gen_lflooruv2sfv2si2                    (rtx, rtx);
extern rtx        gen_lrounduv2sfv2si2                    (rtx, rtx);
extern rtx        gen_lfrintnuv2sfv2si2                   (rtx, rtx);
extern rtx        gen_lbtruncv4sfv4si2                    (rtx, rtx);
extern rtx        gen_lceilv4sfv4si2                      (rtx, rtx);
extern rtx        gen_lfloorv4sfv4si2                     (rtx, rtx);
extern rtx        gen_lroundv4sfv4si2                     (rtx, rtx);
extern rtx        gen_lfrintnv4sfv4si2                    (rtx, rtx);
extern rtx        gen_lbtruncuv4sfv4si2                   (rtx, rtx);
extern rtx        gen_lceiluv4sfv4si2                     (rtx, rtx);
extern rtx        gen_lflooruv4sfv4si2                    (rtx, rtx);
extern rtx        gen_lrounduv4sfv4si2                    (rtx, rtx);
extern rtx        gen_lfrintnuv4sfv4si2                   (rtx, rtx);
extern rtx        gen_lbtruncv2dfv2di2                    (rtx, rtx);
extern rtx        gen_lceilv2dfv2di2                      (rtx, rtx);
extern rtx        gen_lfloorv2dfv2di2                     (rtx, rtx);
extern rtx        gen_lroundv2dfv2di2                     (rtx, rtx);
extern rtx        gen_lfrintnv2dfv2di2                    (rtx, rtx);
extern rtx        gen_lbtruncuv2dfv2di2                   (rtx, rtx);
extern rtx        gen_lceiluv2dfv2di2                     (rtx, rtx);
extern rtx        gen_lflooruv2dfv2di2                    (rtx, rtx);
extern rtx        gen_lrounduv2dfv2di2                    (rtx, rtx);
extern rtx        gen_lfrintnuv2dfv2di2                   (rtx, rtx);
extern rtx        gen_lbtrunchfhi2                        (rtx, rtx);
extern rtx        gen_lceilhfhi2                          (rtx, rtx);
extern rtx        gen_lfloorhfhi2                         (rtx, rtx);
extern rtx        gen_lroundhfhi2                         (rtx, rtx);
extern rtx        gen_lfrintnhfhi2                        (rtx, rtx);
extern rtx        gen_lbtruncuhfhi2                       (rtx, rtx);
extern rtx        gen_lceiluhfhi2                         (rtx, rtx);
extern rtx        gen_lflooruhfhi2                        (rtx, rtx);
extern rtx        gen_lrounduhfhi2                        (rtx, rtx);
extern rtx        gen_lfrintnuhfhi2                       (rtx, rtx);
extern rtx        gen_fix_trunchfhi2                      (rtx, rtx);
extern rtx        gen_fixuns_trunchfhi2                   (rtx, rtx);
extern rtx        gen_floathihf2                          (rtx, rtx);
extern rtx        gen_floatunshihf2                       (rtx, rtx);
extern rtx        gen_floatv4hiv4hf2                      (rtx, rtx);
extern rtx        gen_floatunsv4hiv4hf2                   (rtx, rtx);
extern rtx        gen_floatv8hiv8hf2                      (rtx, rtx);
extern rtx        gen_floatunsv8hiv8hf2                   (rtx, rtx);
extern rtx        gen_floatv2siv2sf2                      (rtx, rtx);
extern rtx        gen_floatunsv2siv2sf2                   (rtx, rtx);
extern rtx        gen_floatv4siv4sf2                      (rtx, rtx);
extern rtx        gen_floatunsv4siv4sf2                   (rtx, rtx);
extern rtx        gen_floatv2div2df2                      (rtx, rtx);
extern rtx        gen_floatunsv2div2df2                   (rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_lo_v8hf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_lo_v4sf    (rtx, rtx, rtx);
extern rtx        gen_fcvtzsv4hf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuv4hf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzsv8hf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuv8hf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzsv2sf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuv2sf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzsv4sf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuv4sf3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzsv2df3                         (rtx, rtx, rtx);
extern rtx        gen_fcvtzuv2df3                         (rtx, rtx, rtx);
extern rtx        gen_scvtfv4hi3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfv4hi3                          (rtx, rtx, rtx);
extern rtx        gen_scvtfv8hi3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfv8hi3                          (rtx, rtx, rtx);
extern rtx        gen_scvtfv2si3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfv2si3                          (rtx, rtx, rtx);
extern rtx        gen_scvtfv4si3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfv4si3                          (rtx, rtx, rtx);
extern rtx        gen_scvtfv2di3                          (rtx, rtx, rtx);
extern rtx        gen_ucvtfv2di3                          (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_hi_v8hf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_vec_unpacks_hi_v4sf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_float_extend_lo_v2df        (rtx, rtx);
extern rtx        gen_aarch64_float_extend_lo_v4sf        (rtx, rtx);
extern rtx        gen_aarch64_float_truncate_lo_v2sf      (rtx, rtx);
extern rtx        gen_aarch64_float_truncate_lo_v4hf      (rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v4sf_le   (rtx, rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v8hf_le   (rtx, rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v4sf_be   (rtx, rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v8hf_be   (rtx, rtx, rtx);
extern rtx        gen_smaxv4hf3                           (rtx, rtx, rtx);
extern rtx        gen_sminv4hf3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv8hf3                           (rtx, rtx, rtx);
extern rtx        gen_sminv8hf3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv2sf3                           (rtx, rtx, rtx);
extern rtx        gen_sminv2sf3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv4sf3                           (rtx, rtx, rtx);
extern rtx        gen_sminv4sf3                           (rtx, rtx, rtx);
extern rtx        gen_smaxv2df3                           (rtx, rtx, rtx);
extern rtx        gen_sminv2df3                           (rtx, rtx, rtx);
extern rtx        gen_smax_nanv4hf3                       (rtx, rtx, rtx);
extern rtx        gen_smin_nanv4hf3                       (rtx, rtx, rtx);
extern rtx        gen_fmaxv4hf3                           (rtx, rtx, rtx);
extern rtx        gen_fminv4hf3                           (rtx, rtx, rtx);
extern rtx        gen_smax_nanv8hf3                       (rtx, rtx, rtx);
extern rtx        gen_smin_nanv8hf3                       (rtx, rtx, rtx);
extern rtx        gen_fmaxv8hf3                           (rtx, rtx, rtx);
extern rtx        gen_fminv8hf3                           (rtx, rtx, rtx);
extern rtx        gen_smax_nanv2sf3                       (rtx, rtx, rtx);
extern rtx        gen_smin_nanv2sf3                       (rtx, rtx, rtx);
extern rtx        gen_fmaxv2sf3                           (rtx, rtx, rtx);
extern rtx        gen_fminv2sf3                           (rtx, rtx, rtx);
extern rtx        gen_smax_nanv4sf3                       (rtx, rtx, rtx);
extern rtx        gen_smin_nanv4sf3                       (rtx, rtx, rtx);
extern rtx        gen_fmaxv4sf3                           (rtx, rtx, rtx);
extern rtx        gen_fminv4sf3                           (rtx, rtx, rtx);
extern rtx        gen_smax_nanv2df3                       (rtx, rtx, rtx);
extern rtx        gen_smin_nanv2df3                       (rtx, rtx, rtx);
extern rtx        gen_fmaxv2df3                           (rtx, rtx, rtx);
extern rtx        gen_fminv2df3                           (rtx, rtx, rtx);
extern rtx        gen_aarch64_faddpv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faddpv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faddpv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faddpv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faddpv2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv8qi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv16qi    (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv4hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv8hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv4si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv2di     (rtx, rtx);
extern rtx        gen_aarch64_reduc_plus_internalv2si     (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v2sf                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v2df                (rtx, rtx);
extern rtx        gen_clrsbv8qi2                          (rtx, rtx);
extern rtx        gen_clrsbv16qi2                         (rtx, rtx);
extern rtx        gen_clrsbv4hi2                          (rtx, rtx);
extern rtx        gen_clrsbv8hi2                          (rtx, rtx);
extern rtx        gen_clrsbv2si2                          (rtx, rtx);
extern rtx        gen_clrsbv4si2                          (rtx, rtx);
extern rtx        gen_clzv8qi2                            (rtx, rtx);
extern rtx        gen_clzv16qi2                           (rtx, rtx);
extern rtx        gen_clzv4hi2                            (rtx, rtx);
extern rtx        gen_clzv8hi2                            (rtx, rtx);
extern rtx        gen_clzv2si2                            (rtx, rtx);
extern rtx        gen_clzv4si2                            (rtx, rtx);
extern rtx        gen_popcountv8qi2                       (rtx, rtx);
extern rtx        gen_popcountv16qi2                      (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv8qi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv8qi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv8qi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv8qi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv16qi    (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv16qi    (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv16qi    (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv16qi    (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv4hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv4hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv4hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv4hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv8hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv8hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv8hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv8hi     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv4si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv4si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv4si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv4si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umax_internalv2si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_umin_internalv2si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv2si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv2si     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_nan_internalv4hf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_nan_internalv4hf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv4hf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv4hf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_nan_internalv8hf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_nan_internalv8hf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv8hf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv8hf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_nan_internalv2sf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_nan_internalv2sf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv2sf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv2sf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_nan_internalv4sf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_nan_internalv4sf (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv4sf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv4sf     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_nan_internalv2df (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_nan_internalv2df (rtx, rtx);
extern rtx        gen_aarch64_reduc_smax_internalv2df     (rtx, rtx);
extern rtx        gen_aarch64_reduc_smin_internalv2df     (rtx, rtx);
extern rtx        gen_aarch64_simd_bslv8qi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv16qi_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv8hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2si_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4si_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2di_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bsldi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bsldi_alt              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev4hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev8hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev2sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev4sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_lanev2df                (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesv8qi                 (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesv4hi                 (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesv4hf                 (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesv2si                 (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesv2sf                 (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesdi                   (rtx, rtx, rtx);
extern rtx        gen_load_pair_lanesdf                   (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesv8qi                (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesv4hi                (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesv4hf                (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesv2si                (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesv2sf                (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesdi                  (rtx, rtx, rtx);
extern rtx        gen_store_pair_lanesdf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv16qi_hi_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv16qi_hi_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv16qi_hi_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv16qi_hi_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv8hi_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv8hi_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv8hi_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv8hi_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv4si_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv4si_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv4si_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv4si_hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv16qi_lo_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv16qi_lo_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv16qi_lo_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv16qi_lo_internal      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv8hi_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv8hi_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv8hi_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv8hi_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv4si_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv4si_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv4si_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv4si_lo_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssublv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usublv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv16qi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv16qi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv16qi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv16qi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv8hi_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv8hi_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv8hi_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv8hi_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddwv4si_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubwv4si_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddwv4si_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubwv4si_internal          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v16qi_internal        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v16qi_internal        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v16qi_internal        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v16qi_internal        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v8hi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v8hi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v8hi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v8hi_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v4si_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v4si_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v4si_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v4si_internal         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shaddv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhaddv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhaddv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhaddv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_shsubv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uhsubv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srhsubv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urhsubv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_addhnv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhnv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_subhnv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhnv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_addhnv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhnv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_subhnv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhnv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_addhnv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhnv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_subhnv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhnv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_addhn2v8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhn2v8hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_subhn2v8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhn2v8hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_addhn2v4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhn2v4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_subhn2v4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhn2v4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_addhn2v2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_raddhn2v2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_subhn2v2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rsubhn2v2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_pmulv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_pmulv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxv2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxhf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxsf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmulxdf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqaddsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqaddsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqadddi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqadddi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqsubdi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqsubdi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddqi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddqi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddhi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddhi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqaddsi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqaddsi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_suqadddi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_usqadddi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqmovunv8hi                 (rtx, rtx);
extern rtx        gen_aarch64_sqmovunv4si                 (rtx, rtx);
extern rtx        gen_aarch64_sqmovunv2di                 (rtx, rtx);
extern rtx        gen_aarch64_sqmovunhi                   (rtx, rtx);
extern rtx        gen_aarch64_sqmovunsi                   (rtx, rtx);
extern rtx        gen_aarch64_sqmovundi                   (rtx, rtx);
extern rtx        gen_aarch64_sqmovnv8hi                  (rtx, rtx);
extern rtx        gen_aarch64_uqmovnv8hi                  (rtx, rtx);
extern rtx        gen_aarch64_sqmovnv4si                  (rtx, rtx);
extern rtx        gen_aarch64_uqmovnv4si                  (rtx, rtx);
extern rtx        gen_aarch64_sqmovnv2di                  (rtx, rtx);
extern rtx        gen_aarch64_uqmovnv2di                  (rtx, rtx);
extern rtx        gen_aarch64_sqmovnhi                    (rtx, rtx);
extern rtx        gen_aarch64_uqmovnhi                    (rtx, rtx);
extern rtx        gen_aarch64_sqmovnsi                    (rtx, rtx);
extern rtx        gen_aarch64_uqmovnsi                    (rtx, rtx);
extern rtx        gen_aarch64_sqmovndi                    (rtx, rtx);
extern rtx        gen_aarch64_uqmovndi                    (rtx, rtx);
extern rtx        gen_aarch64_sqnegv8qi                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv8qi                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegv16qi                  (rtx, rtx);
extern rtx        gen_aarch64_sqabsv16qi                  (rtx, rtx);
extern rtx        gen_aarch64_sqnegv4hi                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv4hi                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegv8hi                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv8hi                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegv2si                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv2si                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegv4si                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv4si                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegv2di                   (rtx, rtx);
extern rtx        gen_aarch64_sqabsv2di                   (rtx, rtx);
extern rtx        gen_aarch64_sqnegqi                     (rtx, rtx);
extern rtx        gen_aarch64_sqabsqi                     (rtx, rtx);
extern rtx        gen_aarch64_sqneghi                     (rtx, rtx);
extern rtx        gen_aarch64_sqabshi                     (rtx, rtx);
extern rtx        gen_aarch64_sqnegsi                     (rtx, rtx);
extern rtx        gen_aarch64_sqabssi                     (rtx, rtx);
extern rtx        gen_aarch64_sqnegdi                     (rtx, rtx);
extern rtx        gen_aarch64_sqabsdi                     (rtx, rtx);
extern rtx        gen_aarch64_sqdmulhv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhv4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulhv8hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulhv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhv2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulhv4si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulhhi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhhi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulhsi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulhsi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanev4hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanev4hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanev8hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanev8hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanev2si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanev2si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanev4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanev4si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqv4hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqv4hi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqv8hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqv8hi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqv2si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqv2si          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqv4si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqv4si          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanehi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanehi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_lanesi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_lanesi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqhi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqhi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmulh_laneqsi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmulh_laneqsi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahv4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshv4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahv8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshv8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahv2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshv2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahv4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshv4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlahsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlshsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanev4hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanev4hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanev8hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanev8hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanev2si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanev2si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanev4si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanev4si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanehi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanehi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_lanesi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_lanesi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqv4hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqv4hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqv8hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqv8hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqv2si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqv2si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqv4si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqv4si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqhi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqhi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlah_laneqsi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrdmlsh_laneqsi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlalv4hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlslv4hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlalv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlslv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlalhi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlslhi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlalsi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlslsi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_lanev4hi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_lanev4hi            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_lanev2si            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_lanev2si            (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_laneqv4hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_laneqv4hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_laneqv2si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_laneqv2si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_lanehi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_lanehi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_lanesi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_lanesi              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_laneqhi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_laneqhi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_laneqsi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_laneqsi             (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_nv4hi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_nv4hi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal_nv2si               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl_nv2si               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2v8hi_internal       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2v8hi_internal       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2v4si_internal       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2v4si_internal       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_lanev8hi_internal  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_lanev8hi_internal  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_lanev4si_internal  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_lanev4si_internal  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_laneqv8hi_internal (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_laneqv8hi_internal (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_laneqv4si_internal (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_laneqv4si_internal (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_nv8hi_internal     (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_nv8hi_internal     (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_nv4si_internal     (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_nv4si_internal     (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmullv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmullv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmullhi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmullsi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_lanev4hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_lanev2si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_laneqv4hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_laneqv2si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_lanehi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_lanesi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_laneqhi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_laneqsi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_nv4hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull_nv2si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2v8hi_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2v4si_internal       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_lanev8hi_internal  (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_lanev4si_internal  (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_laneqv8hi_internal (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_laneqv4si_internal (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_nv8hi_internal     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_nv4si_internal     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshlv2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushlv2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshlv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshlv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshldi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushldi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshldi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshldi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv8qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv4hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv2si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlv2di                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlqi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlqi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlqi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlhi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlhi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlhi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshlsi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshlsi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshlsi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshldi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshldi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshldi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshldi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll2_nv16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll2_nv16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll2_nv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll2_nv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sshll2_nv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_ushll2_nv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv16qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv16qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv8hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv8hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv4si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv4si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_nv2di                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_nv2di                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_srshr_ndi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_urshr_ndi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv8qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv8qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv16qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv16qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv4hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv4hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv8hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv8hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_nv2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_nv2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssra_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usra_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_srsra_ndi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ursra_ndi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv8qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv16qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv4hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv8hi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv2si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv4si                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_nv2di                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssli_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usli_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ssri_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_usri_ndi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv16qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv16qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv8hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv8hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv4si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv4si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nv2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nv2di                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nv2di                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nqi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nqi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nqi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nhi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nhi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nhi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_nsi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_nsi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_nsi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshlu_ndi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshl_ndi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshl_ndi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_nv8hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_nv8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_nv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_nv8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_nv8hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_nv8hi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_nv4si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_nv4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_nv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_nv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_nv4si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_nv4si               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_nv2di               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_nv2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_nv2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_nv2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_nv2di               (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_nv2di               (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_nhi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_nhi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_nhi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_nhi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_nhi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_nhi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_nsi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_nsi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_nsi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_nsi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_nsi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_nsi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrun_ndi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrun_ndi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqshrn_ndi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqshrn_ndi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqrshrn_ndi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uqrshrn_ndi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltdi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmledi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqdi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgedi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtdi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv4hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv8hi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv2si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltuv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleuv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeuv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtuv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltudi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmleudi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgeudi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtudi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv8qi                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv16qi                  (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv4hi                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv8hi                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv2si                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv4si                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstv2di                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_cmtstdi                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltv2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlev2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqv2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgev2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtv2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlthf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlehf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqhf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgehf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgthf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltsf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmlesf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqsf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgesf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtsf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmltdf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmledf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmeqdf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgedf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_cmgtdf                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclev4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgev4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtv4hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclev8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgev8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtv8hf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclev2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgev2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtv2sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclev4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgev4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtv4sf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltv2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclev2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgev2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtv2df                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclthf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclehf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgehf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgthf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltsf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_faclesf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgesf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtsf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facltdf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facledf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgedf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_facgtdf                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_addpv8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_addpv4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_addpv2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_addpdi                      (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2v2df                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv8qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv16qi              (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv4hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv8hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv2si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv4si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv2di               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv4hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv8hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv2sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv4sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rv2df               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rdi                 (rtx, rtx);
extern rtx        gen_aarch64_simd_ld2rdf                 (rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev8qi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev16qi  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev4hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev8hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev2si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev4si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev2di   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev4hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev8hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev2sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev4sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanev2df   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanedi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesoi_lanedf     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_st2v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st2v2df                (rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev8qi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev16qi (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev4hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev8hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev2si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev4si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev2di  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev4hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev8hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev2sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev4sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanev2df  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanedi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesoi_lanedf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3v2df                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv8qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv16qi              (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv4hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv8hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv2si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv4si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv2di               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv4hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv8hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv2sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv4sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rv2df               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rdi                 (rtx, rtx);
extern rtx        gen_aarch64_simd_ld3rdf                 (rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev8qi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev16qi  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev4hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev8hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev2si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev4si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev2di   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev4hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev8hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev2sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev4sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanev2df   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanedi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesci_lanedf     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_st3v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st3v2df                (rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev8qi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev16qi (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev4hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev8hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev2si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev4si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev2di  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev4hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev8hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev2sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev4sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanev2df  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanedi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesci_lanedf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4v2df                (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv8qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv16qi              (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv4hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv8hi               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv2si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv4si               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv2di               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv4hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv8hf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv2sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv4sf               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rv2df               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rdi                 (rtx, rtx);
extern rtx        gen_aarch64_simd_ld4rdf                 (rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev8qi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev16qi  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev4hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev8hi   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev2si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev4si   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev2di   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev4hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev8hf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev2sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev4sf   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanev2df   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanedi     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_load_lanesxi_lanedf     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_st4v16qi               (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v8hi                (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v2di                (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v8hf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v4sf                (rtx, rtx);
extern rtx        gen_aarch64_simd_st4v2df                (rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev8qi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev16qi (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev4hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev8hi  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev2si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev4si  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev2di  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev4hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev8hf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev2sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev4sf  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanev2df  (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanedi    (rtx, rtx, rtx);
extern rtx        gen_aarch64_vec_store_lanesxi_lanedf    (rtx, rtx, rtx);
extern rtx        gen_aarch64_rev_reglistoi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_rev_reglistci               (rtx, rtx, rtx);
extern rtx        gen_aarch64_rev_reglistxi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_be_ld1v8qi                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v16qi                 (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v4hi                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v8hi                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v2si                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v4si                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v2di                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v4hf                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v8hf                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v2sf                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v4sf                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1v2df                  (rtx, rtx);
extern rtx        gen_aarch64_be_ld1di                    (rtx, rtx);
extern rtx        gen_aarch64_be_st1v8qi                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v16qi                 (rtx, rtx);
extern rtx        gen_aarch64_be_st1v4hi                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v8hi                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v2si                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v4si                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v2di                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v4hf                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v8hf                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v2sf                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v4sf                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1v2df                  (rtx, rtx);
extern rtx        gen_aarch64_be_st1di                    (rtx, rtx);
extern rtx        gen_aarch64_ld2v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld2v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld2v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld2v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld2v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld2di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_ld2df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_ld3v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld3v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld3v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld3v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld3v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld3di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_ld3df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_ld4v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld4v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld4v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld4v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld4v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_ld4di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_ld4df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_tbl1v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_tbl1v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_tbl2v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_tbl3v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_tbl3v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_tbx4v8qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_tbx4v16qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbl3v8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbl3v16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbx3v8qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbx3v16qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbl4v8qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbl4v16qi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbx4v8qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_qtbx4v16qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev16qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v8qi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v16qi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v4hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v8hi                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v2si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v4si                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v2di                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v4hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v8hf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v2sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v4sf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip1v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_zip2v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn1v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_trn2v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp1v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_uzp2v2df                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_extv8qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv4hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv2si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv4hf                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv8hf                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv2sf                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv4sf                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_extv2df                     (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rev64v8qi                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v8qi                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v8qi                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v16qi                  (rtx, rtx);
extern rtx        gen_aarch64_rev32v16qi                  (rtx, rtx);
extern rtx        gen_aarch64_rev16v16qi                  (rtx, rtx);
extern rtx        gen_aarch64_rev64v4hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v4hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v4hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v8hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v8hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v8hi                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v2si                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v2si                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v2si                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v4si                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v4si                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v4si                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v2di                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v2di                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v2di                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v4hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v4hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v4hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v8hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v8hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v8hf                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v2sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v2sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v2sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v4sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v4sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v4sf                   (rtx, rtx);
extern rtx        gen_aarch64_rev64v2df                   (rtx, rtx);
extern rtx        gen_aarch64_rev32v2df                   (rtx, rtx);
extern rtx        gen_aarch64_rev16v2df                   (rtx, rtx);
extern rtx        gen_aarch64_st2v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st2v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st2v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st2v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st2v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st2di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_st2df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_st3v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st3v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st3v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st3v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st3v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st3di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_st3df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_st4v8qi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st4v4hi_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st4v4hf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st4v2si_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st4v2sf_dreg                (rtx, rtx);
extern rtx        gen_aarch64_st4di_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_st4df_dreg                  (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v16qi_x2            (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v8hi_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v4si_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v2di_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v8hf_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v4sf_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v2df_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v8qi_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v4hi_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v4hf_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v2si_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1v2sf_x2             (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1di_x2               (rtx, rtx);
extern rtx        gen_aarch64_simd_ld1df_x2               (rtx, rtx);
extern rtx        gen_aarch64_frecpev4hf                  (rtx, rtx);
extern rtx        gen_aarch64_frecpev8hf                  (rtx, rtx);
extern rtx        gen_aarch64_frecpev2sf                  (rtx, rtx);
extern rtx        gen_aarch64_frecpev4sf                  (rtx, rtx);
extern rtx        gen_aarch64_frecpev2df                  (rtx, rtx);
extern rtx        gen_aarch64_frecpehf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpxhf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpesf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpxsf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpedf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpxdf                    (rtx, rtx);
extern rtx        gen_aarch64_frecpsv4hf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpsv8hf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpsv2sf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpsv4sf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpsv2df                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpshf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpssf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_frecpsdf                    (rtx, rtx, rtx);
extern rtx        gen_aarch64_urecpev2si                  (rtx, rtx);
extern rtx        gen_aarch64_urecpev4si                  (rtx, rtx);
extern rtx        gen_aarch64_crypto_aesev16qi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_aesdv16qi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_aesmcv16qi           (rtx, rtx);
extern rtx        gen_aarch64_crypto_aesimcv16qi          (rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1hsi              (rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1hv4si            (rtx, rtx);
extern rtx        gen_aarch64_be_crypto_sha1hv4si         (rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1su1v4si          (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1cv4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1mv4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1pv4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha1su0v4si          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha256hv4si          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha256h2v4si         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha256su0v4si        (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha256su1v4si        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha512hqv2di         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha512h2qv2di        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha512su0qv2di       (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_sha512su1qv2di       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_eor3qv8hi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_rax1qv2di                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_xarqv2di                    (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_bcaxqv8hi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3ss1qv4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3tt1aqv4si                (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3tt1bqv4si                (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3tt2aqv4si                (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3tt2bqv4si                (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3partw1qv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm3partw2qv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sm4eqv4si                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sm4ekeyqv4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_lowv2sf          (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_lowv4sf         (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_lowv2sf          (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_lowv4sf         (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_highv2sf         (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_highv4sf        (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_highv2sf         (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_highv4sf        (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_lane_lowv2sf     (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_lane_lowv2sf     (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_lane_highv2sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_lane_highv2sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_laneq_lowv4sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_laneq_lowv4sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_laneq_highv4sf  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_laneq_highv4sf  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_laneq_lowv2sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_laneq_lowv2sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlal_laneq_highv2sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlsl_laneq_highv2sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_lane_lowv4sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_lane_lowv4sf    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlalq_lane_highv4sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_fmlslq_lane_highv4sf   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_pmulldi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_crypto_pmullv2di            (rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapqi          (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swaphi          (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapsi          (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapdi          (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapqi_lse      (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swaphi_lse      (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapsi_lse      (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_compare_and_swapdi_lse      (rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangeqi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangehi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangesi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangedi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangeqi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangehi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangesi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_exchangedi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addqi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subqi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orqi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorqi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andqi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addhi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subhi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orhi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorhi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andhi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addsi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subsi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orsi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorsi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andsi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_adddi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subdi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_ordi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xordi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_anddi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addqi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subqi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orqi_lse             (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorqi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andqi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addhi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subhi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orhi_lse             (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorhi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andhi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_addsi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subsi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_orsi_lse             (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xorsi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_andsi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_adddi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_subdi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_ordi_lse             (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xordi_lse            (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_anddi_lse            (rtx, rtx, rtx);
extern rtx        gen_atomic_nandqi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_nandhi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_nandsi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_nanddi                       (rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orqi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orhi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orsi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_adddi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subdi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_ordi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xordi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_anddi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orqi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orhi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_addsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_orsi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xorsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_andsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_adddi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_subdi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_ordi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_xordi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_fetch_anddi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_nandqi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_nandhi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_nandsi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_nanddi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchqi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchqi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchhi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchhi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchsi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchsi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchdi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchdi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchdi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchdi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchdi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchqi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchqi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchhi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchhi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchsi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchsi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_add_fetchdi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_sub_fetchdi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_or_fetchdi_lse       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_xor_fetchdi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_and_fetchdi_lse      (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_nand_fetchqi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_nand_fetchhi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_nand_fetchsi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_nand_fetchdi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_loadqi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_loadhi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_loadsi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_loaddi                       (rtx, rtx, rtx);
extern rtx        gen_atomic_storeqi                      (rtx, rtx, rtx);
extern rtx        gen_atomic_storehi                      (rtx, rtx, rtx);
extern rtx        gen_atomic_storesi                      (rtx, rtx, rtx);
extern rtx        gen_atomic_storedi                      (rtx, rtx, rtx);
extern rtx        gen_aarch64_load_exclusiveqi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_load_exclusivehi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_load_exclusivesi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_load_exclusivedi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_store_exclusiveqi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_store_exclusivehi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_store_exclusivesi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_store_exclusivedi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_swpqi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_swphi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_swpsi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_swpdi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_casqi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_cashi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_cassi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_casdi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadsetqi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadclrqi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadeorqi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadaddqi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadsethi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadclrhi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadeorhi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadaddhi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadsetsi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadclrsi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadeorsi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadaddsi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadsetdi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadclrdi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadeordi            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_atomic_loadadddi            (rtx, rtx, rtx, rtx);
extern rtx        gen_maskloadvnx16qivnx16bi              (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx8hivnx8bi                (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx4sivnx4bi                (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx2divnx2bi                (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx8hfvnx8bi                (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx4sfvnx4bi                (rtx, rtx, rtx);
extern rtx        gen_maskloadvnx2dfvnx2bi                (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx16qivnx16bi             (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx8hivnx8bi               (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx4sivnx4bi               (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx2divnx2bi               (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx8hfvnx8bi               (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx4sfvnx4bi               (rtx, rtx, rtx);
extern rtx        gen_maskstorevnx2dfvnx2bi               (rtx, rtx, rtx);
extern rtx        gen_mask_gather_loadvnx4si              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_gather_loadvnx4sf              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_gather_loadvnx2di              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_gather_loadvnx2df              (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_scatter_storevnx4si            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_scatter_storevnx4sf            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_scatter_storevnx2di            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_mask_scatter_storevnx2df            (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_pred_movvnx32qi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx16hi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx8si                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx4di                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx16hf                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx8sf                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx4df                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx48qi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx24hi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx12si                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx6di                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx24hf                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx12sf                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx6df                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx64qi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx32hi                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx16si                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx8di                      (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx32hf                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx16sf                     (rtx, rtx, rtx);
extern rtx        gen_pred_movvnx8df                      (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx16qi                (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx8hi                 (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx4si                 (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx2di                 (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx8hf                 (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx4sf                 (rtx, rtx, rtx);
extern rtx        gen_extract_last_vnx2df                 (rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx8hf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx4sf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_sve_ld1rvnx2df                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_seriesvnx16qi                   (rtx, rtx, rtx);
extern rtx        gen_vec_seriesvnx8hi                    (rtx, rtx, rtx);
extern rtx        gen_vec_seriesvnx4si                    (rtx, rtx, rtx);
extern rtx        gen_vec_seriesvnx2di                    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx32qivnx16qi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx16hivnx8hi    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx8sivnx4si     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx4divnx2di     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx16hfvnx8hf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx8sfvnx4sf     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx4dfvnx2df     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx48qivnx16qi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx24hivnx8hi    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx12sivnx4si    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx6divnx2di     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx24hfvnx8hf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx12sfvnx4sf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx6dfvnx2df     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx64qivnx16qi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx32hivnx8hi    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx16sivnx4si    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx8divnx2di     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx32hfvnx8hf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx16sfvnx4sf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_load_lanesvnx8dfvnx2df     (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx32qivnx16qi  (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx16hivnx8hi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx8sivnx4si    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx4divnx2di    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx16hfvnx8hf   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx8sfvnx4sf    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx4dfvnx2df    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx48qivnx16qi  (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx24hivnx8hi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx12sivnx4si   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx6divnx2di    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx24hfvnx8hf   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx12sfvnx4sf   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx6dfvnx2df    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx64qivnx16qi  (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx32hivnx8hi   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx16sivnx4si   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx8divnx2di    (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx32hfvnx8hf   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx16sfvnx4sf   (rtx, rtx, rtx);
extern rtx        gen_vec_mask_store_lanesvnx8dfvnx2df    (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip1vnx2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_zip2vnx2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn1vnx2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_trn2vnx2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp1vnx2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_uzp2vnx2df              (rtx, rtx, rtx);
extern rtx        gen_addvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_addvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_addvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_addvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_subvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_iorvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_xorvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_andvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_iorvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_iorvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_iorvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_bicvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_bicvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_bicvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_bicvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx16bi3                         (rtx, rtx, rtx);
extern rtx        gen_andvnx8bi3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx4bi3                          (rtx, rtx, rtx);
extern rtx        gen_andvnx2bi3                          (rtx, rtx, rtx);
extern rtx        gen_pred_andvnx16bi3                    (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_iorvnx16bi3                    (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_xorvnx16bi3                    (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_andvnx8bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_iorvnx8bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_xorvnx8bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_andvnx4bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_iorvnx4bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_xorvnx4bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_andvnx2bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_iorvnx2bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_pred_xorvnx2bi3                     (rtx, rtx, rtx, rtx);
extern rtx        gen_ptest_ptruevnx16bi                  (rtx, rtx);
extern rtx        gen_ptest_ptruevnx8bi                   (rtx, rtx);
extern rtx        gen_ptest_ptruevnx4bi                   (rtx, rtx);
extern rtx        gen_ptest_ptruevnx2bi                   (rtx, rtx);
extern rtx        gen_while_ultsivnx16bi                  (rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx16bi                  (rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx8bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx8bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx4bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx4bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx2bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx2bi                   (rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx16bi_cc               (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx16bi_cc               (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx8bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx8bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx4bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx4bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultsivnx2bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_while_ultdivnx2bi_cc                (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx16qivnx16bi           (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx8hivnx8bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx4sivnx4bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx2divnx2bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx8hfvnx8bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx4sfvnx4bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_vnx2dfvnx2bi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_dupvnx16qi_const        (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_dupvnx8hi_const         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_dupvnx4si_const         (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_dupvnx2di_const         (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_smaxvnx16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_umaxvnx16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_sminvnx16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_uminvnx16qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_andvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_iorvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_xorvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_smaxvnx8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_umaxvnx8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_sminvnx8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_uminvnx8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_andvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_iorvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_xorvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_smaxvnx4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_umaxvnx4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_sminvnx4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_uminvnx4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_andvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_iorvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_xorvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_smaxvnx2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_umaxvnx2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_sminvnx2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_uminvnx2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_andvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_iorvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_xorvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx16qi           (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx8hi            (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx4si            (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx2di            (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx8hf            (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx4sf            (rtx, rtx, rtx, rtx);
extern rtx        gen_fold_extract_last_vnx2df            (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_floatvnx4sivnx2df2      (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_floatunsvnx4sivnx2df2   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_floatvnx2divnx2df2      (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_floatunsvnx2divnx2df2   (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_extendvnx8hfvnx4sf2     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_extendvnx4sfvnx2df2     (rtx, rtx, rtx);
extern rtx        gen_aarch64_sve_punpklo_vnx16bi         (rtx, rtx);
extern rtx        gen_aarch64_sve_punpkhi_vnx16bi         (rtx, rtx);
extern rtx        gen_aarch64_sve_punpklo_vnx8bi          (rtx, rtx);
extern rtx        gen_aarch64_sve_punpkhi_vnx8bi          (rtx, rtx);
extern rtx        gen_aarch64_sve_punpklo_vnx4bi          (rtx, rtx);
extern rtx        gen_aarch64_sve_punpkhi_vnx4bi          (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpkhi_vnx16qi         (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpkhi_vnx16qi         (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpklo_vnx16qi         (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpklo_vnx16qi         (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpkhi_vnx8hi          (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpkhi_vnx8hi          (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpklo_vnx8hi          (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpklo_vnx8hi          (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpkhi_vnx4si          (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpkhi_vnx4si          (rtx, rtx);
extern rtx        gen_aarch64_sve_sunpklo_vnx4si          (rtx, rtx);
extern rtx        gen_aarch64_sve_uunpklo_vnx4si          (rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx8bi               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx4bi               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx2bi               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx8hi               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx4si               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx2di               (rtx, rtx, rtx);
extern rtx        gen_cond_addvnx8hf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx8hf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx4sf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx4sf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_addvnx2df                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cond_subvnx2df                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx16qi              (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx8hi               (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx4si               (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx2di               (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx8hf               (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx4sf               (rtx, rtx, rtx);
extern rtx        gen_vec_shl_insert_vnx2df               (rtx, rtx, rtx);
extern rtx        gen_cbranchsi4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchdi4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchsf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchdf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchcc4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_modsi3                              (rtx, rtx, rtx);
extern rtx        gen_moddi3                              (rtx, rtx, rtx);
extern rtx        gen_casesi                              (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_prologue                            (void);
extern rtx        gen_epilogue                            (void);
extern rtx        gen_sibcall_epilogue                    (void);
extern rtx        gen_return                              (void);
extern rtx        gen_call                                (rtx, rtx, rtx);
extern rtx        gen_call_value                          (rtx, rtx, rtx, rtx);
extern rtx        gen_sibcall                             (rtx, rtx, rtx);
extern rtx        gen_sibcall_value                       (rtx, rtx, rtx, rtx);
extern rtx        gen_untyped_call                        (rtx, rtx, rtx);
extern rtx        gen_movqi                               (rtx, rtx);
extern rtx        gen_movhi                               (rtx, rtx);
extern rtx        gen_movsi                               (rtx, rtx);
extern rtx        gen_movdi                               (rtx, rtx);
extern rtx        gen_movti                               (rtx, rtx);
extern rtx        gen_movhf                               (rtx, rtx);
extern rtx        gen_movsf                               (rtx, rtx);
extern rtx        gen_movdf                               (rtx, rtx);
extern rtx        gen_movtf                               (rtx, rtx);
extern rtx        gen_movmemdi                            (rtx, rtx, rtx, rtx);
extern rtx        gen_extendsidi2                         (rtx, rtx);
extern rtx        gen_zero_extendsidi2                    (rtx, rtx);
extern rtx        gen_extendqisi2                         (rtx, rtx);
extern rtx        gen_zero_extendqisi2                    (rtx, rtx);
extern rtx        gen_extendqidi2                         (rtx, rtx);
extern rtx        gen_zero_extendqidi2                    (rtx, rtx);
extern rtx        gen_extendhisi2                         (rtx, rtx);
extern rtx        gen_zero_extendhisi2                    (rtx, rtx);
extern rtx        gen_extendhidi2                         (rtx, rtx);
extern rtx        gen_zero_extendhidi2                    (rtx, rtx);
extern rtx        gen_extendqihi2                         (rtx, rtx);
extern rtx        gen_zero_extendqihi2                    (rtx, rtx);
extern rtx        gen_addsi3                              (rtx, rtx, rtx);
extern rtx        gen_adddi3                              (rtx, rtx, rtx);
extern rtx        gen_addti3                              (rtx, rtx, rtx);
extern rtx        gen_addsi3_carryin                      (rtx, rtx, rtx);
extern rtx        gen_adddi3_carryin                      (rtx, rtx, rtx);
extern rtx        gen_subti3                              (rtx, rtx, rtx);
extern rtx        gen_subsi3_carryin                      (rtx, rtx, rtx);
extern rtx        gen_subdi3_carryin                      (rtx, rtx, rtx);
extern rtx        gen_abssi2                              (rtx, rtx);
extern rtx        gen_absdi2                              (rtx, rtx);
extern rtx        gen_mulditi3                            (rtx, rtx, rtx);
extern rtx        gen_umulditi3                           (rtx, rtx, rtx);
extern rtx        gen_multi3                              (rtx, rtx, rtx);
extern rtx        gen_cstoresi4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_cstoredi4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_cstorecc4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_cstoresf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_cstoredf4                           (rtx, rtx, rtx, rtx);
extern rtx        gen_cmovsi6                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_cmovdi6                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_cmovsf6                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_cmovdf6                             (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_movqicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_movhicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_movsicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_movdicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_movsfsicc                           (rtx, rtx, rtx, rtx);
extern rtx        gen_movsfdicc                           (rtx, rtx, rtx, rtx);
extern rtx        gen_movdfsicc                           (rtx, rtx, rtx, rtx);
extern rtx        gen_movdfdicc                           (rtx, rtx, rtx, rtx);
extern rtx        gen_movsfcc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_movdfcc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_negsicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_notsicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_negdicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_notdicc                             (rtx, rtx, rtx, rtx);
extern rtx        gen_umaxsi3                             (rtx, rtx, rtx);
extern rtx        gen_umaxdi3                             (rtx, rtx, rtx);
extern rtx        gen_ffssi2                              (rtx, rtx);
extern rtx        gen_ffsdi2                              (rtx, rtx);
extern rtx        gen_popcountsi2                         (rtx, rtx);
extern rtx        gen_popcountdi2                         (rtx, rtx);
extern rtx        gen_ashlsi3                             (rtx, rtx, rtx);
extern rtx        gen_ashrsi3                             (rtx, rtx, rtx);
extern rtx        gen_lshrsi3                             (rtx, rtx, rtx);
extern rtx        gen_ashldi3                             (rtx, rtx, rtx);
extern rtx        gen_ashrdi3                             (rtx, rtx, rtx);
extern rtx        gen_lshrdi3                             (rtx, rtx, rtx);
extern rtx        gen_ashlqi3                             (rtx, rtx, rtx);
extern rtx        gen_ashlhi3                             (rtx, rtx, rtx);
extern rtx        gen_rotrsi3                             (rtx, rtx, rtx);
extern rtx        gen_rotrdi3                             (rtx, rtx, rtx);
extern rtx        gen_rotlsi3                             (rtx, rtx, rtx);
extern rtx        gen_rotldi3                             (rtx, rtx, rtx);
extern rtx        gen_extv                                (rtx, rtx, rtx, rtx);
extern rtx        gen_extzv                               (rtx, rtx, rtx, rtx);
extern rtx        gen_insvsi                              (rtx, rtx, rtx, rtx);
extern rtx        gen_insvdi                              (rtx, rtx, rtx, rtx);
extern rtx        gen_floatsihf2                          (rtx, rtx);
extern rtx        gen_floatunssihf2                       (rtx, rtx);
extern rtx        gen_floatdihf2                          (rtx, rtx);
extern rtx        gen_floatunsdihf2                       (rtx, rtx);
extern rtx        gen_divhf3                              (rtx, rtx, rtx);
extern rtx        gen_divsf3                              (rtx, rtx, rtx);
extern rtx        gen_divdf3                              (rtx, rtx, rtx);
extern rtx        gen_sqrthf2                             (rtx, rtx);
extern rtx        gen_sqrtsf2                             (rtx, rtx);
extern rtx        gen_sqrtdf2                             (rtx, rtx);
extern rtx        gen_lrintsfsi2                          (rtx, rtx);
extern rtx        gen_lrintsfdi2                          (rtx, rtx);
extern rtx        gen_lrintdfsi2                          (rtx, rtx);
extern rtx        gen_lrintdfdi2                          (rtx, rtx);
extern rtx        gen_copysigndf3                         (rtx, rtx, rtx);
extern rtx        gen_copysignsf3                         (rtx, rtx, rtx);
extern rtx        gen_xorsignsf3                          (rtx, rtx, rtx);
extern rtx        gen_xorsigndf3                          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpsfsi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpsfdi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpdfsi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpdfdi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcptfsi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcptfdi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv8qisi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv16qisi         (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4hisi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv8hisi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2sisi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4sisi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2disi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2sfsi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4sfsi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2dfsi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv8qidi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv16qidi         (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4hidi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv8hidi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2sidi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4sidi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2didi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2sfdi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv4sfdi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movcpv2dfdi          (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movti                (rtx, rtx, rtx);
extern rtx        gen_aarch64_reload_movtf                (rtx, rtx, rtx);
extern rtx        gen_add_losym                           (rtx, rtx, rtx);
extern rtx        gen_tlsgd_small_si                      (rtx, rtx);
extern rtx        gen_tlsgd_small_di                      (rtx, rtx);
extern rtx        gen_tlsdesc_small_si                    (rtx);
extern rtx        gen_tlsdesc_small_di                    (rtx);
extern rtx        gen_get_thread_pointerdi                (rtx);
extern rtx        gen_stack_protect_set                   (rtx, rtx);
extern rtx        gen_stack_protect_test                  (rtx, rtx, rtx);
extern rtx        gen_doloop_end                          (rtx, rtx);
extern rtx        gen_set_clobber_cc                      (rtx, rtx);
extern rtx        gen_despeculate_copyqi                  (rtx, rtx, rtx);
extern rtx        gen_despeculate_copyhi                  (rtx, rtx, rtx);
extern rtx        gen_despeculate_copysi                  (rtx, rtx, rtx);
extern rtx        gen_despeculate_copydi                  (rtx, rtx, rtx);
extern rtx        gen_despeculate_copyti                  (rtx, rtx, rtx);
extern rtx        gen_movv8qi                             (rtx, rtx);
extern rtx        gen_movv16qi                            (rtx, rtx);
extern rtx        gen_movv4hi                             (rtx, rtx);
extern rtx        gen_movv8hi                             (rtx, rtx);
extern rtx        gen_movv2si                             (rtx, rtx);
extern rtx        gen_movv4si                             (rtx, rtx);
extern rtx        gen_movv2di                             (rtx, rtx);
extern rtx        gen_movv4hf                             (rtx, rtx);
extern rtx        gen_movv8hf                             (rtx, rtx);
extern rtx        gen_movv2sf                             (rtx, rtx);
extern rtx        gen_movv4sf                             (rtx, rtx);
extern rtx        gen_movv2df                             (rtx, rtx);
extern rtx        gen_movmisalignv8qi                     (rtx, rtx);
extern rtx        gen_movmisalignv16qi                    (rtx, rtx);
extern rtx        gen_movmisalignv4hi                     (rtx, rtx);
extern rtx        gen_movmisalignv8hi                     (rtx, rtx);
extern rtx        gen_movmisalignv2si                     (rtx, rtx);
extern rtx        gen_movmisalignv4si                     (rtx, rtx);
extern rtx        gen_movmisalignv2di                     (rtx, rtx);
extern rtx        gen_movmisalignv2sf                     (rtx, rtx);
extern rtx        gen_movmisalignv4sf                     (rtx, rtx);
extern rtx        gen_movmisalignv2df                     (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv16qi         (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv8hi          (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv4si          (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv2di          (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv8hf          (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv4sf          (rtx, rtx);
extern rtx        gen_aarch64_split_simd_movv2df          (rtx, rtx);
extern rtx        gen_ctzv2si2                            (rtx, rtx);
extern rtx        gen_ctzv4si2                            (rtx, rtx);
extern rtx        gen_xorsignv4hf3                        (rtx, rtx, rtx);
extern rtx        gen_xorsignv8hf3                        (rtx, rtx, rtx);
extern rtx        gen_xorsignv2sf3                        (rtx, rtx, rtx);
extern rtx        gen_xorsignv4sf3                        (rtx, rtx, rtx);
extern rtx        gen_xorsignv2df3                        (rtx, rtx, rtx);
extern rtx        gen_sdot_prodv8qi                       (rtx, rtx, rtx, rtx);
extern rtx        gen_udot_prodv8qi                       (rtx, rtx, rtx, rtx);
extern rtx        gen_sdot_prodv16qi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_udot_prodv16qi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_copysignv4hf3                       (rtx, rtx, rtx);
extern rtx        gen_copysignv8hf3                       (rtx, rtx, rtx);
extern rtx        gen_copysignv2sf3                       (rtx, rtx, rtx);
extern rtx        gen_copysignv4sf3                       (rtx, rtx, rtx);
extern rtx        gen_copysignv2df3                       (rtx, rtx, rtx);
extern rtx        gen_rsqrtv2sf2                          (rtx, rtx);
extern rtx        gen_rsqrtv4sf2                          (rtx, rtx);
extern rtx        gen_rsqrtv2df2                          (rtx, rtx);
extern rtx        gen_rsqrtsf2                            (rtx, rtx);
extern rtx        gen_rsqrtdf2                            (rtx, rtx);
extern rtx        gen_ashlv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_ashlv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_ashlv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_ashlv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_ashlv2si3                           (rtx, rtx, rtx);
extern rtx        gen_ashlv4si3                           (rtx, rtx, rtx);
extern rtx        gen_ashlv2di3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_lshrv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv2si3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv4si3                           (rtx, rtx, rtx);
extern rtx        gen_lshrv2di3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv8qi3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv16qi3                          (rtx, rtx, rtx);
extern rtx        gen_ashrv4hi3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv8hi3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv2si3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv4si3                           (rtx, rtx, rtx);
extern rtx        gen_ashrv2di3                           (rtx, rtx, rtx);
extern rtx        gen_vashlv8qi3                          (rtx, rtx, rtx);
extern rtx        gen_vashlv16qi3                         (rtx, rtx, rtx);
extern rtx        gen_vashlv4hi3                          (rtx, rtx, rtx);
extern rtx        gen_vashlv8hi3                          (rtx, rtx, rtx);
extern rtx        gen_vashlv2si3                          (rtx, rtx, rtx);
extern rtx        gen_vashlv4si3                          (rtx, rtx, rtx);
extern rtx        gen_vashlv2di3                          (rtx, rtx, rtx);
extern rtx        gen_vashrv8qi3                          (rtx, rtx, rtx);
extern rtx        gen_vashrv16qi3                         (rtx, rtx, rtx);
extern rtx        gen_vashrv4hi3                          (rtx, rtx, rtx);
extern rtx        gen_vashrv8hi3                          (rtx, rtx, rtx);
extern rtx        gen_vashrv2si3                          (rtx, rtx, rtx);
extern rtx        gen_vashrv4si3                          (rtx, rtx, rtx);
extern rtx        gen_aarch64_ashr_simddi                 (rtx, rtx, rtx);
extern rtx        gen_vlshrv8qi3                          (rtx, rtx, rtx);
extern rtx        gen_vlshrv16qi3                         (rtx, rtx, rtx);
extern rtx        gen_vlshrv4hi3                          (rtx, rtx, rtx);
extern rtx        gen_vlshrv8hi3                          (rtx, rtx, rtx);
extern rtx        gen_vlshrv2si3                          (rtx, rtx, rtx);
extern rtx        gen_vlshrv4si3                          (rtx, rtx, rtx);
extern rtx        gen_aarch64_lshr_simddi                 (rtx, rtx, rtx);
extern rtx        gen_vec_setv8qi                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv16qi                        (rtx, rtx, rtx);
extern rtx        gen_vec_setv4hi                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv8hi                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv2si                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv4si                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv2di                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv4hf                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv8hf                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv2sf                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv4sf                         (rtx, rtx, rtx);
extern rtx        gen_vec_setv2df                         (rtx, rtx, rtx);
extern rtx        gen_smaxv2di3                           (rtx, rtx, rtx);
extern rtx        gen_sminv2di3                           (rtx, rtx, rtx);
extern rtx        gen_umaxv2di3                           (rtx, rtx, rtx);
extern rtx        gen_uminv2di3                           (rtx, rtx, rtx);
extern rtx        gen_move_lo_quad_v16qi                  (rtx, rtx);
extern rtx        gen_move_lo_quad_v8hi                   (rtx, rtx);
extern rtx        gen_move_lo_quad_v4si                   (rtx, rtx);
extern rtx        gen_move_lo_quad_v2di                   (rtx, rtx);
extern rtx        gen_move_lo_quad_v8hf                   (rtx, rtx);
extern rtx        gen_move_lo_quad_v4sf                   (rtx, rtx);
extern rtx        gen_move_lo_quad_v2df                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v16qi                  (rtx, rtx);
extern rtx        gen_move_hi_quad_v8hi                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v4si                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v2di                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v8hf                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v4sf                   (rtx, rtx);
extern rtx        gen_move_hi_quad_v2df                   (rtx, rtx);
extern rtx        gen_vec_pack_trunc_v4hi                 (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_v2si                 (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_di                   (rtx, rtx, rtx);
extern rtx        gen_vec_unpacks_hi_v16qi                (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_v16qi                (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_v8hi                 (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_v8hi                 (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_v4si                 (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_v4si                 (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_v16qi                (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_v16qi                (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_v8hi                 (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_v8hi                 (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_v4si                 (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_v4si                 (rtx, rtx);
extern rtx        gen_vec_widen_smult_lo_v16qi            (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_lo_v16qi            (rtx, rtx, rtx);
extern rtx        gen_vec_widen_smult_lo_v8hi             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_lo_v8hi             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_smult_lo_v4si             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_lo_v4si             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_smult_hi_v16qi            (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_hi_v16qi            (rtx, rtx, rtx);
extern rtx        gen_vec_widen_smult_hi_v8hi             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_hi_v8hi             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_smult_hi_v4si             (rtx, rtx, rtx);
extern rtx        gen_vec_widen_umult_hi_v4si             (rtx, rtx, rtx);
extern rtx        gen_divv4hf3                            (rtx, rtx, rtx);
extern rtx        gen_divv8hf3                            (rtx, rtx, rtx);
extern rtx        gen_divv2sf3                            (rtx, rtx, rtx);
extern rtx        gen_divv4sf3                            (rtx, rtx, rtx);
extern rtx        gen_divv2df3                            (rtx, rtx, rtx);
extern rtx        gen_fixv4hfv4hi2                        (rtx, rtx);
extern rtx        gen_fixunsv4hfv4hi2                     (rtx, rtx);
extern rtx        gen_fixv8hfv8hi2                        (rtx, rtx);
extern rtx        gen_fixunsv8hfv8hi2                     (rtx, rtx);
extern rtx        gen_fixv2sfv2si2                        (rtx, rtx);
extern rtx        gen_fixunsv2sfv2si2                     (rtx, rtx);
extern rtx        gen_fixv4sfv4si2                        (rtx, rtx);
extern rtx        gen_fixunsv4sfv4si2                     (rtx, rtx);
extern rtx        gen_fixv2dfv2di2                        (rtx, rtx);
extern rtx        gen_fixunsv2dfv2di2                     (rtx, rtx);
extern rtx        gen_fix_truncv4hfv4hi2                  (rtx, rtx);
extern rtx        gen_fixuns_truncv4hfv4hi2               (rtx, rtx);
extern rtx        gen_fix_truncv8hfv8hi2                  (rtx, rtx);
extern rtx        gen_fixuns_truncv8hfv8hi2               (rtx, rtx);
extern rtx        gen_fix_truncv2sfv2si2                  (rtx, rtx);
extern rtx        gen_fixuns_truncv2sfv2si2               (rtx, rtx);
extern rtx        gen_fix_truncv4sfv4si2                  (rtx, rtx);
extern rtx        gen_fixuns_truncv4sfv4si2               (rtx, rtx);
extern rtx        gen_fix_truncv2dfv2di2                  (rtx, rtx);
extern rtx        gen_fixuns_truncv2dfv2di2               (rtx, rtx);
extern rtx        gen_ftruncv4hf2                         (rtx, rtx);
extern rtx        gen_ftruncv8hf2                         (rtx, rtx);
extern rtx        gen_ftruncv2sf2                         (rtx, rtx);
extern rtx        gen_ftruncv4sf2                         (rtx, rtx);
extern rtx        gen_ftruncv2df2                         (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_v8hf                 (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_v4sf                 (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_v8hf                 (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_v4sf                 (rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v4sf      (rtx, rtx, rtx);
extern rtx        gen_aarch64_float_truncate_hi_v8hf      (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_v2df                 (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_df                   (rtx, rtx, rtx);
extern rtx        gen_reduc_plus_scal_v8qi                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v16qi               (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v4hi                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v8hi                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v2si                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v4si                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v2di                (rtx, rtx);
extern rtx        gen_reduc_plus_scal_v4sf                (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_v4hf            (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_v4hf            (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v4hf                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v4hf                (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_v8hf            (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_v8hf            (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v8hf                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v8hf                (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_v2sf            (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_v2sf            (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v2sf                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v2sf                (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_v4sf            (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_v4sf            (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v4sf                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v4sf                (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_v2df            (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_v2df            (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v2df                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v2df                (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v8qi                (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v8qi                (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v8qi                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v8qi                (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v16qi               (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v16qi               (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v16qi               (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v16qi               (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v4hi                (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v4hi                (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v4hi                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v4hi                (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v8hi                (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v8hi                (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v8hi                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v8hi                (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v2si                (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v2si                (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v2si                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v2si                (rtx, rtx);
extern rtx        gen_reduc_umax_scal_v4si                (rtx, rtx);
extern rtx        gen_reduc_umin_scal_v4si                (rtx, rtx);
extern rtx        gen_reduc_smax_scal_v4si                (rtx, rtx);
extern rtx        gen_reduc_smin_scal_v4si                (rtx, rtx);
extern rtx        gen_aarch64_simd_bslv8qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv16qi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2di                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv8hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv4sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bslv2df                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bsldi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_bsldf                  (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v8qiv8qi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v16qiv16qi               (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v4hiv4hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v8hiv8hi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v2siv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v4siv4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v2div2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v2sfv2si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v4sfv4si                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_v2dfv2di                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vcond_mask_didi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv8qiv8qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv16qiv16qi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv4hiv4hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv8hiv8hi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv2siv2si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv4siv4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv2div2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpdidi                         (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv2sfv2si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv4sfv4si                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpv2dfv2di                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv8qiv8qi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv16qiv16qi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv4hiv4hi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv8hiv8hi                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv2siv2si                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv4siv4si                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuv2div2di                    (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpudidi                        (rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv8qiv8qi                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv16qiv16qi                     (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv4hiv4hi                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv8hiv8hi                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2siv2si                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv4siv4si                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2div2di                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2sfv2sf                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv4sfv4sf                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2dfv2df                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconddidi                           (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2siv2sf                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2sfv2si                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv4siv4sf                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv4sfv4si                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2div2df                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondv2dfv2di                       (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv8qiv8qi                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv16qiv16qi                    (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv4hiv4hi                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv8hiv8hi                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv2siv2si                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv4siv4si                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv2div2di                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondudidi                          (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv2sfv2si                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv4sfv4si                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduv2dfv2di                      (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev8qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev4hi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev4hf                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev2si                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinev2sf                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinedi                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_combinedf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinev8qi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinev4hi            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinev4hf            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinev2si            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinev2sf            (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinedi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_simd_combinedf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddl2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddl2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddl2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddl2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddl2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddl2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubl2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubl2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubl2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubl2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubl2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubl2v4si                  (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv16qi3                    (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv8hi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv4si3                     (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv8qi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv4hi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_ssumv2si3                     (rtx, rtx, rtx);
extern rtx        gen_widen_usumv16qi3                    (rtx, rtx, rtx);
extern rtx        gen_widen_usumv8hi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_usumv4si3                     (rtx, rtx, rtx);
extern rtx        gen_widen_usumv8qi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_usumv4hi3                     (rtx, rtx, rtx);
extern rtx        gen_widen_usumv2si3                     (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_saddw2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_uaddw2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_ssubw2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v16qi                 (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v8hi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_usubw2v4si                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2v8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2v4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2v8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2v4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_lanev8hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_lanev4si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_laneqv8hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_laneqv4si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_lanev8hi           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_lanev4si           (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_laneqv8hi          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_laneqv4si          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_nv8hi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlal2_nv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_nv8hi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmlsl2_nv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2v8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2v4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_lanev8hi           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_lanev4si           (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_laneqv8hi          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_laneqv4si          (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_nv8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_sqdmull2_nv4si              (rtx, rtx, rtx);
extern rtx        gen_sqrtv4hf2                           (rtx, rtx);
extern rtx        gen_sqrtv8hf2                           (rtx, rtx);
extern rtx        gen_sqrtv2sf2                           (rtx, rtx);
extern rtx        gen_sqrtv4sf2                           (rtx, rtx);
extern rtx        gen_sqrtv2df2                           (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv16qi               (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv8hi                (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv4si                (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv2di                (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv8hf                (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv4sf                (rtx, rtx);
extern rtx        gen_vec_load_lanesoiv2df                (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv16qi              (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv8hi               (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv4si               (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv2di               (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv8hf               (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv4sf               (rtx, rtx);
extern rtx        gen_vec_store_lanesoiv2df               (rtx, rtx);
extern rtx        gen_vec_load_lanesciv16qi               (rtx, rtx);
extern rtx        gen_vec_load_lanesciv8hi                (rtx, rtx);
extern rtx        gen_vec_load_lanesciv4si                (rtx, rtx);
extern rtx        gen_vec_load_lanesciv2di                (rtx, rtx);
extern rtx        gen_vec_load_lanesciv8hf                (rtx, rtx);
extern rtx        gen_vec_load_lanesciv4sf                (rtx, rtx);
extern rtx        gen_vec_load_lanesciv2df                (rtx, rtx);
extern rtx        gen_vec_store_lanesciv16qi              (rtx, rtx);
extern rtx        gen_vec_store_lanesciv8hi               (rtx, rtx);
extern rtx        gen_vec_store_lanesciv4si               (rtx, rtx);
extern rtx        gen_vec_store_lanesciv2di               (rtx, rtx);
extern rtx        gen_vec_store_lanesciv8hf               (rtx, rtx);
extern rtx        gen_vec_store_lanesciv4sf               (rtx, rtx);
extern rtx        gen_vec_store_lanesciv2df               (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv16qi               (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv8hi                (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv4si                (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv2di                (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv8hf                (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv4sf                (rtx, rtx);
extern rtx        gen_vec_load_lanesxiv2df                (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv16qi              (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv8hi               (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv4si               (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv2di               (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv8hf               (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv4sf               (rtx, rtx);
extern rtx        gen_vec_store_lanesxiv2df               (rtx, rtx);
extern rtx        gen_movoi                               (rtx, rtx);
extern rtx        gen_movci                               (rtx, rtx);
extern rtx        gen_movxi                               (rtx, rtx);
extern rtx        gen_aarch64_ld2rv8qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv8qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv8qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv16qi                   (rtx, rtx);
extern rtx        gen_aarch64_ld3rv16qi                   (rtx, rtx);
extern rtx        gen_aarch64_ld4rv16qi                   (rtx, rtx);
extern rtx        gen_aarch64_ld2rv4hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv4hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv4hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv8hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv8hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv8hi                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv2si                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv2si                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv2si                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv4si                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv4si                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv4si                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv2di                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv2di                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv2di                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv4hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv4hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv4hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv8hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv8hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv8hf                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv2sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv2sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv2sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv4sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv4sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv4sf                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rv2df                    (rtx, rtx);
extern rtx        gen_aarch64_ld3rv2df                    (rtx, rtx);
extern rtx        gen_aarch64_ld4rv2df                    (rtx, rtx);
extern rtx        gen_aarch64_ld2rdi                      (rtx, rtx);
extern rtx        gen_aarch64_ld3rdi                      (rtx, rtx);
extern rtx        gen_aarch64_ld4rdi                      (rtx, rtx);
extern rtx        gen_aarch64_ld2rdf                      (rtx, rtx);
extern rtx        gen_aarch64_ld3rdf                      (rtx, rtx);
extern rtx        gen_aarch64_ld4rdf                      (rtx, rtx);
extern rtx        gen_aarch64_ld2v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v2si                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld2di                       (rtx, rtx);
extern rtx        gen_aarch64_ld2df                       (rtx, rtx);
extern rtx        gen_aarch64_ld3v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v2si                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld3di                       (rtx, rtx);
extern rtx        gen_aarch64_ld3df                       (rtx, rtx);
extern rtx        gen_aarch64_ld4v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v2si                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld4di                       (rtx, rtx);
extern rtx        gen_aarch64_ld4df                       (rtx, rtx);
extern rtx        gen_aarch64_ld1v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld1v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v2si                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v4si                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v2di                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld1v2df                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld2v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v4si                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v2di                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld2v2df                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld3v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v4si                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v2di                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld3v2df                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_ld4v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v4si                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v2di                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_ld4v2df                     (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v16qi                  (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v8hi                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v4si                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v2di                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v8hf                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v4sf                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v2df                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v8qi                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v4hi                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v4hf                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v2si                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2v2sf                   (rtx, rtx);
extern rtx        gen_aarch64_ld1x2di                     (rtx, rtx);
extern rtx        gen_aarch64_ld1x2df                     (rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev8qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev8qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev8qi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev16qi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev16qi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev16qi               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev4hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev8hi                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev2si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev4si                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev2di                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev2di                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev2di                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev4hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev4hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev4hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev8hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev8hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev8hf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev2sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev2sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev2sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev4sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev4sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev4sf                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanev2df                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanev2df                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanev2df                (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanedi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanedi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanedi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld2_lanedf                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld3_lanedf                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_ld4_lanedf                  (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoiv8qi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoiv4hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoiv4hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoiv2si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoiv2sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoidi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregoidf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregciv8qi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregciv4hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregciv4hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregciv2si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregciv2sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregcidi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregcidf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxiv8qi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxiv4hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxiv4hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxiv2si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxiv2sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxidi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_dregxidf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregoiv2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregciv2df              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv16qi             (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv8hi              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv4si              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv2di              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv8hf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv4sf              (rtx, rtx, rtx);
extern rtx        gen_aarch64_get_qregxiv2df              (rtx, rtx, rtx);
extern rtx        gen_vec_permv8qi                        (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permv16qi                       (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_st2v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_st2v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_st2v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_st2v2si                     (rtx, rtx);
extern rtx        gen_aarch64_st2v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_st2di                       (rtx, rtx);
extern rtx        gen_aarch64_st2df                       (rtx, rtx);
extern rtx        gen_aarch64_st3v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_st3v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_st3v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_st3v2si                     (rtx, rtx);
extern rtx        gen_aarch64_st3v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_st3di                       (rtx, rtx);
extern rtx        gen_aarch64_st3df                       (rtx, rtx);
extern rtx        gen_aarch64_st4v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_st4v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_st4v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_st4v2si                     (rtx, rtx);
extern rtx        gen_aarch64_st4v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_st4di                       (rtx, rtx);
extern rtx        gen_aarch64_st4df                       (rtx, rtx);
extern rtx        gen_aarch64_st2v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_st2v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_st2v4si                     (rtx, rtx);
extern rtx        gen_aarch64_st2v2di                     (rtx, rtx);
extern rtx        gen_aarch64_st2v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_st2v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_st2v2df                     (rtx, rtx);
extern rtx        gen_aarch64_st3v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_st3v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_st3v4si                     (rtx, rtx);
extern rtx        gen_aarch64_st3v2di                     (rtx, rtx);
extern rtx        gen_aarch64_st3v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_st3v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_st3v2df                     (rtx, rtx);
extern rtx        gen_aarch64_st4v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_st4v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_st4v4si                     (rtx, rtx);
extern rtx        gen_aarch64_st4v2di                     (rtx, rtx);
extern rtx        gen_aarch64_st4v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_st4v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_st4v2df                     (rtx, rtx);
extern rtx        gen_aarch64_st2_lanev8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev8qi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev16qi               (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev4hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev8hi                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev2si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev4si                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev2di                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev4hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev4hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev4hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev8hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev8hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev8hf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev2sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev2sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev2sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev4sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev4sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev4sf                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanev2df                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanev2df                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanev2df                (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanedi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanedi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanedi                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st2_lanedf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st3_lanedf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st4_lanedf                  (rtx, rtx, rtx);
extern rtx        gen_aarch64_st1v8qi                     (rtx, rtx);
extern rtx        gen_aarch64_st1v16qi                    (rtx, rtx);
extern rtx        gen_aarch64_st1v4hi                     (rtx, rtx);
extern rtx        gen_aarch64_st1v8hi                     (rtx, rtx);
extern rtx        gen_aarch64_st1v2si                     (rtx, rtx);
extern rtx        gen_aarch64_st1v4si                     (rtx, rtx);
extern rtx        gen_aarch64_st1v2di                     (rtx, rtx);
extern rtx        gen_aarch64_st1v4hf                     (rtx, rtx);
extern rtx        gen_aarch64_st1v8hf                     (rtx, rtx);
extern rtx        gen_aarch64_st1v2sf                     (rtx, rtx);
extern rtx        gen_aarch64_st1v4sf                     (rtx, rtx);
extern rtx        gen_aarch64_st1v2df                     (rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv16qi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv8hi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv2di              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv8hf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv4sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregoiv2df              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv16qi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv8hi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv2di              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv8hf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv4sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregciv2df              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv16qi             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv8hi              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv4si              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv2di              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv8hf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv4sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_set_qregxiv2df              (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_initv8qiqi                      (rtx, rtx);
extern rtx        gen_vec_initv16qiqi                     (rtx, rtx);
extern rtx        gen_vec_initv4hihi                      (rtx, rtx);
extern rtx        gen_vec_initv8hihi                      (rtx, rtx);
extern rtx        gen_vec_initv2sisi                      (rtx, rtx);
extern rtx        gen_vec_initv4sisi                      (rtx, rtx);
extern rtx        gen_vec_initv2didi                      (rtx, rtx);
extern rtx        gen_vec_initv4hfhf                      (rtx, rtx);
extern rtx        gen_vec_initv8hfhf                      (rtx, rtx);
extern rtx        gen_vec_initv2sfsf                      (rtx, rtx);
extern rtx        gen_vec_initv4sfsf                      (rtx, rtx);
extern rtx        gen_vec_initv2dfdf                      (rtx, rtx);
extern rtx        gen_vec_extractv8qiqi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv16qiqi                  (rtx, rtx, rtx);
extern rtx        gen_vec_extractv4hihi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv8hihi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv2sisi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv4sisi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv2didi                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv4hfhf                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv8hfhf                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv2sfsf                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv4sfsf                   (rtx, rtx, rtx);
extern rtx        gen_vec_extractv2dfdf                   (rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_lowv2sf               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_lowv2sf               (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_lowv4sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_lowv4sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_highv2sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_highv2sf              (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_highv4sf             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_highv4sf             (rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_lane_lowv2sf          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_lane_lowv2sf          (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_lane_highv2sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_lane_highv2sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_laneq_lowv4sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_laneq_lowv4sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_laneq_highv4sf       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_laneq_highv4sf       (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_laneq_lowv2sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_laneq_lowv2sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlal_laneq_highv2sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlsl_laneq_highv2sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_lane_lowv4sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_lane_lowv4sf         (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlalq_lane_highv4sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_aarch64_fmlslq_lane_highv4sf        (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_compare_and_swapqi           (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_compare_and_swaphi           (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_compare_and_swapsi           (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_compare_and_swapdi           (rtx, rtx, rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_exchangeqi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_exchangehi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_exchangesi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_exchangedi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_addqi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_subqi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_orqi                         (rtx, rtx, rtx);
extern rtx        gen_atomic_xorqi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_andqi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_addhi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_subhi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_orhi                         (rtx, rtx, rtx);
extern rtx        gen_atomic_xorhi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_andhi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_addsi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_subsi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_orsi                         (rtx, rtx, rtx);
extern rtx        gen_atomic_xorsi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_andsi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_adddi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_subdi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_ordi                         (rtx, rtx, rtx);
extern rtx        gen_atomic_xordi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_anddi                        (rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_addqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_subqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_orqi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_xorqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_andqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_addhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_subhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_orhi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_xorhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_andhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_addsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_subsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_orsi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_xorsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_andsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_adddi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_subdi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_ordi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_xordi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_fetch_anddi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_add_fetchqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_sub_fetchqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_or_fetchqi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_xor_fetchqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_and_fetchqi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_add_fetchhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_sub_fetchhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_or_fetchhi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_xor_fetchhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_and_fetchhi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_add_fetchsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_sub_fetchsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_or_fetchsi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_xor_fetchsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_and_fetchsi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_add_fetchdi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_sub_fetchdi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_or_fetchdi                   (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_xor_fetchdi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_atomic_and_fetchdi                  (rtx, rtx, rtx, rtx);
extern rtx        gen_mem_thread_fence                    (rtx);
extern rtx        gen_dmb                                 (rtx);
extern rtx        gen_movvnx16qi                          (rtx, rtx);
extern rtx        gen_movvnx8hi                           (rtx, rtx);
extern rtx        gen_movvnx4si                           (rtx, rtx);
extern rtx        gen_movvnx2di                           (rtx, rtx);
extern rtx        gen_movvnx8hf                           (rtx, rtx);
extern rtx        gen_movvnx4sf                           (rtx, rtx);
extern rtx        gen_movvnx2df                           (rtx, rtx);
extern rtx        gen_aarch64_sve_reload_be               (rtx, rtx, rtx);
extern rtx        gen_movmisalignvnx16qi                  (rtx, rtx);
extern rtx        gen_movmisalignvnx8hi                   (rtx, rtx);
extern rtx        gen_movmisalignvnx4si                   (rtx, rtx);
extern rtx        gen_movmisalignvnx2di                   (rtx, rtx);
extern rtx        gen_movmisalignvnx8hf                   (rtx, rtx);
extern rtx        gen_movmisalignvnx4sf                   (rtx, rtx);
extern rtx        gen_movmisalignvnx2df                   (rtx, rtx);
extern rtx        gen_gather_loadvnx4si                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_gather_loadvnx2di                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_gather_loadvnx4sf                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_gather_loadvnx2df                   (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_scatter_storevnx4si                 (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_scatter_storevnx2di                 (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_scatter_storevnx4sf                 (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_scatter_storevnx2df                 (rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_movvnx32qi                          (rtx, rtx);
extern rtx        gen_movvnx16hi                          (rtx, rtx);
extern rtx        gen_movvnx8si                           (rtx, rtx);
extern rtx        gen_movvnx4di                           (rtx, rtx);
extern rtx        gen_movvnx16hf                          (rtx, rtx);
extern rtx        gen_movvnx8sf                           (rtx, rtx);
extern rtx        gen_movvnx4df                           (rtx, rtx);
extern rtx        gen_movvnx48qi                          (rtx, rtx);
extern rtx        gen_movvnx24hi                          (rtx, rtx);
extern rtx        gen_movvnx12si                          (rtx, rtx);
extern rtx        gen_movvnx6di                           (rtx, rtx);
extern rtx        gen_movvnx24hf                          (rtx, rtx);
extern rtx        gen_movvnx12sf                          (rtx, rtx);
extern rtx        gen_movvnx6df                           (rtx, rtx);
extern rtx        gen_movvnx64qi                          (rtx, rtx);
extern rtx        gen_movvnx32hi                          (rtx, rtx);
extern rtx        gen_movvnx16si                          (rtx, rtx);
extern rtx        gen_movvnx8di                           (rtx, rtx);
extern rtx        gen_movvnx32hf                          (rtx, rtx);
extern rtx        gen_movvnx16sf                          (rtx, rtx);
extern rtx        gen_movvnx8df                           (rtx, rtx);
extern rtx        gen_movvnx16bi                          (rtx, rtx);
extern rtx        gen_movvnx8bi                           (rtx, rtx);
extern rtx        gen_movvnx4bi                           (rtx, rtx);
extern rtx        gen_movvnx2bi                           (rtx, rtx);
extern rtx        gen_vec_extractvnx16biqi                (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx8bihi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx4bisi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx2bidi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx16qiqi                (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx8hihi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx4sisi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx2didi                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx8hfhf                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx4sfsf                 (rtx, rtx, rtx);
extern rtx        gen_vec_extractvnx2dfdf                 (rtx, rtx, rtx);
extern rtx        gen_vec_duplicatevnx16qi                (rtx, rtx);
extern rtx        gen_vec_duplicatevnx8hi                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx4si                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx2di                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx8hf                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx4sf                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx2df                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx16bi                (rtx, rtx);
extern rtx        gen_vec_duplicatevnx8bi                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx4bi                 (rtx, rtx);
extern rtx        gen_vec_duplicatevnx2bi                 (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx32qivnx16qi        (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx16hivnx8hi         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx8sivnx4si          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx4divnx2di          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx16hfvnx8hf         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx8sfvnx4sf          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx4dfvnx2df          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx48qivnx16qi        (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx24hivnx8hi         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx12sivnx4si         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx6divnx2di          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx24hfvnx8hf         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx12sfvnx4sf         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx6dfvnx2df          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx64qivnx16qi        (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx32hivnx8hi         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx16sivnx4si         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx8divnx2di          (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx32hfvnx8hf         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx16sfvnx4sf         (rtx, rtx);
extern rtx        gen_vec_load_lanesvnx8dfvnx2df          (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx32qivnx16qi       (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx16hivnx8hi        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx8sivnx4si         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx4divnx2di         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx16hfvnx8hf        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx8sfvnx4sf         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx4dfvnx2df         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx48qivnx16qi       (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx24hivnx8hi        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx12sivnx4si        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx6divnx2di         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx24hfvnx8hf        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx12sfvnx4sf        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx6dfvnx2df         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx64qivnx16qi       (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx32hivnx8hi        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx16sivnx4si        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx8divnx2di         (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx32hfvnx8hf        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx16sfvnx4sf        (rtx, rtx);
extern rtx        gen_vec_store_lanesvnx8dfvnx2df         (rtx, rtx);
extern rtx        gen_vec_permvnx16qi                     (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx8hi                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx4si                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx2di                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx8hf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx4sf                      (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_permvnx2df                      (rtx, rtx, rtx, rtx);
extern rtx        gen_mulvnx16qi3                         (rtx, rtx, rtx);
extern rtx        gen_mulvnx8hi3                          (rtx, rtx, rtx);
extern rtx        gen_mulvnx4si3                          (rtx, rtx, rtx);
extern rtx        gen_mulvnx2di3                          (rtx, rtx, rtx);
extern rtx        gen_smulvnx16qi3_highpart               (rtx, rtx, rtx);
extern rtx        gen_umulvnx16qi3_highpart               (rtx, rtx, rtx);
extern rtx        gen_smulvnx8hi3_highpart                (rtx, rtx, rtx);
extern rtx        gen_umulvnx8hi3_highpart                (rtx, rtx, rtx);
extern rtx        gen_smulvnx4si3_highpart                (rtx, rtx, rtx);
extern rtx        gen_umulvnx4si3_highpart                (rtx, rtx, rtx);
extern rtx        gen_smulvnx2di3_highpart                (rtx, rtx, rtx);
extern rtx        gen_umulvnx2di3_highpart                (rtx, rtx, rtx);
extern rtx        gen_negvnx16qi2                         (rtx, rtx);
extern rtx        gen_one_cmplvnx16qi2                    (rtx, rtx);
extern rtx        gen_popcountvnx16qi2                    (rtx, rtx);
extern rtx        gen_negvnx8hi2                          (rtx, rtx);
extern rtx        gen_one_cmplvnx8hi2                     (rtx, rtx);
extern rtx        gen_popcountvnx8hi2                     (rtx, rtx);
extern rtx        gen_negvnx4si2                          (rtx, rtx);
extern rtx        gen_one_cmplvnx4si2                     (rtx, rtx);
extern rtx        gen_popcountvnx4si2                     (rtx, rtx);
extern rtx        gen_negvnx2di2                          (rtx, rtx);
extern rtx        gen_one_cmplvnx2di2                     (rtx, rtx);
extern rtx        gen_popcountvnx2di2                     (rtx, rtx);
extern rtx        gen_iorvnx16bi3                         (rtx, rtx, rtx);
extern rtx        gen_xorvnx16bi3                         (rtx, rtx, rtx);
extern rtx        gen_iorvnx8bi3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx8bi3                          (rtx, rtx, rtx);
extern rtx        gen_iorvnx4bi3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx4bi3                          (rtx, rtx, rtx);
extern rtx        gen_iorvnx2bi3                          (rtx, rtx, rtx);
extern rtx        gen_xorvnx2bi3                          (rtx, rtx, rtx);
extern rtx        gen_one_cmplvnx16bi2                    (rtx, rtx);
extern rtx        gen_one_cmplvnx8bi2                     (rtx, rtx);
extern rtx        gen_one_cmplvnx4bi2                     (rtx, rtx);
extern rtx        gen_one_cmplvnx2bi2                     (rtx, rtx);
extern rtx        gen_vashlvnx16qi3                       (rtx, rtx, rtx);
extern rtx        gen_vashrvnx16qi3                       (rtx, rtx, rtx);
extern rtx        gen_vlshrvnx16qi3                       (rtx, rtx, rtx);
extern rtx        gen_vashlvnx8hi3                        (rtx, rtx, rtx);
extern rtx        gen_vashrvnx8hi3                        (rtx, rtx, rtx);
extern rtx        gen_vlshrvnx8hi3                        (rtx, rtx, rtx);
extern rtx        gen_vashlvnx4si3                        (rtx, rtx, rtx);
extern rtx        gen_vashrvnx4si3                        (rtx, rtx, rtx);
extern rtx        gen_vlshrvnx4si3                        (rtx, rtx, rtx);
extern rtx        gen_vashlvnx2di3                        (rtx, rtx, rtx);
extern rtx        gen_vashrvnx2di3                        (rtx, rtx, rtx);
extern rtx        gen_vlshrvnx2di3                        (rtx, rtx, rtx);
extern rtx        gen_ashlvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_ashrvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_lshrvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_ashlvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_ashrvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_lshrvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_ashlvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_ashrvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_lshrvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_ashlvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_ashrvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_lshrvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_vcondvnx16qivnx16qi                 (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx8hivnx8hi                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx4sivnx4si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx2divnx2di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx8hfvnx8hi                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx4sfvnx4si                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx2dfvnx2di                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx16qivnx16qi                (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx8hivnx8hi                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx4sivnx4si                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx2divnx2di                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx8hfvnx8hi                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx4sfvnx4si                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vconduvnx2dfvnx2di                  (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx4sivnx4sf                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx2divnx2df                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx4sfvnx4sf                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vcondvnx2dfvnx2df                   (rtx, rtx, rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx16qivnx16bi               (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx8hivnx8bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx4sivnx4bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx2divnx2bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuvnx16qivnx16bi              (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuvnx8hivnx8bi                (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuvnx4sivnx4bi                (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpuvnx2divnx2bi                (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx8hfvnx8bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx4sfvnx4bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_vec_cmpvnx2dfvnx2bi                 (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchvnx16bi4                     (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchvnx8bi4                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchvnx4bi4                      (rtx, rtx, rtx, rtx);
extern rtx        gen_cbranchvnx2bi4                      (rtx, rtx, rtx, rtx);
extern rtx        gen_smaxvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_sminvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_umaxvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_uminvnx16qi3                        (rtx, rtx, rtx);
extern rtx        gen_smaxvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_umaxvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_uminvnx8hi3                         (rtx, rtx, rtx);
extern rtx        gen_smaxvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_umaxvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_uminvnx4si3                         (rtx, rtx, rtx);
extern rtx        gen_smaxvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_umaxvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_uminvnx2di3                         (rtx, rtx, rtx);
extern rtx        gen_smaxvnx8hf3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx8hf3                         (rtx, rtx, rtx);
extern rtx        gen_smaxvnx4sf3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx4sf3                         (rtx, rtx, rtx);
extern rtx        gen_smaxvnx2df3                         (rtx, rtx, rtx);
extern rtx        gen_sminvnx2df3                         (rtx, rtx, rtx);
extern rtx        gen_smax_nanvnx8hf3                     (rtx, rtx, rtx);
extern rtx        gen_smin_nanvnx8hf3                     (rtx, rtx, rtx);
extern rtx        gen_fmaxvnx8hf3                         (rtx, rtx, rtx);
extern rtx        gen_fminvnx8hf3                         (rtx, rtx, rtx);
extern rtx        gen_smax_nanvnx4sf3                     (rtx, rtx, rtx);
extern rtx        gen_smin_nanvnx4sf3                     (rtx, rtx, rtx);
extern rtx        gen_fmaxvnx4sf3                         (rtx, rtx, rtx);
extern rtx        gen_fminvnx4sf3                         (rtx, rtx, rtx);
extern rtx        gen_smax_nanvnx2df3                     (rtx, rtx, rtx);
extern rtx        gen_smin_nanvnx2df3                     (rtx, rtx, rtx);
extern rtx        gen_fmaxvnx2df3                         (rtx, rtx, rtx);
extern rtx        gen_fminvnx2df3                         (rtx, rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx16qi             (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx8hi              (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx4si              (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx2di              (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx8hf              (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx4sf              (rtx, rtx);
extern rtx        gen_reduc_plus_scal_vnx2df              (rtx, rtx);
extern rtx        gen_reduc_umax_scal_vnx16qi             (rtx, rtx);
extern rtx        gen_reduc_umin_scal_vnx16qi             (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx16qi             (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx16qi             (rtx, rtx);
extern rtx        gen_reduc_umax_scal_vnx8hi              (rtx, rtx);
extern rtx        gen_reduc_umin_scal_vnx8hi              (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx8hi              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx8hi              (rtx, rtx);
extern rtx        gen_reduc_umax_scal_vnx4si              (rtx, rtx);
extern rtx        gen_reduc_umin_scal_vnx4si              (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx4si              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx4si              (rtx, rtx);
extern rtx        gen_reduc_umax_scal_vnx2di              (rtx, rtx);
extern rtx        gen_reduc_umin_scal_vnx2di              (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx2di              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx2di              (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_vnx8hf          (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_vnx8hf          (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx8hf              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx8hf              (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_vnx4sf          (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_vnx4sf          (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx4sf              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx4sf              (rtx, rtx);
extern rtx        gen_reduc_smax_nan_scal_vnx2df          (rtx, rtx);
extern rtx        gen_reduc_smin_nan_scal_vnx2df          (rtx, rtx);
extern rtx        gen_reduc_smax_scal_vnx2df              (rtx, rtx);
extern rtx        gen_reduc_smin_scal_vnx2df              (rtx, rtx);
extern rtx        gen_reduc_and_scal_vnx16qi              (rtx, rtx);
extern rtx        gen_reduc_ior_scal_vnx16qi              (rtx, rtx);
extern rtx        gen_reduc_xor_scal_vnx16qi              (rtx, rtx);
extern rtx        gen_reduc_and_scal_vnx8hi               (rtx, rtx);
extern rtx        gen_reduc_ior_scal_vnx8hi               (rtx, rtx);
extern rtx        gen_reduc_xor_scal_vnx8hi               (rtx, rtx);
extern rtx        gen_reduc_and_scal_vnx4si               (rtx, rtx);
extern rtx        gen_reduc_ior_scal_vnx4si               (rtx, rtx);
extern rtx        gen_reduc_xor_scal_vnx4si               (rtx, rtx);
extern rtx        gen_reduc_and_scal_vnx2di               (rtx, rtx);
extern rtx        gen_reduc_ior_scal_vnx2di               (rtx, rtx);
extern rtx        gen_reduc_xor_scal_vnx2di               (rtx, rtx);
extern rtx        gen_fold_left_plus_vnx8hf               (rtx, rtx, rtx);
extern rtx        gen_fold_left_plus_vnx4sf               (rtx, rtx, rtx);
extern rtx        gen_fold_left_plus_vnx2df               (rtx, rtx, rtx);
extern rtx        gen_addvnx8hf3                          (rtx, rtx, rtx);
extern rtx        gen_addvnx4sf3                          (rtx, rtx, rtx);
extern rtx        gen_addvnx2df3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx8hf3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx4sf3                          (rtx, rtx, rtx);
extern rtx        gen_subvnx2df3                          (rtx, rtx, rtx);
extern rtx        gen_mulvnx8hf3                          (rtx, rtx, rtx);
extern rtx        gen_mulvnx4sf3                          (rtx, rtx, rtx);
extern rtx        gen_mulvnx2df3                          (rtx, rtx, rtx);
extern rtx        gen_fmavnx8hf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fmavnx4sf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fmavnx2df4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmavnx8hf4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmavnx4sf4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmavnx2df4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_fmsvnx8hf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fmsvnx4sf4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fmsvnx2df4                          (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmsvnx8hf4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmsvnx4sf4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_fnmsvnx2df4                         (rtx, rtx, rtx, rtx);
extern rtx        gen_divvnx8hf3                          (rtx, rtx, rtx);
extern rtx        gen_divvnx4sf3                          (rtx, rtx, rtx);
extern rtx        gen_divvnx2df3                          (rtx, rtx, rtx);
extern rtx        gen_negvnx8hf2                          (rtx, rtx);
extern rtx        gen_absvnx8hf2                          (rtx, rtx);
extern rtx        gen_sqrtvnx8hf2                         (rtx, rtx);
extern rtx        gen_negvnx4sf2                          (rtx, rtx);
extern rtx        gen_absvnx4sf2                          (rtx, rtx);
extern rtx        gen_sqrtvnx4sf2                         (rtx, rtx);
extern rtx        gen_negvnx2df2                          (rtx, rtx);
extern rtx        gen_absvnx2df2                          (rtx, rtx);
extern rtx        gen_sqrtvnx2df2                         (rtx, rtx);
extern rtx        gen_btruncvnx8hf2                       (rtx, rtx);
extern rtx        gen_ceilvnx8hf2                         (rtx, rtx);
extern rtx        gen_floorvnx8hf2                        (rtx, rtx);
extern rtx        gen_frintnvnx8hf2                       (rtx, rtx);
extern rtx        gen_nearbyintvnx8hf2                    (rtx, rtx);
extern rtx        gen_rintvnx8hf2                         (rtx, rtx);
extern rtx        gen_roundvnx8hf2                        (rtx, rtx);
extern rtx        gen_btruncvnx4sf2                       (rtx, rtx);
extern rtx        gen_ceilvnx4sf2                         (rtx, rtx);
extern rtx        gen_floorvnx4sf2                        (rtx, rtx);
extern rtx        gen_frintnvnx4sf2                       (rtx, rtx);
extern rtx        gen_nearbyintvnx4sf2                    (rtx, rtx);
extern rtx        gen_rintvnx4sf2                         (rtx, rtx);
extern rtx        gen_roundvnx4sf2                        (rtx, rtx);
extern rtx        gen_btruncvnx2df2                       (rtx, rtx);
extern rtx        gen_ceilvnx2df2                         (rtx, rtx);
extern rtx        gen_floorvnx2df2                        (rtx, rtx);
extern rtx        gen_frintnvnx2df2                       (rtx, rtx);
extern rtx        gen_nearbyintvnx2df2                    (rtx, rtx);
extern rtx        gen_rintvnx2df2                         (rtx, rtx);
extern rtx        gen_roundvnx2df2                        (rtx, rtx);
extern rtx        gen_fix_truncvnx8hfvnx8hi2              (rtx, rtx);
extern rtx        gen_fixuns_truncvnx8hfvnx8hi2           (rtx, rtx);
extern rtx        gen_fix_truncvnx4sfvnx4si2              (rtx, rtx);
extern rtx        gen_fixuns_truncvnx4sfvnx4si2           (rtx, rtx);
extern rtx        gen_fix_truncvnx2dfvnx2di2              (rtx, rtx);
extern rtx        gen_fixuns_truncvnx2dfvnx2di2           (rtx, rtx);
extern rtx        gen_floatvnx8hivnx8hf2                  (rtx, rtx);
extern rtx        gen_floatunsvnx8hivnx8hf2               (rtx, rtx);
extern rtx        gen_floatvnx4sivnx4sf2                  (rtx, rtx);
extern rtx        gen_floatunsvnx4sivnx4sf2               (rtx, rtx);
extern rtx        gen_floatvnx2divnx2df2                  (rtx, rtx);
extern rtx        gen_floatunsvnx2divnx2df2               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx16bi              (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx16bi              (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx16bi              (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx16bi              (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx8bi               (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx8bi               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx8bi               (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx8bi               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx4bi               (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx4bi               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx4bi               (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx4bi               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx16qi              (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx16qi              (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx16qi              (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx16qi              (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx8hi               (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx8hi               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx8hi               (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx8hi               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx4si               (rtx, rtx);
extern rtx        gen_vec_unpacku_hi_vnx4si               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx4si               (rtx, rtx);
extern rtx        gen_vec_unpacku_lo_vnx4si               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx8hf               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx8hf               (rtx, rtx);
extern rtx        gen_vec_unpacks_lo_vnx4sf               (rtx, rtx);
extern rtx        gen_vec_unpacks_hi_vnx4sf               (rtx, rtx);
extern rtx        gen_vec_unpacks_float_lo_vnx4si         (rtx, rtx);
extern rtx        gen_vec_unpacks_float_hi_vnx4si         (rtx, rtx);
extern rtx        gen_vec_unpacku_float_lo_vnx4si         (rtx, rtx);
extern rtx        gen_vec_unpacku_float_hi_vnx4si         (rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx4sf               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_trunc_vnx2df               (rtx, rtx, rtx);
extern rtx        gen_vec_pack_sfix_trunc_vnx2df          (rtx, rtx, rtx);
extern rtx        gen_vec_pack_ufix_trunc_vnx2df          (rtx, rtx, rtx);

#endif /* GCC_INSN_FLAGS_H */
