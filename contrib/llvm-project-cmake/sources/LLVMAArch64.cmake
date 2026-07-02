# Source files for LLVM AArch64 target

# AArch64 Utils sources (needed by both Desc and CodeGen)
set(LLVMAARCH64UTILS_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/Utils/AArch64BaseInfo.cpp
)

# AArch64 Info sources
set(LLVMAARCH64INFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/TargetInfo/AArch64TargetInfo.cpp
)

# AArch64 Desc sources
set(LLVMAARCH64DESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64AsmBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64ELFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64ELFStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64InstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64MachObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64MCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64MCCodeEmitter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64MCExpr.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64MCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64TargetStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64WinCOFFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MCTargetDesc/AArch64WinCOFFStreamer.cpp
)

# AArch64 CodeGen sources
set(LLVMAARCH64CODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64CallLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64GlobalISelUtils.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64InstructionSelector.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64LegalizerInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64O0PreLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64PostLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64PostLegalizerLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64PostSelectOptimize.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64PreLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/GISel/AArch64RegisterBankInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64A53Fix835769.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64A57FPLoadBalancing.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64AdvSIMDScalarPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64Arm64ECCallLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64AsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64BranchTargets.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64CallingConvention.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64CleanupLocalDynamicTLSPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64CollectLOH.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64CompressJumpTables.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64CondBrTuning.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ConditionalCompares.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ConditionOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64DeadRegisterDefinitionsPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ExpandImm.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ExpandPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64FalkorHWPFFix.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64FastISel.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64FrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64InstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64ISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64LoadStoreOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64LowerHomogeneousPrologEpilog.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64MachineFunctionInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64MachineScheduler.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64MacroFusion.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64MCInstLower.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64MIPeepholeOpt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64PBQPRegAlloc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64PointerAuth.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64PostCoalescerPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64PrologueEpilogue.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64PromoteConstant.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64RedundantCopyElimination.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64RedundantCondBranchPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64RegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64SelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64SIMDInstrOpt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64SLSHardening.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64SpeculationHardening.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64StackTagging.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64StackTaggingPreRA.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64StorePairSuppress.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64Subtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64SMEAttributes.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64TargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64TargetObjectFile.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64TargetTransformInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/SMEABIPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/MachineSMEABIPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/SMEPeepholeOpt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/AArch64/SVEIntrinsicOpts.cpp
)