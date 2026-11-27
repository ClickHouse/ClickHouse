# Source files for LLVM RISCV target

# RISCV Info sources
set(LLVMRISCVINFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/TargetInfo/RISCVTargetInfo.cpp
)

# RISCV Desc sources
set(LLVMRISCVDESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVAsmBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVBaseInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVELFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVELFStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMatInt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMCCodeEmitter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMCExpr.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMachObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMCObjectFileInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVMCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/MCTargetDesc/RISCVTargetStreamer.cpp
)

# RISCV CodeGen sources
set(LLVMRISCVCODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVCallLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVInstructionSelector.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVLegalizerInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVO0PreLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVPostLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVPreLegalizerCombiner.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/GISel/RISCVRegisterBankInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVAsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVCallingConv.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVCodeGenPrepare.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVConstantPoolValue.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVDeadRegisterDefinitions.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVExpandAtomicPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVExpandPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVFoldMemOffset.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVFrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVGatherScatterLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVIndirectBranchTracking.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVInsertReadWriteCSR.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVInsertVSETVLI.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVInsertWriteVXRM.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVInstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVInterleavedAccess.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVLandingPadSetup.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVLateBranchOpt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVLoadStoreOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVMachineFunctionInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVMachineScheduler.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVMakeCompressible.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVMergeBaseOffset.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVMoveMerger.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVOptWInstrs.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVPostRAExpandPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVPromoteConstant.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVPushPopOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVRedundantCopyElimination.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVRegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVSelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVSubtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVTargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVTargetObjectFile.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVTargetTransformInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVVectorMaskDAGMutation.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVVectorPeephole.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVVLOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVVSETVLIInfoAnalysis.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVVMV0Elimination.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVZacasABIFix.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVZilsdOptimizer.cpp
)