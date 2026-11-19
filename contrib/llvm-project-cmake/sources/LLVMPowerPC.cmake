# Source files for LLVM PowerPC target

# PowerPC Info sources
set(LLVMPOWERPCINFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/TargetInfo/PowerPCTargetInfo.cpp
)

# PowerPC Desc sources
set(LLVMPOWERPCDESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCAsmBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCELFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCELFStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCMCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCMCCodeEmitter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCMCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCPredicates.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCXCOFFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/MCTargetDesc/PPCXCOFFStreamer.cpp
)

# PowerPC CodeGen sources
set(LLVMPOWERPCCODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/GISel/PPCCallLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/GISel/PPCInstructionSelector.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/GISel/PPCLegalizerInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/GISel/PPCRegisterBankInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCAsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCBoolRetToInt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCBranchCoalescing.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCBranchSelector.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCCallingConv.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCCTRLoops.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCCTRLoopsVerify.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCEarlyReturn.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCExpandAtomicPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCFastISel.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCFrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCGenScalarMASSEntries.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCHazardRecognizers.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCInstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCLoopInstrFormPrep.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCLowerMASSVEntries.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCMachineFunctionInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCMachineScheduler.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCMacroFusion.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCMCInstLower.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCMIPeephole.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCPreEmitPeephole.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCReduceCRLogicals.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCRegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCSelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCSubtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCTargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCTargetObjectFile.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCTargetTransformInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCTLSDynamicCall.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCTOCRegDeps.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCVSXFMAMutate.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCVSXSwapRemoval.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPCVSXWACCCopy.cpp
)