# Source files for the LLVM NVPTX target (Info, Desc and CodeGen), which emits the PTX of the kernels that the GPU
# engine compiles at run time. Built only with ENABLE_GPU, next to the target of the host.

set(LLVMNVPTXINFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/TargetInfo/NVPTXTargetInfo.cpp
)

set(LLVMNVPTXDESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/MCTargetDesc/NVPTXInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/MCTargetDesc/NVPTXMCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/MCTargetDesc/NVPTXMCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/MCTargetDesc/NVPTXTargetStreamer.cpp
)

set(LLVMNVPTXCODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXAliasAnalysis.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXAllocaHoisting.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXAsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXAssignValidGlobalNames.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXAtomicLower.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXCtorDtorLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXIRPeephole.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXForwardParams.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXFrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXGenericToNVVM.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXImageOptimizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXInstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXLowerAggrCopies.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXLowerAlloca.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXLowerArgs.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXLowerUnreachable.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXMCExpr.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXPeephole.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXPrologEpilogPass.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXProxyRegErasure.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXRegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXReplaceImageHandles.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXSelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXSubtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXTagInvariantLoads.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXTargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXTargetTransformInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVPTXUtilities.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVVMIntrRange.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/NVPTX/NVVMReflect.cpp
)
