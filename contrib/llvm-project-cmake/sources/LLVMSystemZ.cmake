# Source files for LLVM SystemZ target

# SystemZ Info sources
set(LLVMSYSTEMZINFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/TargetInfo/SystemZTargetInfo.cpp
)

# SystemZ Desc sources
set(LLVMSYSTEMZDESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZELFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZGNUInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZGOFFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZHLASMAsmStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZHLASMInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZInstPrinterCommon.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZMCAsmBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZMCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZMCCodeEmitter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZMCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/MCTargetDesc/SystemZTargetStreamer.cpp
)

# SystemZ CodeGen sources
set(LLVMSYSTEMZCODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZAsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZCallingConv.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZConstantPoolValue.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZCopyPhysRegs.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZElimCompare.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZFrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZHazardRecognizer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZInstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZLDCleanup.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZLongBranch.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZMachineFunctionInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZMachineScheduler.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZMCInstLower.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZPostRewrite.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZRegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZSelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZShortenInst.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZSubtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZTargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZTargetObjectFile.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZTargetTransformInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZTDC.cpp
)