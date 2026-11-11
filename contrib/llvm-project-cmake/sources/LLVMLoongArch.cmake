# Source files for LLVM LoongArch target

# LoongArch Info sources
set(LLVMLOONGARCHINFO_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/TargetInfo/LoongArchTargetInfo.cpp
)

# LoongArch Desc sources
set(LLVMLOONGARCHDESC_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchAsmBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchBaseInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchELFObjectWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchELFStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchInstPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchMatInt.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchMCAsmInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchMCCodeEmitter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchMCTargetDesc.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/MCTargetDesc/LoongArchTargetStreamer.cpp
)

# LoongArch CodeGen sources
set(LLVMLOONGARCHCODEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchAsmPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchDeadRegisterDefinitions.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchExpandAtomicPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchExpandPseudoInsts.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchFrameLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchInstrInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchISelDAGToDAG.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchISelLowering.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchMCInstLower.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchMergeBaseOffset.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchOptWInstrs.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchRegisterInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchSelectionDAGInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchSubtarget.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchTargetMachine.cpp
    ${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArchTargetTransformInfo.cpp
)
