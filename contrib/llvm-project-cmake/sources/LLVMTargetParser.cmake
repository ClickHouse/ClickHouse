# Source files for LLVMTargetParser
set(LLVMTARGETPARSER_SOURCES
    ${LLVM_SOURCE_DIR}/lib/TargetParser/AArch64TargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/ARMTargetParserCommon.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/ARMTargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/CSKYTargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/Host.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/LoongArchTargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/PPCTargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/RISCVISAInfo.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/RISCVTargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/SubtargetFeature.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/TargetDataLayout.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/TargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/Triple.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/X86TargetParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TargetParser/XtensaTargetParser.cpp
)
