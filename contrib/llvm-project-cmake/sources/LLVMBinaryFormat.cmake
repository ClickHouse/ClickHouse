# Source files for LLVMBinaryFormat
set(LLVMBINARYFORMAT_SOURCES
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/AMDGPUMetadataVerifier.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/COFF.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/Dwarf.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/DXContainer.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/ELF.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/MachO.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/Magic.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/MsgPackDocument.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/MsgPackDocumentYAML.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/MsgPackReader.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/MsgPackWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/SFrame.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/Wasm.cpp
    ${LLVM_SOURCE_DIR}/lib/BinaryFormat/XCOFF.cpp
)
