# Source files for LLVMTextAPI
set(LLVMTEXTAPI_SOURCES
    ${LLVM_SOURCE_DIR}/lib/TextAPI/Architecture.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/ArchitectureSet.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/InterfaceFile.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/TextStubV5.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/PackedVersion.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/Platform.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/RecordsSlice.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/RecordVisitor.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/Symbol.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/SymbolSet.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/Target.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/TextAPIError.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/TextStub.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/TextStubCommon.cpp
    ${LLVM_SOURCE_DIR}/lib/TextAPI/Utils.cpp
)
