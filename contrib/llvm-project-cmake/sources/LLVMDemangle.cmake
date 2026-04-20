# Source files for LLVMDemangle
set(LLVMDEMANGLE_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Demangle/Demangle.cpp
    ${LLVM_SOURCE_DIR}/lib/Demangle/DLangDemangle.cpp
    ${LLVM_SOURCE_DIR}/lib/Demangle/ItaniumDemangle.cpp
    ${LLVM_SOURCE_DIR}/lib/Demangle/MicrosoftDemangle.cpp
    ${LLVM_SOURCE_DIR}/lib/Demangle/MicrosoftDemangleNodes.cpp
    ${LLVM_SOURCE_DIR}/lib/Demangle/RustDemangle.cpp
)
