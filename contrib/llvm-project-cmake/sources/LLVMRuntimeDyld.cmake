# Source files for LLVMRuntimeDyld
set(LLVMRUNTIMEDYLD_SOURCES
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/JITSymbol.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RTDyldMemoryManager.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldChecker.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldCOFF.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RuntimeDyld.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldELF.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/RuntimeDyldMachO.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/RuntimeDyld/Targets/RuntimeDyldELFMips.cpp
)
