# Source files for LLVMExecutionEngine
set(LLVMEXECUTIONENGINE_SOURCES
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/ExecutionEngineBindings.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/ExecutionEngine.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/GDBRegistrationListener.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/SectionMemoryManager.cpp
    ${LLVM_SOURCE_DIR}/lib/ExecutionEngine/TargetSelect.cpp
)
