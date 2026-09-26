# Source files for LLVMSandboxIR
set(LLVMSANDBOXIR_SOURCES
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Argument.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/BasicBlock.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Constant.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Context.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Function.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Instruction.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Module.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Pass.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/PassManager.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Region.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Tracker.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Type.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Use.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/User.cpp
    ${LLVM_SOURCE_DIR}/lib/SandboxIR/Value.cpp
)
