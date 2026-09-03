# Source files for LLVMInstCombine
set(LLVMINSTCOMBINE_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineAddSub.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineAndOrXor.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineAtomicRMW.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineCalls.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineCasts.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineCompares.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineLoadStoreAlloca.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineMulDivRem.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineNegator.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombinePHI.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineSelect.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineShifts.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineSimplifyDemanded.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstCombineVectorOps.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/InstCombine/InstructionCombining.cpp
)
