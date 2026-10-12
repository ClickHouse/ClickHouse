# Source files for LLVMCoroutines
set(LLVMCOROUTINES_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroAnnotationElide.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroCleanup.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroConditionalWrapper.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroEarly.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroElide.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroFrame.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/Coroutines.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/CoroSplit.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/MaterializationUtils.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/SpillUtils.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/Coroutines/SuspendCrossingInfo.cpp
)
