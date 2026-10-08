# Source files for LLVMObjCARCOpts
set(LLVMOBJCARCOPTS_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/DependencyAnalysis.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ObjCARC.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ObjCARCContract.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ObjCARCExpand.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ObjCARCOpts.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ProvenanceAnalysis.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/ProvenanceAnalysisEvaluator.cpp
    ${LLVM_SOURCE_DIR}/lib/Transforms/ObjCARC/PtrState.cpp
)
