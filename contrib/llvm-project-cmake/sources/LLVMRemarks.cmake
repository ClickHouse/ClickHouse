# Source files for LLVMRemarks
set(LLVMREMARKS_SOURCES
    ${LLVM_SOURCE_DIR}/lib/Remarks/BitstreamRemarkParser.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/BitstreamRemarkSerializer.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/Remark.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkFormat.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkLinker.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkParser.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkSerializer.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkStreamer.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/RemarkStringTable.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/YAMLRemarkParser.cpp
    ${LLVM_SOURCE_DIR}/lib/Remarks/YAMLRemarkSerializer.cpp
)
