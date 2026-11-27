# Source files for LLVMXRay
set(LLVMXRAY_SOURCES
    ${LLVM_SOURCE_DIR}/lib/XRay/BlockIndexer.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/BlockPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/BlockVerifier.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/FDRRecordProducer.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/FDRRecords.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/FDRTraceExpander.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/FDRTraceWriter.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/FileHeaderReader.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/InstrumentationMap.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/LogBuilderConsumer.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/Profile.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/RecordInitializer.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/RecordPrinter.cpp
    ${LLVM_SOURCE_DIR}/lib/XRay/Trace.cpp
)
