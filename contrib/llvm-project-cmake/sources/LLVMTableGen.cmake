# Source files for LLVMTableGen
set(LLVMTABLEGEN_SOURCES
    ${LLVM_SOURCE_DIR}/lib/TableGen/DetailedRecordsBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/Error.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/JSONBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/Main.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/Parser.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/Record.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/SetTheory.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/StringMatcher.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/StringToOffsetTable.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/TableGenBackend.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/TableGenBackendSkeleton.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/TGLexer.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/TGParser.cpp
    ${LLVM_SOURCE_DIR}/lib/TableGen/TGTimer.cpp
)
