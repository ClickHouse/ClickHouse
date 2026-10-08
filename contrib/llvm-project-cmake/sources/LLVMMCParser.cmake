# Source files for LLVMMCParser
set(LLVMMCPARSER_SOURCES
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/AsmLexer.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/AsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/COFFAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/COFFMasmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/DarwinAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/ELFAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/GOFFAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/MasmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/MCAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/MCAsmParserExtension.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/MCTargetAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/WasmAsmParser.cpp
    ${LLVM_SOURCE_DIR}/lib/MC/MCParser/XCOFFAsmParser.cpp
)
