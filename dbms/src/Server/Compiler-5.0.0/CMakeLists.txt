set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")

add_library(clickhouse-compiler
    driver.cpp
    cc1_main.cpp
    cc1as_main.cpp
    lld.cpp)

target_compile_options(clickhouse-compiler PRIVATE -fno-rtti -fno-exceptions -g0)

llvm_map_components_to_libraries(REQUIRED_LLVM_LIBRARIES all)

# We link statically with zlib, and LLVM (sometimes) tries to bring its own dependency.
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-lz")

target_include_directories(clickhouse-compiler PRIVATE ${LLVM_INCLUDE_DIRS})

# This is extracted almost directly from CMakeFiles/.../link.txt in LLVM build directory.

target_link_libraries(clickhouse-compiler

clangBasic clangCodeGen clangDriver clangFrontend clangFrontendTool
clangRewriteFrontend clangARCMigrate clangStaticAnalyzerFrontend
clangParse clangSerialization clangSema clangEdit clangStaticAnalyzerCheckers
clangASTMatchers clangStaticAnalyzerCore clangAnalysis clangAST clangRewrite clangLex clangBasic

lldCOFF
lldDriver
lldELF
#lldMinGW
lldMachO
lldReaderWriter
lldYAML
#lldCommon
lldCore
lldConfig

${REQUIRED_LLVM_LIBRARIES}

LLVMSupport

#Polly
#PollyISL
#PollyPPCG

libtinfo.a
common)
