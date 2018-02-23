set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error")

add_library(clickhouse-compiler-lib
    driver.cpp
    cc1_main.cpp
    cc1as_main.cpp
    lld.cpp)

target_compile_options(clickhouse-compiler-lib PRIVATE -fno-rtti -fno-exceptions -g0)

llvm_map_components_to_libraries(REQUIRED_LLVM_LIBRARIES all)

# We link statically with zlib, and LLVM (sometimes) tries to bring its own dependency.
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-lz")
# Wrong library in freebsd:
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-l/usr/lib/libexecinfo.so")

message(STATUS "Using LLVM ${LLVM_VERSION}: ${LLVM_INCLUDE_DIRS} : ${REQUIRED_LLVM_LIBRARIES}")

target_include_directories(clickhouse-compiler-lib PRIVATE ${LLVM_INCLUDE_DIRS})

# This is extracted almost directly from CMakeFiles/.../link.txt in LLVM build directory.

target_link_libraries(clickhouse-compiler-lib PRIVATE

clangBasic clangCodeGen clangDriver
clangFrontend 
clangFrontendTool
clangRewriteFrontend clangARCMigrate clangStaticAnalyzerFrontend
clangParse clangSerialization clangSema clangEdit clangStaticAnalyzerCheckers
clangASTMatchers clangStaticAnalyzerCore clangAnalysis clangAST clangRewrite clangLex clangBasic

lldCOFF
lldDriver
lldELF
lldMinGW
lldMachO
lldReaderWriter
lldYAML
lldCommon
lldCore
#lldWasm

${REQUIRED_LLVM_LIBRARIES}

#Polly
#PollyISL
#PollyPPCG

libtinfo.a

PUBLIC ${ZLIB_LIBRARIES} ${EXECINFO_LIBRARY} Threads::Threads
)
