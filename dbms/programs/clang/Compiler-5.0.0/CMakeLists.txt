add_definitions(-Wno-error -Wno-unused-parameter -Wno-non-virtual-dtor -U_LIBCPP_DEBUG)

add_library(clickhouse-compiler-lib ${LINK_MODE}
    driver.cpp
    cc1_main.cpp
    cc1as_main.cpp
    lld.cpp)

target_compile_options(clickhouse-compiler-lib PRIVATE -fno-rtti -fno-exceptions -g0)

string(REPLACE "${INCLUDE_DEBUG_HELPERS}" "" CMAKE_CXX_FLAGS ${CMAKE_CXX_FLAGS}) # cant compile with -fno-rtti

llvm_libs_all(REQUIRED_LLVM_LIBRARIES)

message(STATUS "Using LLVM ${LLVM_VERSION}: ${LLVM_INCLUDE_DIRS} : ${REQUIRED_LLVM_LIBRARIES}")

target_include_directories(clickhouse-compiler-lib SYSTEM PRIVATE ${LLVM_INCLUDE_DIRS})

# This is extracted almost directly from CMakeFiles/.../link.txt in LLVM build directory.

target_link_libraries(clickhouse-compiler-lib PRIVATE

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

PUBLIC ${ZLIB_LIBRARIES} ${EXECINFO_LIBRARY} Threads::Threads
${MALLOC_LIBRARIES}
${GLIBC_COMPATIBILITY_LIBRARIES}
${MEMCPY_LIBRARIES}
)
