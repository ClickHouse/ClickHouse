add_definitions(-Wno-error -Wno-unused-parameter -Wno-non-virtual-dtor -U_LIBCPP_DEBUG)

add_library(clickhouse-compiler-lib
    driver.cpp
    cc1_main.cpp
    cc1as_main.cpp
    lld.cpp)

target_compile_options(clickhouse-compiler-lib PRIVATE -fno-rtti -fno-exceptions -g0)

llvm_map_components_to_libnames(REQUIRED_LLVM_LIBRARIES all)
if (TERMCAP_LIBRARY)
    list(APPEND REQUIRED_LLVM_LIBRARIES ${TERMCAP_LIBRARY})
endif ()
list(APPEND REQUIRED_LLVM_LIBRARIES ${CMAKE_DL_LIBS})


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
clangCrossTU clangIndex

lldCOFF
lldDriver
lldELF
lldMinGW
lldMachO
lldReaderWriter
lldYAML
lldCommon
lldCore

${REQUIRED_LLVM_LIBRARIES}

PUBLIC ${ZLIB_LIBRARIES} ${EXECINFO_LIBRARY} Threads::Threads
)
