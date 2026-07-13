# Lean LLVM build for ClickHouse
#
# Builds LLVM libraries without including LLVM's own CMake definitions.
# Uses llvm-tblgen built from sources to generate .inc files at build time.

if (SANITIZE STREQUAL "memory")
    set (ENABLE_EMBEDDED_COMPILER_DEFAULT OFF)
    set (ENABLE_DWARF_PARSER_DEFAULT OFF)
else()
    set (ENABLE_EMBEDDED_COMPILER_DEFAULT ${ENABLE_LIBRARIES})
    set (ENABLE_DWARF_PARSER_DEFAULT ${ENABLE_LIBRARIES})
endif()

option (ENABLE_EMBEDDED_COMPILER "Enable JIT compilation" ${ENABLE_EMBEDDED_COMPILER_DEFAULT})
option (ENABLE_DWARF_PARSER "Enable DWARF input format" ${ENABLE_DWARF_PARSER_DEFAULT})
option (ENABLE_BLAKE3 "Enable BLAKE3 function" ${ENABLE_LIBRARIES})

if (NOT ENABLE_EMBEDDED_COMPILER AND NOT ENABLE_DWARF_PARSER AND NOT ENABLE_BLAKE3)
    message(STATUS "Not using LLVM")
    return()
endif()

set (LLVM_VERSION "22.1.1bundled")

# Directories
set (LLVM_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/llvm")
set (LLVM_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/llvm-project/llvm")
set (LLVM_CMAKE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

set (LLVM_INCLUDE_DIRS
    "${LLVM_SOURCE_DIR}/include"
    "${LLVM_BINARY_DIR}/include"
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/third-party/siphash/include"
)
if (ENABLE_XRAY)
    list(APPEND LLVM_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/compiler-rt/include")
endif()

# Target architecture
if (ARCH_AMD64)
    set (LLVM_TARGET_ARCH "X86")
    set (LLVM_HOST_TRIPLE_DEFAULT "x86_64-unknown-linux-gnu")
elseif (ARCH_AARCH64)
    set (LLVM_TARGET_ARCH "AArch64")
    set (LLVM_HOST_TRIPLE_DEFAULT "aarch64-unknown-linux-gnu")
elseif (ARCH_PPC64LE)
    set (LLVM_TARGET_ARCH "PowerPC")
    set (LLVM_HOST_TRIPLE_DEFAULT "powerpc64le-unknown-linux-gnu")
elseif (ARCH_S390X)
    set (LLVM_TARGET_ARCH "SystemZ")
    set (LLVM_HOST_TRIPLE_DEFAULT "s390x-unknown-linux-gnu")
elseif (ARCH_RISCV64)
    set (LLVM_TARGET_ARCH "RISCV")
    set (LLVM_HOST_TRIPLE_DEFAULT "riscv64-unknown-linux-gnu")
elseif (ARCH_LOONGARCH64)
    set (LLVM_TARGET_ARCH "LoongArch")
    set (LLVM_HOST_TRIPLE_DEFAULT "loongarch64-unknown-linux-gnu")
else()
    message(FATAL_ERROR "Unsupported architecture for LLVM")
endif()

# Use the compiler target triple when cross-compiling (handles musl, macOS, FreeBSD, etc.)
if (CMAKE_CROSSCOMPILING AND CMAKE_C_COMPILER_TARGET)
    set (LLVM_HOST_TRIPLE "${CMAKE_C_COMPILER_TARGET}")
else()
    set (LLVM_HOST_TRIPLE "${LLVM_HOST_TRIPLE_DEFAULT}")
endif()

message(STATUS "LLVM target: ${LLVM_TARGET_ARCH}")

# LLVM has issues with C++23
set (CMAKE_CXX_STANDARD 17)

# Needed for musl compatibility
add_compile_definitions(_LARGEFILE64_SOURCE)

# BLAKE3 x86 assembly files
if (ARCH_AMD64)
    set(LLVM_BLAKE3_ASM_SOURCES
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_sse2_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_sse41_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx2_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx512_x86-64_unix.S
    )
    set_source_files_properties(${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx512_x86-64_unix.S
        PROPERTIES COMPILE_OPTIONS "-mavx512f;-mavx512vl")
else()
    set(LLVM_BLAKE3_ASM_SOURCES "")
    add_compile_definitions(BLAKE3_NO_AVX512 BLAKE3_NO_AVX2 BLAKE3_NO_SSE41 BLAKE3_NO_SSE2)
endif()

# Include source file lists
include(${LLVM_CMAKE_DIR}/sources/LLVMDemangle.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMSupport.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMTableGen.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMBinaryFormat.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMTargetParser.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCore.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMMC.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMMCParser.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMMCDisassembler.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMBitReader.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMBitstreamReader.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMBitWriter.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMRemarks.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMDebugInfoCodeView.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMDebugInfoDWARF.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMObject.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMTextAPI.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMObjectYAML.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMAsmParser.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMIRReader.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMProfileData.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMAnalysis.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMTransformUtils.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMLinker.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMTarget.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMScalarOpts.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMInstCombine.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMAggressiveInstCombine.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMVectorize.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMipo.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMInstrumentation.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCodeGen.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCodeGenTypes.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMGlobalISel.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMSelectionDAG.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMAsmPrinter.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCGData.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMObjCARCOpts.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMIRPrinter.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCFGuard.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMFrontendOpenMP.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMFrontendAtomic.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMFrontendOffloading.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMFrontendHLSL.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMCoroutines.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMHipStdPar.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMSandboxIR.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMPasses.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMExecutionEngine.cmake)
include(${LLVM_CMAKE_DIR}/sources/LLVMRuntimeDyld.cmake)

if (ARCH_AMD64)
    include(${LLVM_CMAKE_DIR}/sources/LLVMX86.cmake)
elseif (ARCH_AARCH64)
    include(${LLVM_CMAKE_DIR}/sources/LLVMAArch64.cmake)
elseif (ARCH_PPC64LE)
    include(${LLVM_CMAKE_DIR}/sources/LLVMPowerPC.cmake)
elseif (ARCH_S390X)
    include(${LLVM_CMAKE_DIR}/sources/LLVMSystemZ.cmake)
elseif (ARCH_RISCV64)
    include(${LLVM_CMAKE_DIR}/sources/LLVMRISCV.cmake)
elseif (ARCH_LOONGARCH64)
    include(${LLVM_CMAKE_DIR}/sources/LLVMLoongArch.cmake)
endif()

if (ENABLE_XRAY)
    include(${LLVM_CMAKE_DIR}/sources/LLVMXRay.cmake)
endif()

#
# Generate config headers
#
file(MAKE_DIRECTORY "${LLVM_BINARY_DIR}/include/llvm/Config")
file(MAKE_DIRECTORY "${LLVM_BINARY_DIR}/include/llvm/IR")
file(MAKE_DIRECTORY "${LLVM_BINARY_DIR}/include/llvm/CodeGen")

#
# Platform-specific config variables
#
# These variables control platform-specific behavior in config.h.in
# The target platform is determined by CMAKE_SYSTEM_NAME when cross-compiling
#

# Shared library extension
if (APPLE)
    set(LLVM_SHLIB_EXT ".dylib")
else()
    set(LLVM_SHLIB_EXT ".so")
endif()

# mallinfo is only available on Linux with glibc (not musl, not BSD, not macOS)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND NOT USE_MUSL)
    set(HAVE_MALLINFO_DEFINE "#define HAVE_MALLINFO 1")
else()
    set(HAVE_MALLINFO_DEFINE "/* #undef HAVE_MALLINFO */")
endif()

# backtrace/execinfo.h is available on glibc and macOS/BSD, but NOT on musl
if (USE_MUSL)
    set(HAVE_BACKTRACE_DEFINE "/* #undef HAVE_BACKTRACE */")
    set(BACKTRACE_HEADER_DEFINE "/* No backtrace header on musl */")
else()
    set(HAVE_BACKTRACE_DEFINE "#define HAVE_BACKTRACE 1")
    set(BACKTRACE_HEADER_DEFINE "#define BACKTRACE_HEADER <execinfo.h>")
endif()

# getauxval is Linux-specific
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(HAVE_GETAUXVAL_DEFINE "#define HAVE_GETAUXVAL 1")
    set(HAVE_BUILTIN_THREAD_POINTER_DEFINE "#define HAVE_BUILTIN_THREAD_POINTER 1")
else()
    set(HAVE_GETAUXVAL_DEFINE "/* #undef HAVE_GETAUXVAL */")
    set(HAVE_BUILTIN_THREAD_POINTER_DEFINE "/* #undef HAVE_BUILTIN_THREAD_POINTER */")
endif()

# macOS-specific headers and functions
if (APPLE)
    set(HAVE_MACH_MACH_H_DEFINE "#define HAVE_MACH_MACH_H 1")
    set(HAVE_MALLOC_MALLOC_H_DEFINE "#define HAVE_MALLOC_MALLOC_H 1")
    set(HAVE_MALLOC_ZONE_STATISTICS_DEFINE "#define HAVE_MALLOC_ZONE_STATISTICS 1")
else()
    set(HAVE_MACH_MACH_H_DEFINE "/* #undef HAVE_MACH_MACH_H */")
    set(HAVE_MALLOC_MALLOC_H_DEFINE "/* #undef HAVE_MALLOC_MALLOC_H */")
    set(HAVE_MALLOC_ZONE_STATISTICS_DEFINE "/* #undef HAVE_MALLOC_ZONE_STATISTICS */")
endif()

# stat struct: st_mtim (Linux) vs st_mtimespec (macOS/BSD)
if (APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    set(HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC_DEFINE "#define HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 1")
    set(HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC_DEFINE "/* #undef HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC */")
else()
    set(HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC_DEFINE "/* #undef HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC */")
    set(HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC_DEFINE "#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1")
endif()

# arc4random is available on macOS and BSD
if (APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    set(HAVE_DECL_ARC4RANDOM_DECL_DEFINE "#define HAVE_DECL_ARC4RANDOM 1")
    set(HAVE_DECL_ARC4RANDOM_DEFINE "#define HAVE_DECL_ARC4RANDOM 1")
else()
    set(HAVE_DECL_ARC4RANDOM_DECL_DEFINE "#define HAVE_DECL_ARC4RANDOM 0")
    set(HAVE_DECL_ARC4RANDOM_DEFINE "/* #undef HAVE_DECL_ARC4RANDOM */")
endif()

configure_file(
    "${LLVM_CMAKE_DIR}/config/llvm-config.h.in"
    "${LLVM_BINARY_DIR}/include/llvm/Config/llvm-config.h"
    @ONLY
)

configure_file(
    "${LLVM_CMAKE_DIR}/config/config.h.in"
    "${LLVM_BINARY_DIR}/include/llvm/Config/config.h"
    @ONLY
)

configure_file(
    "${LLVM_CMAKE_DIR}/config/abi-breaking.h.in"
    "${LLVM_BINARY_DIR}/include/llvm/Config/abi-breaking.h"
    @ONLY
)

# Target definition files
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/Targets.def"
    "LLVM_TARGET(${LLVM_TARGET_ARCH})\n")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/AsmPrinters.def"
    "LLVM_ASM_PRINTER(${LLVM_TARGET_ARCH})\n")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/AsmParsers.def"
    "LLVM_ASM_PARSER(${LLVM_TARGET_ARCH})\n")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/Disassemblers.def"
    "LLVM_DISASSEMBLER(${LLVM_TARGET_ARCH})\n")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/TargetMCAs.def" "")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Config/TargetExegesis.def" "")

# VCSRevision.h - LLVM version control info (normally generated by LLVM CMake)
file(MAKE_DIRECTORY "${LLVM_BINARY_DIR}/include/llvm/Support")
file(WRITE "${LLVM_BINARY_DIR}/include/llvm/Support/VCSRevision.h"
    "#define LLVM_REVISION \"\"\n#define LLVM_REPOSITORY \"\"\n")

#
# Common compile definitions
#
set(LLVM_COMPILE_DEFINITIONS
    __STDC_CONSTANT_MACROS
    __STDC_FORMAT_MACROS
    __STDC_LIMIT_MACROS
    LLVM_ENABLE_THREADS=1
    LLVM_HAS_ATOMICS=1
    LLVM_ENABLE_EH=1
    LLVM_ENABLE_RTTI=1
)

#
# Cross-compilation support for tablegen tools
#
# When cross-compiling, we need to build tablegen for the HOST system (not target).
# The tablegen executables must be runnable on the build machine to generate .inc files.
#
# We detect cross-compilation by checking CMAKE_CROSSCOMPILING.
#
if (CMAKE_CROSSCOMPILING)
    message(STATUS "LLVM: Cross-compiling detected, building native tablegen tools")

    include(ExternalProject)

    set(NATIVE_TBLGEN_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/native-tblgen")

    # Build native tablegen tools using ExternalProject
    # This runs a separate CMake configuration WITHOUT the target toolchain,
    # so it builds for the host system.
    #
    # IMPORTANT: We do NOT pass CMAKE_C_COMPILER/CMAKE_CXX_COMPILER here.
    # The toolchain file sets sysroot and gcc-toolchain flags that make binaries
    # link against target system libraries, not host libraries.
    # By not specifying compilers, CMake will use the default host compilers.
    ExternalProject_Add(native-tblgen-build
        SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/native-tblgen"
        BINARY_DIR "${NATIVE_TBLGEN_BUILD_DIR}"
        CMAKE_ARGS
            -DLLVM_SOURCE_DIR=${LLVM_SOURCE_DIR}
            -DLLVM_NATIVE_OUTPUT_DIR=${NATIVE_TBLGEN_BUILD_DIR}
            -DCMAKE_BUILD_TYPE=Release
        # Do NOT pass toolchain file or compilers - let CMake find host compilers
        BUILD_ALWAYS OFF
        INSTALL_COMMAND ""
        CMAKE_GENERATOR "${CMAKE_GENERATOR}"
    )

    # Paths to native tablegen executables
    # Note: native-tblgen builds for the host (not the cross-compilation target),
    # so we do not append CMAKE_EXECUTABLE_SUFFIX (which is the target's suffix).
    set(LLVM_TABLEGEN_EXE "${NATIVE_TBLGEN_BUILD_DIR}/llvm-tblgen")
    set(LLVM_MIN_TABLEGEN_EXE "${NATIVE_TBLGEN_BUILD_DIR}/llvm-min-tblgen")

    # Create imported targets for native tablegen
    add_executable(llvm-tblgen-native IMPORTED GLOBAL)
    set_target_properties(llvm-tblgen-native PROPERTIES IMPORTED_LOCATION "${LLVM_TABLEGEN_EXE}")
    add_dependencies(llvm-tblgen-native native-tblgen-build)

    add_executable(llvm-min-tblgen-native IMPORTED GLOBAL)
    set_target_properties(llvm-min-tblgen-native PROPERTIES IMPORTED_LOCATION "${LLVM_MIN_TABLEGEN_EXE}")
    add_dependencies(llvm-min-tblgen-native native-tblgen-build)

    # We still need dummy targets for non-cross builds that expect these
    # But tablegen commands will use the imported targets
    set(TABLEGEN_TARGET llvm-tblgen-native)
    set(MIN_TABLEGEN_TARGET llvm-min-tblgen-native)
    set(USE_NATIVE_TABLEGEN ON)

else()
    # Native build - build tablegen as part of the normal build
    message(STATUS "LLVM: Native build, building tablegen inline")
    set(USE_NATIVE_TABLEGEN OFF)
endif()

#
# Build llvm-min-tblgen and llvm-tblgen (only for native builds)
# For cross-compilation, these are built by ExternalProject above.
#
if (NOT USE_NATIVE_TABLEGEN)

add_executable(llvm-min-tblgen
    # Main entry point
    ${LLVM_SOURCE_DIR}/utils/TableGen/llvm-min-tblgen.cpp
    # TableGen Basic backends (minimal set, no CodeGenTypes dependency)
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/ARMTargetDefEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/Attributes.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/CodeGenIntrinsics.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/DirectiveEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/IntrinsicEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RISCVTargetDefEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RuntimeLibcalls.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RuntimeLibcallsEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/SDNodeProperties.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TableGen.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TargetFeaturesEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TargetLibraryInfoEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/VTEmitter.cpp
    # TableGen library
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
    # Support library
    ${LLVMSUPPORT_SOURCES}
    ${LLVM_BLAKE3_ASM_SOURCES}
    ${LLVMDEMANGLE_SOURCES}
)

target_include_directories(llvm-min-tblgen PRIVATE
    ${LLVM_SOURCE_DIR}/include
    ${LLVM_BINARY_DIR}/include
    ${LLVM_SOURCE_DIR}/utils/TableGen
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/third-party/siphash/include"
)

target_compile_definitions(llvm-min-tblgen PRIVATE ${LLVM_COMPILE_DEFINITIONS})

# On Linux with glibc, link with pthread and dl
# musl has these in libc, so no separate libraries needed
if (UNIX AND NOT APPLE AND NOT USE_MUSL)
    target_link_libraries(llvm-min-tblgen PRIVATE pthread dl)
elseif (UNIX AND NOT APPLE)
    target_link_libraries(llvm-min-tblgen PRIVATE pthread)
endif()

#
# Generate GenVT.inc using llvm-min-tblgen (needed for CodeGenTypes which llvm-tblgen uses)
# This must be done before building llvm-tblgen
#
file(MAKE_DIRECTORY "${LLVM_BINARY_DIR}/include/llvm/CodeGen")
add_custom_command(
    OUTPUT "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
    COMMAND $<TARGET_FILE:llvm-min-tblgen>
        -gen-vt
        -I "${LLVM_SOURCE_DIR}/include"
        "${LLVM_SOURCE_DIR}/include/llvm/CodeGen/ValueTypes.td"
        -o "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
    DEPENDS llvm-min-tblgen "${LLVM_SOURCE_DIR}/include/llvm/CodeGen/ValueTypes.td"
    COMMENT "TableGen (min): GenVT.inc"
    VERBATIM
)
add_custom_target(llvm_gen_vt DEPENDS "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc")

#
# Build full llvm-tblgen (depends on GenVT.inc via CodeGenTypes)
#
add_executable(llvm-tblgen
    # Main entry point
    ${LLVM_SOURCE_DIR}/utils/TableGen/llvm-tblgen.cpp
    # TableGen Basic backends
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/ARMTargetDefEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/Attributes.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/CodeGenIntrinsics.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/DirectiveEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/IntrinsicEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RISCVTargetDefEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RuntimeLibcalls.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/RuntimeLibcallsEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/SDNodeProperties.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TableGen.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TargetFeaturesEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/TargetLibraryInfoEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Basic/VTEmitter.cpp
    # TableGen Common backends
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/CodeExpander.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/CombinerUtils.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/CXXPredicates.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/GlobalISelMatchTable.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/GlobalISelMatchTableExecutorEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/PatternParser.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/GlobalISel/Patterns.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/AsmWriterInst.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenDAGPatterns.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenHwModes.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenInstAlias.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenInstruction.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenRegisters.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenSchedule.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/CodeGenTarget.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/InfoByHwMode.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/InstructionEncoding.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/OptEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/PredicateExpander.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/SubtargetFeatureInfo.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/Types.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/Utils.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/Common/VarLenCodeEmitterGen.cpp
    # TableGen full backends
    ${LLVM_SOURCE_DIR}/utils/TableGen/AsmMatcherEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/AsmWriterEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/CallingConvEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/CodeEmitterGen.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/CodeGenMapTable.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/CompressInstEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/CTagsEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DAGISelEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DAGISelMatcher.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DAGISelMatcherEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DAGISelMatcherGen.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DAGISelMatcherOpt.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DecoderEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DecoderTableEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DecoderTree.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DFAEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DFAPacketizerEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DisassemblerEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/DXILEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/ExegesisEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/FastISelEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/GlobalISelCombinerEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/GlobalISelEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/InstrDocsEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/InstrInfoEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/MacroFusionPredicatorEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/OptionParserEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/OptionRSTEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/PseudoLoweringEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/RegisterBankEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/RegisterInfoEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/SDNodeInfoEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/SearchableTableEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/SubtargetEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/WebAssemblyDisassemblerEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86InstrMappingEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86DisassemblerTables.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86FoldTablesEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86MnemonicTables.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86ModRMFilters.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86RecognizableInstr.cpp
    # TableGen library
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
    # CodeGenTypes library (needs GenVT.inc)
    ${LLVMCODEGENTYPES_SOURCES}
    # Support library
    ${LLVMSUPPORT_SOURCES}
    ${LLVM_BLAKE3_ASM_SOURCES}
    ${LLVMDEMANGLE_SOURCES}
)

# llvm-tblgen needs GenVT.inc which is generated by llvm-min-tblgen
add_dependencies(llvm-tblgen llvm_gen_vt)

target_include_directories(llvm-tblgen PRIVATE
    ${LLVM_SOURCE_DIR}/include
    ${LLVM_BINARY_DIR}/include
    ${LLVM_SOURCE_DIR}/lib/Target
    ${LLVM_SOURCE_DIR}/utils/TableGen
    "${ClickHouse_SOURCE_DIR}/contrib/llvm-project/third-party/siphash/include"
)

target_compile_definitions(llvm-tblgen PRIVATE ${LLVM_COMPILE_DEFINITIONS})

# On Linux with glibc, link with pthread and dl
# musl has these in libc, so no separate libraries needed
if (UNIX AND NOT APPLE AND NOT USE_MUSL)
    target_link_libraries(llvm-tblgen PRIVATE pthread dl)
elseif (UNIX AND NOT APPLE)
    target_link_libraries(llvm-tblgen PRIVATE pthread)
endif()

# End of native tablegen build block
endif() # NOT USE_NATIVE_TABLEGEN

#
# Set up tablegen executable variables
#
if (USE_NATIVE_TABLEGEN)
    # For cross-compilation: use native (host) tablegen
    set(LLVM_TABLEGEN_DEPENDS native-tblgen-build)
    # The executable path is set above as LLVM_TABLEGEN_EXE
else()
    # For native compilation: use the tablegen we just built
    set(LLVM_TABLEGEN_EXE "$<TARGET_FILE:llvm-tblgen>")
    set(LLVM_MIN_TABLEGEN_EXE "$<TARGET_FILE:llvm-min-tblgen>")
    set(LLVM_TABLEGEN_DEPENDS llvm-tblgen)
endif()

#
# For cross-compilation: Copy GenVT.inc from native build
# (For native compilation, llvm_gen_vt is defined inside the if (NOT USE_NATIVE_TABLEGEN) block above)
#
if (USE_NATIVE_TABLEGEN)
    set(NATIVE_GENVT_INC "${NATIVE_TBLGEN_BUILD_DIR}/include/llvm/CodeGen/GenVT.inc")

    # Copy GenVT.inc from native build to our include dir
    add_custom_command(
        OUTPUT "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
        COMMAND ${CMAKE_COMMAND} -E copy
            "${NATIVE_GENVT_INC}"
            "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
        DEPENDS native-tblgen-build
        COMMENT "Copying native GenVT.inc"
        VERBATIM
    )
    add_custom_target(llvm_gen_vt DEPENDS "${LLVM_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc")
endif()

#
# TableGen macros
#
# Note: GEN_FLAGS should be passed as a CMake list (semicolon-separated) for multiple flags
macro(tablegen_generate TARGET_NAME TD_FILE GEN_FLAGS OUTPUT_FILE)
    set(OUTPUT_PATH "${LLVM_BINARY_DIR}/${OUTPUT_FILE}")
    get_filename_component(OUTPUT_DIR "${OUTPUT_PATH}" DIRECTORY)
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")

    add_custom_command(
        OUTPUT "${OUTPUT_PATH}"
        COMMAND ${LLVM_TABLEGEN_EXE}
            ${GEN_FLAGS}
            -I "${LLVM_SOURCE_DIR}/include"
            -I "${LLVM_SOURCE_DIR}/lib/Target/${LLVM_TARGET_ARCH}"
            "${TD_FILE}"
            -o "${OUTPUT_PATH}"
        DEPENDS ${LLVM_TABLEGEN_DEPENDS} "${TD_FILE}"
        COMMENT "TableGen: ${OUTPUT_FILE}"
        COMMAND_EXPAND_LISTS
    )
    list(APPEND ${TARGET_NAME}_TABLEGEN_OUTPUTS "${OUTPUT_PATH}")
endmacro()

#
# Generate common TableGen outputs
#
tablegen_generate(COMMON
    "${LLVM_SOURCE_DIR}/include/llvm/IR/Intrinsics.td"
    "-gen-intrinsic-enums"
    "include/llvm/IR/IntrinsicEnums.inc"
)

tablegen_generate(COMMON
    "${LLVM_SOURCE_DIR}/include/llvm/IR/Intrinsics.td"
    "-gen-intrinsic-impl"
    "include/llvm/IR/IntrinsicImpl.inc"
)

tablegen_generate(COMMON
    "${LLVM_SOURCE_DIR}/include/llvm/IR/Attributes.td"
    "-gen-attrs"
    "include/llvm/IR/Attributes.inc"
)

tablegen_generate(COMMON
    "${LLVM_SOURCE_DIR}/include/llvm/IR/RuntimeLibcalls.td"
    "-gen-runtime-libcalls"
    "include/llvm/IR/RuntimeLibcalls.inc"
)

tablegen_generate(COMMON
    "${LLVM_SOURCE_DIR}/include/llvm/Analysis/TargetLibraryInfo.td"
    "-gen-target-library-info"
    "include/llvm/Analysis/TargetLibraryInfo.inc"
)

# Note: GenVT.inc is generated by llvm-min-tblgen earlier in the build process

#
# Intrinsics headers for all architectures (needed by LLVMCore even when building for single arch)
#
set(INTRINSICS_TD "${LLVM_SOURCE_DIR}/include/llvm/IR/Intrinsics.td")

tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=aarch64" "include/llvm/IR/IntrinsicsAArch64.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=amdgcn" "include/llvm/IR/IntrinsicsAMDGPU.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=arm" "include/llvm/IR/IntrinsicsARM.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=bpf" "include/llvm/IR/IntrinsicsBPF.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=dx" "include/llvm/IR/IntrinsicsDirectX.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=hexagon" "include/llvm/IR/IntrinsicsHexagon.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=loongarch" "include/llvm/IR/IntrinsicsLoongArch.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=mips" "include/llvm/IR/IntrinsicsMips.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=nvvm" "include/llvm/IR/IntrinsicsNVPTX.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=ppc" "include/llvm/IR/IntrinsicsPowerPC.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=r600" "include/llvm/IR/IntrinsicsR600.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=riscv" "include/llvm/IR/IntrinsicsRISCV.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=spv" "include/llvm/IR/IntrinsicsSPIRV.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=s390" "include/llvm/IR/IntrinsicsS390.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=wasm" "include/llvm/IR/IntrinsicsWebAssembly.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=x86" "include/llvm/IR/IntrinsicsX86.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=xcore" "include/llvm/IR/IntrinsicsXCore.h")
tablegen_generate(COMMON "${INTRINSICS_TD}" "-gen-intrinsic-enums;-intrinsic-prefix=ve" "include/llvm/IR/IntrinsicsVE.h")

#
# Frontend tablegen outputs (OpenMP, OpenACC)
#
set(OMP_TD "${LLVM_SOURCE_DIR}/include/llvm/Frontend/OpenMP/OMP.td")
tablegen_generate(COMMON "${OMP_TD}" "-gen-directive-decl" "include/llvm/Frontend/OpenMP/OMP.h.inc")
tablegen_generate(COMMON "${OMP_TD}" "-gen-directive-impl" "include/llvm/Frontend/OpenMP/OMP.inc")

#
# TargetParser tablegen outputs (needed regardless of which arch we're building for)
#
# For these we need a modified tablegen_generate that uses different include dirs
# Note: GEN_FLAGS should be passed as a CMake list (semicolon-separated) for multiple flags
macro(tablegen_generate_target_parser TARGET_NAME TD_FILE GEN_FLAGS EXTRA_INCLUDES OUTPUT_FILE)
    set(OUTPUT_PATH "${LLVM_BINARY_DIR}/${OUTPUT_FILE}")
    get_filename_component(OUTPUT_DIR "${OUTPUT_PATH}" DIRECTORY)
    file(MAKE_DIRECTORY "${OUTPUT_DIR}")

    add_custom_command(
        OUTPUT "${OUTPUT_PATH}"
        COMMAND ${LLVM_TABLEGEN_EXE}
            ${GEN_FLAGS}
            -I "${LLVM_SOURCE_DIR}/include"
            -I "${EXTRA_INCLUDES}"
            "${TD_FILE}"
            -o "${OUTPUT_PATH}"
        DEPENDS ${LLVM_TABLEGEN_DEPENDS} "${TD_FILE}"
        COMMENT "TableGen: ${OUTPUT_FILE}"
        COMMAND_EXPAND_LISTS
    )
    list(APPEND ${TARGET_NAME}_TABLEGEN_OUTPUTS "${OUTPUT_PATH}")
endmacro()

# ARM target parser def
tablegen_generate_target_parser(COMMON
    "${LLVM_SOURCE_DIR}/lib/Target/ARM/ARM.td"
    "-gen-arm-target-def"
    "${LLVM_SOURCE_DIR}/lib/Target/ARM"
    "include/llvm/TargetParser/ARMTargetParserDef.inc"
)

# AArch64 target parser def
tablegen_generate_target_parser(COMMON
    "${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64.td"
    "-gen-arm-target-def"
    "${LLVM_SOURCE_DIR}/lib/Target/AArch64"
    "include/llvm/TargetParser/AArch64TargetParserDef.inc"
)

# RISCV target parser def
tablegen_generate_target_parser(COMMON
    "${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCV.td"
    "-gen-riscv-target-def"
    "${LLVM_SOURCE_DIR}/lib/Target/RISCV"
    "include/llvm/TargetParser/RISCVTargetParserDef.inc"
)

# PPC target features
tablegen_generate_target_parser(COMMON
    "${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPC.td"
    "-gen-target-features"
    "${LLVM_SOURCE_DIR}/lib/Target/PowerPC"
    "include/llvm/TargetParser/PPCGenTargetFeatures.inc"
)

#
# X86-specific TableGen
#
if (ARCH_AMD64)
    set(X86_TD "${LLVM_SOURCE_DIR}/lib/Target/X86/X86.td")

    tablegen_generate(X86 "${X86_TD}" "-gen-register-info" "lib/Target/X86/X86GenRegisterInfo.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-instr-info" "lib/Target/X86/X86GenInstrInfo.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-subtarget" "lib/Target/X86/X86GenSubtargetInfo.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-dag-isel" "lib/Target/X86/X86GenDAGISel.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-callingconv" "lib/Target/X86/X86GenCallingConv.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-asm-writer" "lib/Target/X86/X86GenAsmWriter.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-asm-writer;-asmwriternum=1" "lib/Target/X86/X86GenAsmWriter1.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-asm-matcher" "lib/Target/X86/X86GenAsmMatcher.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-fast-isel" "lib/Target/X86/X86GenFastISel.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-global-isel" "lib/Target/X86/X86GenGlobalISel.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-register-bank" "lib/Target/X86/X86GenRegisterBank.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-x86-fold-tables" "lib/Target/X86/X86GenFoldTables.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-x86-mnemonic-tables;-asmwriternum=1" "lib/Target/X86/X86GenMnemonicTables.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-x86-instr-mapping" "lib/Target/X86/X86GenInstrMapping.inc")
    tablegen_generate(X86 "${X86_TD}" "-gen-global-isel-combiner;-combiners=X86PreLegalizerCombiner" "lib/Target/X86/X86GenPreLegalizeGICombiner.inc")
endif()

#
# AArch64-specific TableGen
#
if (ARCH_AARCH64)
    set(AARCH64_TD "${LLVM_SOURCE_DIR}/lib/Target/AArch64/AArch64.td")

    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-register-info" "lib/Target/AArch64/AArch64GenRegisterInfo.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-instr-info" "lib/Target/AArch64/AArch64GenInstrInfo.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-subtarget" "lib/Target/AArch64/AArch64GenSubtargetInfo.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-dag-isel" "lib/Target/AArch64/AArch64GenDAGISel.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-callingconv" "lib/Target/AArch64/AArch64GenCallingConv.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-asm-writer" "lib/Target/AArch64/AArch64GenAsmWriter.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-asm-writer;-asmwriternum=1" "lib/Target/AArch64/AArch64GenAsmWriter1.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-asm-matcher" "lib/Target/AArch64/AArch64GenAsmMatcher.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-fast-isel" "lib/Target/AArch64/AArch64GenFastISel.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-global-isel" "lib/Target/AArch64/AArch64GenGlobalISel.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-global-isel-combiner;-combiners=AArch64O0PreLegalizerCombiner" "lib/Target/AArch64/AArch64GenO0PreLegalizeGICombiner.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-global-isel-combiner;-combiners=AArch64PreLegalizerCombiner" "lib/Target/AArch64/AArch64GenPreLegalizeGICombiner.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-global-isel-combiner;-combiners=AArch64PostLegalizerCombiner" "lib/Target/AArch64/AArch64GenPostLegalizeGICombiner.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-global-isel-combiner;-combiners=AArch64PostLegalizerLowering" "lib/Target/AArch64/AArch64GenPostLegalizeGILowering.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-emitter" "lib/Target/AArch64/AArch64GenMCCodeEmitter.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-pseudo-lowering" "lib/Target/AArch64/AArch64GenMCPseudoLowering.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-register-bank" "lib/Target/AArch64/AArch64GenRegisterBank.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-sd-node-info" "lib/Target/AArch64/AArch64GenSDNodeInfo.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-searchable-tables" "lib/Target/AArch64/AArch64GenSystemOperands.inc")
    tablegen_generate(AARCH64 "${AARCH64_TD}" "-gen-exegesis" "lib/Target/AArch64/AArch64GenExegesis.inc")
endif()

#
# PowerPC-specific TableGen
#
if (ARCH_PPC64LE)
    set(PPC_TD "${LLVM_SOURCE_DIR}/lib/Target/PowerPC/PPC.td")

    tablegen_generate(PPC "${PPC_TD}" "-gen-register-info" "lib/Target/PowerPC/PPCGenRegisterInfo.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-instr-info" "lib/Target/PowerPC/PPCGenInstrInfo.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-subtarget" "lib/Target/PowerPC/PPCGenSubtargetInfo.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-dag-isel" "lib/Target/PowerPC/PPCGenDAGISel.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-callingconv" "lib/Target/PowerPC/PPCGenCallingConv.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-asm-writer" "lib/Target/PowerPC/PPCGenAsmWriter.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-asm-matcher" "lib/Target/PowerPC/PPCGenAsmMatcher.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-fast-isel" "lib/Target/PowerPC/PPCGenFastISel.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-emitter" "lib/Target/PowerPC/PPCGenMCCodeEmitter.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-register-bank" "lib/Target/PowerPC/PPCGenRegisterBank.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-global-isel" "lib/Target/PowerPC/PPCGenGlobalISel.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-exegesis" "lib/Target/PowerPC/PPCGenExegesis.inc")
    tablegen_generate(PPC "${PPC_TD}" "-gen-sd-node-info" "lib/Target/PowerPC/PPCGenSDNodeInfo.inc")
endif()

#
# SystemZ-specific TableGen
#
if (ARCH_S390X)
    set(SYSTEMZ_TD "${LLVM_SOURCE_DIR}/lib/Target/SystemZ/SystemZ.td")

    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-register-info" "lib/Target/SystemZ/SystemZGenRegisterInfo.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-instr-info" "lib/Target/SystemZ/SystemZGenInstrInfo.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-subtarget" "lib/Target/SystemZ/SystemZGenSubtargetInfo.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-dag-isel" "lib/Target/SystemZ/SystemZGenDAGISel.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-callingconv" "lib/Target/SystemZ/SystemZGenCallingConv.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-asm-writer" "lib/Target/SystemZ/SystemZGenGNUAsmWriter.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-asm-writer;-asmwriternum=1" "lib/Target/SystemZ/SystemZGenHLASMAsmWriter.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-asm-matcher" "lib/Target/SystemZ/SystemZGenAsmMatcher.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-emitter" "lib/Target/SystemZ/SystemZGenMCCodeEmitter.inc")
    tablegen_generate(SYSTEMZ "${SYSTEMZ_TD}" "-gen-sd-node-info" "lib/Target/SystemZ/SystemZGenSDNodeInfo.inc")
endif()

#
# RISCV-specific TableGen
#
if (ARCH_RISCV64)
    set(RISCV_TD "${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCV.td")
    # GlobalISel and combiners use RISCVGISel.td which includes RISCVCombine.td
    set(RISCV_GISEL_TD "${LLVM_SOURCE_DIR}/lib/Target/RISCV/RISCVGISel.td")

    tablegen_generate(RISCV "${RISCV_TD}" "-gen-register-info" "lib/Target/RISCV/RISCVGenRegisterInfo.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-instr-info" "lib/Target/RISCV/RISCVGenInstrInfo.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-subtarget" "lib/Target/RISCV/RISCVGenSubtargetInfo.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-dag-isel" "lib/Target/RISCV/RISCVGenDAGISel.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-asm-writer" "lib/Target/RISCV/RISCVGenAsmWriter.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-asm-matcher" "lib/Target/RISCV/RISCVGenAsmMatcher.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-compress-inst-emitter" "lib/Target/RISCV/RISCVGenCompressInstEmitter.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-macro-fusion-pred" "lib/Target/RISCV/RISCVGenMacroFusion.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-emitter" "lib/Target/RISCV/RISCVGenMCCodeEmitter.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-pseudo-lowering" "lib/Target/RISCV/RISCVGenMCPseudoLowering.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-register-bank" "lib/Target/RISCV/RISCVGenRegisterBank.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-searchable-tables" "lib/Target/RISCV/RISCVGenSearchableTables.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-sd-node-info" "lib/Target/RISCV/RISCVGenSDNodeInfo.inc")
    tablegen_generate(RISCV "${RISCV_TD}" "-gen-exegesis" "lib/Target/RISCV/RISCVGenExegesis.inc")
    # GlobalISel commands use RISCVGISel.td (which includes RISCVCombine.td)
    tablegen_generate(RISCV "${RISCV_GISEL_TD}" "-gen-global-isel" "lib/Target/RISCV/RISCVGenGlobalISel.inc")
    tablegen_generate(RISCV "${RISCV_GISEL_TD}" "-gen-global-isel-combiner;-combiners=RISCVO0PreLegalizerCombiner" "lib/Target/RISCV/RISCVGenO0PreLegalizeGICombiner.inc")
    tablegen_generate(RISCV "${RISCV_GISEL_TD}" "-gen-global-isel-combiner;-combiners=RISCVPreLegalizerCombiner" "lib/Target/RISCV/RISCVGenPreLegalizeGICombiner.inc")
    tablegen_generate(RISCV "${RISCV_GISEL_TD}" "-gen-global-isel-combiner;-combiners=RISCVPostLegalizerCombiner" "lib/Target/RISCV/RISCVGenPostLegalizeGICombiner.inc")
endif()

#
# LoongArch-specific TableGen
#
if (ARCH_LOONGARCH64)
    set(LOONGARCH_TD "${LLVM_SOURCE_DIR}/lib/Target/LoongArch/LoongArch.td")

    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-register-info" "lib/Target/LoongArch/LoongArchGenRegisterInfo.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-instr-info" "lib/Target/LoongArch/LoongArchGenInstrInfo.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-subtarget" "lib/Target/LoongArch/LoongArchGenSubtargetInfo.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-dag-isel" "lib/Target/LoongArch/LoongArchGenDAGISel.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-asm-writer" "lib/Target/LoongArch/LoongArchGenAsmWriter.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-asm-matcher" "lib/Target/LoongArch/LoongArchGenAsmMatcher.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-pseudo-lowering" "lib/Target/LoongArch/LoongArchGenMCPseudoLowering.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-emitter" "lib/Target/LoongArch/LoongArchGenMCCodeEmitter.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-disassembler" "lib/Target/LoongArch/LoongArchGenDisassemblerTables.inc")
    tablegen_generate(LOONGARCH "${LOONGARCH_TD}" "-gen-sd-node-info" "lib/Target/LoongArch/LoongArchGenSDNodeInfo.inc")
endif()

# Combine all tablegen outputs
set(ALL_TABLEGEN_OUTPUTS ${COMMON_TABLEGEN_OUTPUTS})
if (ARCH_AMD64)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${X86_TABLEGEN_OUTPUTS})
elseif (ARCH_AARCH64)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${AARCH64_TABLEGEN_OUTPUTS})
elseif (ARCH_PPC64LE)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${PPC_TABLEGEN_OUTPUTS})
elseif (ARCH_S390X)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${SYSTEMZ_TABLEGEN_OUTPUTS})
elseif (ARCH_RISCV64)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${RISCV_TABLEGEN_OUTPUTS})
elseif (ARCH_LOONGARCH64)
    list(APPEND ALL_TABLEGEN_OUTPUTS ${LOONGARCH_TABLEGEN_OUTPUTS})
endif()

add_custom_target(llvm_tablegen_outputs DEPENDS ${ALL_TABLEGEN_OUTPUTS})

# Make sure GenVT.inc (generated by llvm-min-tblgen) is also a dependency
add_dependencies(llvm_tablegen_outputs llvm_gen_vt)

#
# Helper function to add LLVM library
#
function(add_llvm_library name)
    cmake_parse_arguments(ARG "" "" "SOURCES;DEPENDS" ${ARGN})

    add_library(${name} STATIC ${ARG_SOURCES})

    target_include_directories(${name} PUBLIC ${LLVM_INCLUDE_DIRS})
    # Include generated files (tablegen outputs) for target architecture
    target_include_directories(${name} PRIVATE "${LLVM_BINARY_DIR}/lib/Target/${LLVM_TARGET_ARCH}")
    # Include source directory for target architecture (for headers like TargetInfo/X86TargetInfo.h)
    target_include_directories(${name} PRIVATE "${LLVM_SOURCE_DIR}/lib/Target/${LLVM_TARGET_ARCH}")
    target_compile_definitions(${name} PRIVATE ${LLVM_COMPILE_DEFINITIONS})

    add_dependencies(${name} llvm_tablegen_outputs)

    if(ARG_DEPENDS)
        target_link_libraries(${name} PUBLIC ${ARG_DEPENDS})
    endif()
endfunction()

#
# Build LLVM libraries (in dependency order)
#
add_llvm_library(LLVMDemangle SOURCES ${LLVMDEMANGLE_SOURCES})
add_llvm_library(LLVMSupport SOURCES ${LLVMSUPPORT_SOURCES} ${LLVM_BLAKE3_ASM_SOURCES} DEPENDS LLVMDemangle)
add_llvm_library(LLVMBinaryFormat SOURCES ${LLVMBINARYFORMAT_SOURCES} DEPENDS LLVMSupport)
add_llvm_library(LLVMTargetParser SOURCES ${LLVMTARGETPARSER_SOURCES} DEPENDS LLVMSupport)
add_llvm_library(LLVMCore SOURCES ${LLVMCORE_SOURCES} DEPENDS LLVMSupport LLVMBinaryFormat LLVMTargetParser LLVMRemarks)
add_llvm_library(LLVMMC SOURCES ${LLVMMC_SOURCES} DEPENDS LLVMSupport LLVMBinaryFormat LLVMTargetParser)
add_llvm_library(LLVMMCParser SOURCES ${LLVMMCPARSER_SOURCES} DEPENDS LLVMMC LLVMSupport)
add_llvm_library(LLVMMCDisassembler SOURCES ${LLVMMCDISASSEMBLER_SOURCES} DEPENDS LLVMMC LLVMSupport)
add_llvm_library(LLVMBitstreamReader SOURCES ${LLVMBITSTREAMREADER_SOURCES} DEPENDS LLVMSupport)
add_llvm_library(LLVMBitReader SOURCES ${LLVMBITREADER_SOURCES} DEPENDS LLVMCore LLVMBitstreamReader LLVMSupport)
add_llvm_library(LLVMBitWriter SOURCES ${LLVMBITWRITER_SOURCES} DEPENDS LLVMCore LLVMSupport)
add_llvm_library(LLVMRemarks SOURCES ${LLVMREMARKS_SOURCES} DEPENDS LLVMSupport LLVMBitstreamReader)
add_llvm_library(LLVMDebugInfoCodeView SOURCES ${LLVMDEBUGINFOCODEVI_SOURCES} DEPENDS LLVMSupport)
add_llvm_library(LLVMObject SOURCES ${LLVMOBJECT_SOURCES} DEPENDS LLVMCore LLVMMC LLVMBinaryFormat LLVMSupport LLVMTargetParser LLVMTextAPI)
add_llvm_library(LLVMTextAPI SOURCES ${LLVMTEXTAPI_SOURCES} DEPENDS LLVMSupport LLVMBinaryFormat LLVMTargetParser)
add_llvm_library(LLVMObjectYAML SOURCES ${LLVMOBJECTYAML_SOURCES} DEPENDS LLVMBinaryFormat LLVMObject LLVMSupport LLVMTargetParser LLVMDebugInfoCodeView LLVMMC)
add_llvm_library(LLVMAsmParser SOURCES ${LLVMASMPARSER_SOURCES} DEPENDS LLVMBinaryFormat LLVMCore LLVMSupport LLVMTargetParser)
add_llvm_library(LLVMIRReader SOURCES ${LLVMIRREADER_SOURCES} DEPENDS LLVMAsmParser LLVMBitReader LLVMCore LLVMSupport)
add_llvm_library(LLVMDebugInfoDWARF SOURCES ${LLVMDEBUGINFODWARF_SOURCES} DEPENDS LLVMSupport LLVMObject LLVMBinaryFormat)
add_llvm_library(LLVMProfileData SOURCES ${LLVMPROFILEDATA_SOURCES} DEPENDS LLVMSupport LLVMCore)
add_llvm_library(LLVMAnalysis SOURCES ${LLVMANALYSIS_SOURCES} DEPENDS LLVMCore LLVMObject LLVMProfileData LLVMSupport LLVMFrontendHLSL)
add_llvm_library(LLVMTransformUtils SOURCES ${LLVMTRANSFORMUTILS_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMSupport)
add_llvm_library(LLVMLinker SOURCES ${LLVMLINKER_SOURCES} DEPENDS LLVMCore LLVMObject LLVMSupport LLVMTransformUtils LLVMTargetParser)
add_llvm_library(LLVMTarget SOURCES ${LLVMTARGET_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMSupport)
add_llvm_library(LLVMScalarOpts SOURCES ${LLVMSCALAROPTS_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMInstCombine SOURCES ${LLVMINSTCOMBINE_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMAggressiveInstCombine SOURCES ${LLVMAGGRESSIVEINSTCOMBINE_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMVectorize SOURCES ${LLVMVECTORIZE_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMipo SOURCES ${LLVMIPO_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMScalarOpts LLVMInstCombine LLVMSupport LLVMFrontendOpenMP LLVMCGData)
add_llvm_library(LLVMInstrumentation SOURCES ${LLVMINSTRUMENTATION_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMProfileData LLVMSupport)
add_llvm_library(LLVMCodeGenTypes SOURCES ${LLVMCODEGENTYPES_SOURCES} DEPENDS LLVMSupport)
add_llvm_library(LLVMCodeGen SOURCES ${LLVMCODEGEN_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTarget LLVMTransformUtils LLVMScalarOpts LLVMMC LLVMCodeGenTypes LLVMBitReader LLVMBitWriter LLVMProfileData LLVMSupport LLVMCGData)
add_llvm_library(LLVMGlobalISel SOURCES ${LLVMGLOBALISEL_SOURCES} DEPENDS LLVMCore LLVMCodeGen LLVMAnalysis LLVMTarget LLVMMC LLVMSupport)
add_llvm_library(LLVMSelectionDAG SOURCES ${LLVMSELECTIONDAG_SOURCES} DEPENDS LLVMCore LLVMCodeGen LLVMAnalysis LLVMTarget LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMAsmPrinter SOURCES ${LLVMASMPRINTER_SOURCES} DEPENDS LLVMCore LLVMCodeGen LLVMAnalysis LLVMTarget LLVMMC LLVMMCParser LLVMDebugInfoCodeView LLVMRemarks LLVMSupport)
add_llvm_library(LLVMPasses SOURCES ${LLVMPASSES_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMScalarOpts LLVMInstCombine LLVMVectorize LLVMipo LLVMInstrumentation LLVMTarget LLVMSupport)
add_llvm_library(LLVMRuntimeDyld SOURCES ${LLVMRUNTIMEDYLD_SOURCES} DEPENDS LLVMCore LLVMObject LLVMMC LLVMSupport)
add_llvm_library(LLVMExecutionEngine SOURCES ${LLVMEXECUTIONENGINE_SOURCES} DEPENDS LLVMCore LLVMTarget LLVMObject LLVMMC LLVMRuntimeDyld LLVMSupport)
add_llvm_library(LLVMCGData SOURCES ${LLVMCGDATA_SOURCES} DEPENDS LLVMSupport LLVMCore LLVMObject)
add_llvm_library(LLVMObjCARCOpts SOURCES ${LLVMOBJCARCOPTS_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMSupport)
add_llvm_library(LLVMIRPrinter SOURCES ${LLVMIRPRINTER_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMSupport)
add_llvm_library(LLVMCFGuard SOURCES ${LLVMCFGUARD_SOURCES} DEPENDS LLVMCore LLVMSupport)
add_llvm_library(LLVMFrontendOpenMP SOURCES ${LLVMFRONTENDOPENMP_SOURCES} DEPENDS LLVMCore LLVMTransformUtils LLVMSupport LLVMFrontendAtomic LLVMFrontendOffloading)
add_llvm_library(LLVMFrontendAtomic SOURCES ${LLVMFRONTENDATOMIC_SOURCES} DEPENDS LLVMCore LLVMSupport LLVMAnalysis)
add_llvm_library(LLVMFrontendOffloading SOURCES ${LLVMFRONTENDOFFLOADING_SOURCES} DEPENDS LLVMCore LLVMSupport)
add_llvm_library(LLVMFrontendHLSL SOURCES ${LLVMFRONTENDHLSL_SOURCES} DEPENDS LLVMBinaryFormat LLVMCore LLVMSupport)
add_llvm_library(LLVMCoroutines SOURCES ${LLVMCOROUTINES_SOURCES} DEPENDS LLVMCore LLVMAnalysis LLVMTransformUtils LLVMipo LLVMSupport)
add_llvm_library(LLVMHipStdPar SOURCES ${LLVMHIPSTDPAR_SOURCES} DEPENDS LLVMAnalysis LLVMCore LLVMSupport LLVMTransformUtils)
add_llvm_library(LLVMSandboxIR SOURCES ${LLVMSANDBOXIR_SOURCES} DEPENDS LLVMCore LLVMSupport)

# Architecture-specific libraries
if (ARCH_AMD64)
    add_llvm_library(LLVMX86Info SOURCES ${LLVMX86INFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMX86Desc SOURCES ${LLVMX86DESC_SOURCES} DEPENDS LLVMX86Info LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMX86CodeGen SOURCES ${LLVMX86CODEGEN_SOURCES} DEPENDS LLVMX86Info LLVMX86Desc LLVMCodeGen LLVMGlobalISel LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
elseif (ARCH_AARCH64)
    add_llvm_library(LLVMAArch64Info SOURCES ${LLVMAARCH64INFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMAArch64Desc SOURCES ${LLVMAARCH64DESC_SOURCES} ${LLVMAARCH64UTILS_SOURCES} DEPENDS LLVMAArch64Info LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMAArch64CodeGen SOURCES ${LLVMAARCH64CODEGEN_SOURCES} DEPENDS LLVMAArch64Info LLVMAArch64Desc LLVMCodeGen LLVMGlobalISel LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
elseif (ARCH_PPC64LE)
    add_llvm_library(LLVMPowerPCInfo SOURCES ${LLVMPOWERPCINFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMPowerPCDesc SOURCES ${LLVMPOWERPCDESC_SOURCES} DEPENDS LLVMPowerPCInfo LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMPowerPCCodeGen SOURCES ${LLVMPOWERPCCODEGEN_SOURCES} DEPENDS LLVMPowerPCInfo LLVMPowerPCDesc LLVMCodeGen LLVMGlobalISel LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
elseif (ARCH_S390X)
    add_llvm_library(LLVMSystemZInfo SOURCES ${LLVMSYSTEMZINFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMSystemZDesc SOURCES ${LLVMSYSTEMZDESC_SOURCES} DEPENDS LLVMSystemZInfo LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMSystemZCodeGen SOURCES ${LLVMSYSTEMZCODEGEN_SOURCES} DEPENDS LLVMSystemZInfo LLVMSystemZDesc LLVMCodeGen LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
elseif (ARCH_RISCV64)
    add_llvm_library(LLVMRISCVInfo SOURCES ${LLVMRISCVINFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMRISCVDesc SOURCES ${LLVMRISCVDESC_SOURCES} DEPENDS LLVMRISCVInfo LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMRISCVCodeGen SOURCES ${LLVMRISCVCODEGEN_SOURCES} DEPENDS LLVMRISCVInfo LLVMRISCVDesc LLVMCodeGen LLVMGlobalISel LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
elseif (ARCH_LOONGARCH64)
    add_llvm_library(LLVMLoongArchInfo SOURCES ${LLVMLOONGARCHINFO_SOURCES} DEPENDS LLVMMC LLVMSupport)
    add_llvm_library(LLVMLoongArchDesc SOURCES ${LLVMLOONGARCHDESC_SOURCES} DEPENDS LLVMLoongArchInfo LLVMMC LLVMMCDisassembler LLVMSupport)
    add_llvm_library(LLVMLoongArchCodeGen SOURCES ${LLVMLOONGARCHCODEGEN_SOURCES} DEPENDS LLVMLoongArchInfo LLVMLoongArchDesc LLVMCodeGen LLVMSelectionDAG LLVMAsmPrinter LLVMTarget LLVMMC LLVMSupport)
endif()

# XRay support
if (ENABLE_XRAY)
    add_llvm_library(LLVMXRay SOURCES ${LLVMXRAY_SOURCES} DEPENDS LLVMSupport LLVMObject)
endif()

#
# Create the interface library that ClickHouse links against
#
if (NOT ENABLE_EMBEDDED_COMPILER AND NOT ENABLE_DWARF_PARSER)
    set (REQUIRED_LLVM_LIBRARIES LLVMSupport)
else()
    set (REQUIRED_LLVM_LIBRARIES
        LLVMExecutionEngine
        LLVMRuntimeDyld
        LLVMAsmPrinter
        LLVMDebugInfoDWARF
        LLVMGlobalISel
        LLVMSelectionDAG
        LLVMMCDisassembler
        LLVMPasses
        LLVMCodeGen
        LLVMCodeGenTypes
        LLVMipo
        LLVMBitWriter
        LLVMInstrumentation
        LLVMScalarOpts
        LLVMAggressiveInstCombine
        LLVMInstCombine
        LLVMVectorize
        LLVMTransformUtils
        LLVMTarget
        LLVMAnalysis
        LLVMProfileData
        LLVMObject
        LLVMBitReader
        LLVMCore
        LLVMRemarks
        LLVMBitstreamReader
        LLVMMCParser
        LLVMMC
        LLVMBinaryFormat
        LLVMDebugInfoCodeView
        LLVMCGData
        LLVMObjCARCOpts
        LLVMIRPrinter
        LLVMCFGuard
        LLVMFrontendOpenMP
        LLVMFrontendAtomic
        LLVMFrontendOffloading
        LLVMFrontendHLSL
        LLVMCoroutines
        LLVMHipStdPar
        LLVMSandboxIR
        LLVMTextAPI
        LLVMObjectYAML
        LLVMAsmParser
        LLVMIRReader
        LLVMLinker
        LLVMTargetParser
        LLVMSupport
        LLVMDemangle
    )

    if (ARCH_AMD64)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMX86Info LLVMX86Desc LLVMX86CodeGen)
    elseif (ARCH_AARCH64)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMAArch64Info LLVMAArch64Desc LLVMAArch64CodeGen)
    elseif (ARCH_PPC64LE)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMPowerPCInfo LLVMPowerPCDesc LLVMPowerPCCodeGen)
    elseif (ARCH_S390X)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMSystemZInfo LLVMSystemZDesc LLVMSystemZCodeGen)
    elseif (ARCH_RISCV64)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMRISCVInfo LLVMRISCVDesc LLVMRISCVCodeGen)
    elseif (ARCH_LOONGARCH64)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMLoongArchInfo LLVMLoongArchDesc LLVMLoongArchCodeGen)
    endif()

    if (ENABLE_XRAY)
        list(APPEND REQUIRED_LLVM_LIBRARIES LLVMXRay)
    endif()
endif()

add_library(_llvm INTERFACE)
target_link_libraries(_llvm INTERFACE ${REQUIRED_LLVM_LIBRARIES})
target_include_directories(_llvm SYSTEM BEFORE INTERFACE ${LLVM_INCLUDE_DIRS})
add_library(ch_contrib::llvm ALIAS _llvm)
