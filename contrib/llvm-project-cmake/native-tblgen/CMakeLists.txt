# Native build for llvm-tblgen tools
# This is used when cross-compiling to build tablegen for the host system
#
# This CMakeLists.txt is invoked by ExternalProject_Add from the main CMakeLists.txt
# It builds llvm-min-tblgen and llvm-tblgen without the target sysroot, making them
# runnable on the host system.

cmake_minimum_required(VERSION 3.20)
project(llvm-native-tblgen CXX C ASM)

# LLVM source directory (passed from parent)
if (NOT DEFINED LLVM_SOURCE_DIR)
    message(FATAL_ERROR "LLVM_SOURCE_DIR must be defined")
endif()

# Output directory for generated files
if (NOT DEFINED LLVM_NATIVE_OUTPUT_DIR)
    set(LLVM_NATIVE_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Detect host architecture for config headers
# These are used by the config templates but don't affect tablegen functionality
if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(LLVM_TARGET_ARCH "X86")
    set(LLVM_HOST_TRIPLE "x86_64-unknown-linux-gnu")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64")
    set(LLVM_TARGET_ARCH "AArch64")
    set(LLVM_HOST_TRIPLE "aarch64-unknown-linux-gnu")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "ppc64le")
    set(LLVM_TARGET_ARCH "PowerPC")
    set(LLVM_HOST_TRIPLE "powerpc64le-unknown-linux-gnu")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "s390x")
    set(LLVM_TARGET_ARCH "SystemZ")
    set(LLVM_HOST_TRIPLE "s390x-unknown-linux-gnu")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "riscv64")
    set(LLVM_TARGET_ARCH "RISCV")
    set(LLVM_HOST_TRIPLE "riscv64-unknown-linux-gnu")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "loongarch64")
    set(LLVM_TARGET_ARCH "LoongArch")
    set(LLVM_HOST_TRIPLE "loongarch64-unknown-linux-gnu")
else()
    # Default to X86 - tablegen doesn't actually use this at runtime
    set(LLVM_TARGET_ARCH "X86")
    set(LLVM_HOST_TRIPLE "x86_64-unknown-linux-gnu")
endif()

# Generate config headers
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Config")

#
# Platform-specific config variables for the HOST system
# Note: native-tblgen builds for the host, so we use CMAKE_HOST_SYSTEM_NAME.
# We do not attempt to detect musl here: ClickHouse CI hosts are always glibc,
# and if someone builds on a musl host the missing headers will fail clearly.
#

# Shared library extension
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    set(LLVM_SHLIB_EXT ".dylib")
else()
    set(LLVM_SHLIB_EXT ".so")
endif()

# mallinfo is only available on Linux with glibc
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    set(HAVE_MALLINFO_DEFINE "#define HAVE_MALLINFO 1")
else()
    set(HAVE_MALLINFO_DEFINE "/* #undef HAVE_MALLINFO */")
endif()

# backtrace/execinfo.h - available on glibc Linux and macOS.
# FreeBSD requires a separate libexecinfo which we don't link here,
# so we leave backtrace disabled there.
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    set(HAVE_BACKTRACE_DEFINE "#define HAVE_BACKTRACE 1")
    set(BACKTRACE_HEADER_DEFINE "#define BACKTRACE_HEADER <execinfo.h>")
else()
    set(HAVE_BACKTRACE_DEFINE "/* #undef HAVE_BACKTRACE */")
    set(BACKTRACE_HEADER_DEFINE "/* No backtrace header */")
endif()

# getauxval is Linux-specific
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux")
    set(HAVE_GETAUXVAL_DEFINE "#define HAVE_GETAUXVAL 1")
    set(HAVE_BUILTIN_THREAD_POINTER_DEFINE "#define HAVE_BUILTIN_THREAD_POINTER 1")
else()
    set(HAVE_GETAUXVAL_DEFINE "/* #undef HAVE_GETAUXVAL */")
    set(HAVE_BUILTIN_THREAD_POINTER_DEFINE "/* #undef HAVE_BUILTIN_THREAD_POINTER */")
endif()

# macOS-specific headers and functions
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin")
    set(HAVE_MACH_MACH_H_DEFINE "#define HAVE_MACH_MACH_H 1")
    set(HAVE_MALLOC_MALLOC_H_DEFINE "#define HAVE_MALLOC_MALLOC_H 1")
    set(HAVE_MALLOC_ZONE_STATISTICS_DEFINE "#define HAVE_MALLOC_ZONE_STATISTICS 1")
else()
    set(HAVE_MACH_MACH_H_DEFINE "/* #undef HAVE_MACH_MACH_H */")
    set(HAVE_MALLOC_MALLOC_H_DEFINE "/* #undef HAVE_MALLOC_MALLOC_H */")
    set(HAVE_MALLOC_ZONE_STATISTICS_DEFINE "/* #undef HAVE_MALLOC_ZONE_STATISTICS */")
endif()

# stat struct: st_mtim (Linux) vs st_mtimespec (macOS/BSD)
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin" OR CMAKE_HOST_SYSTEM_NAME STREQUAL "FreeBSD")
    set(HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC_DEFINE "#define HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 1")
    set(HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC_DEFINE "/* #undef HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC */")
else()
    set(HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC_DEFINE "/* #undef HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC */")
    set(HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC_DEFINE "#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1")
endif()

# arc4random is available on macOS and BSD
if (CMAKE_HOST_SYSTEM_NAME STREQUAL "Darwin" OR CMAKE_HOST_SYSTEM_NAME STREQUAL "FreeBSD")
    set(HAVE_DECL_ARC4RANDOM_DECL_DEFINE "#define HAVE_DECL_ARC4RANDOM 1")
    set(HAVE_DECL_ARC4RANDOM_DEFINE "#define HAVE_DECL_ARC4RANDOM 1")
else()
    set(HAVE_DECL_ARC4RANDOM_DECL_DEFINE "#define HAVE_DECL_ARC4RANDOM 0")
    set(HAVE_DECL_ARC4RANDOM_DEFINE "/* #undef HAVE_DECL_ARC4RANDOM */")
endif()

# llvm-config.h - minimal version
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../config/llvm-config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Config/llvm-config.h"
    @ONLY
)

# config.h - minimal version  
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../config/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Config/config.h"
    @ONLY
)

# abi-breaking.h
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/../config/abi-breaking.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Config/abi-breaking.h"
    @ONLY
)

# VCSRevision.h
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Support")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/Support/VCSRevision.h"
    "#define LLVM_REVISION \"\"\n#define LLVM_REPOSITORY \"\"\n")

# Compile definitions
set(LLVM_COMPILE_DEFINITIONS
    __STDC_CONSTANT_MACROS
    __STDC_FORMAT_MACROS
    __STDC_LIMIT_MACROS
    LLVM_ENABLE_THREADS=1
    LLVM_HAS_ATOMICS=1
    LLVM_ENABLE_EH=1
    LLVM_ENABLE_RTTI=1
)

# Include source file lists
include(${CMAKE_CURRENT_SOURCE_DIR}/../sources/LLVMDemangle.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/../sources/LLVMSupport.cmake)

#
# BLAKE3 architecture-specific sources
# The LLVMSupport sources include blake3_neon.c for ARM, but we need
# x86 assembly files when building on x86_64 hosts.
#
# Note: LLVMSupport.cmake includes blake3_neon.c unconditionally.
# On x86, NEON is not available but blake3_neon.c compiles to nothing when NEON is not detected.
# We need to add the x86 assembly files for the actual optimized implementations.
#
message(STATUS "Native tablegen: CMAKE_HOST_SYSTEM_PROCESSOR=${CMAKE_HOST_SYSTEM_PROCESSOR}")
if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    set(BLAKE3_ASM_SOURCES
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_sse2_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_sse41_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx2_x86-64_unix.S
        ${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx512_x86-64_unix.S
    )
    # AVX-512 requires specific compiler flags
    set_source_files_properties(${LLVM_SOURCE_DIR}/lib/Support/BLAKE3/blake3_avx512_x86-64_unix.S
        PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512vl")
else()
    # For non-x86 hosts (ARM, etc), disable x86 BLAKE3 implementations
    set(BLAKE3_ASM_SOURCES "")
    list(APPEND LLVM_COMPILE_DEFINITIONS BLAKE3_NO_AVX512 BLAKE3_NO_AVX2 BLAKE3_NO_SSE41 BLAKE3_NO_SSE2)
endif()

#
# Build llvm-min-tblgen
#
add_executable(llvm-min-tblgen
    ${LLVM_SOURCE_DIR}/utils/TableGen/llvm-min-tblgen.cpp
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
    ${LLVMSUPPORT_SOURCES}
    ${LLVMDEMANGLE_SOURCES}
    ${BLAKE3_ASM_SOURCES}
)

target_include_directories(llvm-min-tblgen PRIVATE
    ${LLVM_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${LLVM_SOURCE_DIR}/utils/TableGen
    ${LLVM_SOURCE_DIR}/../third-party/siphash/include
)

target_compile_definitions(llvm-min-tblgen PRIVATE ${LLVM_COMPILE_DEFINITIONS})

# On Apple, libdl does not exist as a separate library
if (UNIX AND NOT APPLE)
    target_link_libraries(llvm-min-tblgen PRIVATE pthread dl)
elseif (UNIX)
    target_link_libraries(llvm-min-tblgen PRIVATE pthread)
endif()

#
# Generate GenVT.inc
#
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/CodeGen")
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
    COMMAND $<TARGET_FILE:llvm-min-tblgen>
        -gen-vt
        -I "${LLVM_SOURCE_DIR}/include"
        "${LLVM_SOURCE_DIR}/include/llvm/CodeGen/ValueTypes.td"
        -o "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc"
    DEPENDS llvm-min-tblgen
    COMMENT "TableGen (native-min): GenVT.inc"
    VERBATIM
)
add_custom_target(native_gen_vt DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/include/llvm/CodeGen/GenVT.inc")

#
# Build full llvm-tblgen
#
include(${CMAKE_CURRENT_SOURCE_DIR}/../sources/LLVMCodeGenTypes.cmake)

add_executable(llvm-tblgen
    ${LLVM_SOURCE_DIR}/utils/TableGen/llvm-tblgen.cpp
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
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86DisassemblerTables.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86FoldTablesEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86InstrMappingEmitter.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86MnemonicTables.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86ModRMFilters.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/X86RecognizableInstr.cpp
    ${LLVM_SOURCE_DIR}/utils/TableGen/WebAssemblyDisassemblerEmitter.cpp
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
    ${LLVMSUPPORT_SOURCES}
    ${LLVMDEMANGLE_SOURCES}
    ${LLVMCODEGENTYPES_SOURCES}
    ${BLAKE3_ASM_SOURCES}
)

target_include_directories(llvm-tblgen PRIVATE
    ${LLVM_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/include
    ${LLVM_SOURCE_DIR}/utils/TableGen
    ${LLVM_SOURCE_DIR}/../third-party/siphash/include
)

target_compile_definitions(llvm-tblgen PRIVATE ${LLVM_COMPILE_DEFINITIONS})

add_dependencies(llvm-tblgen native_gen_vt)

# On Apple, libdl does not exist as a separate library
if (UNIX AND NOT APPLE)
    target_link_libraries(llvm-tblgen PRIVATE pthread dl)
elseif (UNIX)
    target_link_libraries(llvm-tblgen PRIVATE pthread)
endif()

# Install the executables to the output directory
install(TARGETS llvm-min-tblgen llvm-tblgen
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
