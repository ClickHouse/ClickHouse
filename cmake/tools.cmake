# Compiler

if (NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message (FATAL_ERROR "Compiler ${CMAKE_CXX_COMPILER_ID} is not supported")
endif ()

# Print details to output
execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version
    OUTPUT_VARIABLE COMPILER_SELF_IDENTIFICATION
    COMMAND_ERROR_IS_FATAL ANY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
message (STATUS "Using compiler:\n${COMPILER_SELF_IDENTIFICATION}")

# Require minimum compiler versions
set (CLANG_MINIMUM_VERSION 17)
set (XCODE_MINIMUM_VERSION 12.0)
set (APPLE_CLANG_MINIMUM_VERSION 12.0.0)

if (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    # (Experimental!) Specify "-DALLOW_APPLECLANG=ON" when running CMake configuration step, if you want to experiment with using it.
    if (NOT ALLOW_APPLECLANG AND NOT DEFINED ENV{ALLOW_APPLECLANG})
        message (FATAL_ERROR "Compilation with AppleClang is unsupported. Please use vanilla Clang, e.g. from Homebrew.")
    endif ()

    # For a mapping between XCode / AppleClang / vanilla Clang versions, see https://en.wikipedia.org/wiki/Xcode
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${APPLE_CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Compilation with AppleClang version ${CMAKE_CXX_COMPILER_VERSION} is unsupported, the minimum required version is ${APPLE_CLANG_MINIMUM_VERSION} (Xcode ${XCODE_MINIMUM_VERSION}).")
    endif ()
else ()
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Compilation with Clang version ${CMAKE_CXX_COMPILER_VERSION} is unsupported, the minimum required version is ${CLANG_MINIMUM_VERSION}.")
    endif ()
endif ()

string (REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
list (GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Linker
option (LINKER_NAME "Linker name or full path")

if (LINKER_NAME MATCHES "gold")
    message (FATAL_ERROR "Linking with gold is unsupported. Please use lld.")
endif ()

if (NOT LINKER_NAME)
    if (OS_LINUX AND NOT ARCH_S390X)
        find_program (LLD_PATH NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
    elseif (OS_DARWIN)
        find_program (LLD_PATH NAMES "ld")
        # Duplicate libraries passed to the linker is not a problem.
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-no_warn_duplicate_libraries")
    endif ()
    if (LLD_PATH)
        if (OS_LINUX OR OS_DARWIN)
            # Clang driver simply allows full linker path.
            set (LINKER_NAME ${LLD_PATH})
        endif ()
    endif()
endif()

if (LINKER_NAME)
    find_program (LLD_PATH NAMES ${LINKER_NAME})
    if (NOT LLD_PATH)
        message (FATAL_ERROR "Using linker ${LINKER_NAME} but can't find its path.")
    endif ()
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --ld-path=${LLD_PATH}")
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME}")
elseif (NOT ARCH_S390X AND NOT OS_FREEBSD)
    message (FATAL_ERROR "The only supported linker is LLVM's LLD, but we cannot find it.")
else ()
    message(STATUS "Using linker: <default>")
endif ()

# Archiver
find_program (LLVM_AR_PATH NAMES "llvm-ar-${COMPILER_VERSION_MAJOR}" "llvm-ar")
if (LLVM_AR_PATH)
    set (CMAKE_AR "${LLVM_AR_PATH}")
endif ()
message(STATUS "Using archiver: ${CMAKE_AR}")

# Ranlib
find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib-${COMPILER_VERSION_MAJOR}" "llvm-ranlib")
if (LLVM_RANLIB_PATH)
    set (CMAKE_RANLIB "${LLVM_RANLIB_PATH}")
endif ()
message(STATUS "Using ranlib: ${CMAKE_RANLIB}")

# Install Name Tool
find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool-${COMPILER_VERSION_MAJOR}" "llvm-install-name-tool")
if (LLVM_INSTALL_NAME_TOOL_PATH)
    set (CMAKE_INSTALL_NAME_TOOL "${LLVM_INSTALL_NAME_TOOL_PATH}")
endif ()
message(STATUS "Using install-name-tool: ${CMAKE_INSTALL_NAME_TOOL}")

# Objcopy
find_program (OBJCOPY_PATH NAMES "llvm-objcopy-${COMPILER_VERSION_MAJOR}" "llvm-objcopy" "objcopy")
if (OBJCOPY_PATH)
    message (STATUS "Using objcopy: ${OBJCOPY_PATH}")
else ()
    message (FATAL_ERROR "Cannot find objcopy.")
endif ()

# Strip
find_program (STRIP_PATH NAMES "llvm-strip-${COMPILER_VERSION_MAJOR}" "llvm-strip" "strip")
if (STRIP_PATH)
    message (STATUS "Using strip: ${STRIP_PATH}")
else ()
    message (FATAL_ERROR "Cannot find strip.")
endif ()

if (OS_DARWIN AND NOT CMAKE_TOOLCHAIN_FILE)
    # utils/list-licenses/list-licenses.sh (which generates system table system.licenses) needs the GNU versions of find and grep. These are
    # not available out-of-the-box on Mac. As a special case, Darwin builds in CI are cross-compiled from x86 Linux where the GNU userland is
    # available.
    find_program(GFIND_PATH NAMES "gfind")
    if (NOT GFIND_PATH)
        message (FATAL_ERROR "GNU find not found. You can install it with 'brew install findutils'.")
    endif()
    find_program(GGREP_PATH NAMES "ggrep")
    if (NOT GGREP_PATH)
        message (FATAL_ERROR "GNU grep not found. You can install it with 'brew install grep'.")
    endif()
endif ()
