# Compiler

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    set (COMPILER_CLANG 1) # Safe to treat AppleClang as a regular Clang, in general.
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

execute_process(COMMAND ${CMAKE_CXX_COMPILER} --version)

if (COMPILER_GCC)
    # Require minimum version of gcc
    set (GCC_MINIMUM_VERSION 11)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${GCC_MINIMUM_VERSION} AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least ${GCC_MINIMUM_VERSION}. For example, if GCC ${GCC_MINIMUM_VERSION} is available under gcc-${GCC_MINIMUM_VERSION}, g++-${GCC_MINIMUM_VERSION} names, do the following: export CC=gcc-${GCC_MINIMUM_VERSION} CXX=g++-${GCC_MINIMUM_VERSION}; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()

    message (WARNING "GCC compiler is not officially supported for ClickHouse. You should migrate to clang.")

elseif (COMPILER_CLANG)
    # Require minimum version of clang/apple-clang
    if (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
        # (Experimental!) Specify "-DALLOW_APPLECLANG=ON" when running CMake configuration step, if you want to experiment with using it.
        if (NOT ALLOW_APPLECLANG AND NOT DEFINED ENV{ALLOW_APPLECLANG})
            message (FATAL_ERROR "AppleClang is not supported, you should install clang from brew. See the instruction: https://clickhouse.com/docs/en/development/build-osx/")
        endif ()

        # AppleClang 10.0.1 (Xcode 10.2) corresponds to LLVM/Clang upstream version 7.0.0
        # AppleClang 11.0.0 (Xcode 11.0) corresponds to LLVM/Clang upstream version 8.0.0
        set (XCODE_MINIMUM_VERSION 10.2)
        set (APPLE_CLANG_MINIMUM_VERSION 10.0.1)
        if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${APPLE_CLANG_MINIMUM_VERSION})
            message (FATAL_ERROR "AppleClang compiler version must be at least ${APPLE_CLANG_MINIMUM_VERSION} (Xcode ${XCODE_MINIMUM_VERSION}).")
        elseif (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11.0.0)
            # char8_t is available starting (upstream vanilla) Clang 7, but prior to Clang 8,
            # it is not enabled by -std=c++20 and can be enabled with an explicit -fchar8_t.
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fchar8_t")
            set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fchar8_t")
        endif ()
    else ()
        set (CLANG_MINIMUM_VERSION 12)
        if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
            message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
        endif ()
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler. Compilation has only been tested with Clang and GCC.")
endif ()

string (REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
list (GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

# Linker

# Example values: `lld-10`, `gold`.
option (LINKER_NAME "Linker name or full path")

if (COMPILER_GCC AND NOT LINKER_NAME)
    find_program (LLD_PATH NAMES "ld.lld")
    find_program (GOLD_PATH NAMES "ld.gold")
elseif (NOT LINKER_NAME)
    find_program (LLD_PATH NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "lld-${COMPILER_VERSION_MAJOR}" "ld.lld" "lld")
    find_program (GOLD_PATH NAMES "ld.gold" "gold")
endif ()

if (OS_LINUX AND NOT LINKER_NAME)
    # We prefer LLD linker over Gold or BFD on Linux.
    if (LLD_PATH)
        if (COMPILER_GCC)
            # GCC driver requires one of supported linker names like "lld".
            set (LINKER_NAME "lld")
        else ()
            # Clang driver simply allows full linker path.
            set (LINKER_NAME ${LLD_PATH})
        endif ()
    endif ()

    if (NOT LINKER_NAME)
        if (GOLD_PATH)
            if (COMPILER_GCC)
                set (LINKER_NAME "gold")
            else ()
                set (LINKER_NAME ${GOLD_PATH})
            endif ()
        endif ()
    endif ()
endif ()

if (LINKER_NAME)
    if (COMPILER_CLANG AND (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 12.0.0 OR CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 12.0.0))
        find_program (LLD_PATH NAMES ${LINKER_NAME})
        if (NOT LLD_PATH)
            message (FATAL_ERROR "Using linker ${LINKER_NAME} but can't find its path.")
        endif ()
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --ld-path=${LLD_PATH}")
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --ld-path=${LLD_PATH}")
    else ()
        set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
        set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    endif ()

    message(STATUS "Using custom linker by name: ${LINKER_NAME}")
endif ()

# Archiver

if (COMPILER_GCC)
    find_program (LLVM_AR_PATH NAMES "llvm-ar" "llvm-ar-13" "llvm-ar-12" "llvm-ar-11")
else ()
    find_program (LLVM_AR_PATH NAMES "llvm-ar-${COMPILER_VERSION_MAJOR}" "llvm-ar")
endif ()

if (LLVM_AR_PATH)
    set (CMAKE_AR "${LLVM_AR_PATH}")
endif ()

# Ranlib

if (COMPILER_GCC)
    find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib" "llvm-ranlib-13" "llvm-ranlib-12" "llvm-ranlib-11")
else ()
    find_program (LLVM_RANLIB_PATH NAMES "llvm-ranlib-${COMPILER_VERSION_MAJOR}" "llvm-ranlib")
endif ()

if (LLVM_RANLIB_PATH)
    set (CMAKE_RANLIB "${LLVM_RANLIB_PATH}")
endif ()

# Install Name Tool

if (COMPILER_GCC)
    find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool" "llvm-install-name-tool-13" "llvm-install-name-tool-12" "llvm-install-name-tool-11")
else ()
    find_program (LLVM_INSTALL_NAME_TOOL_PATH NAMES "llvm-install-name-tool-${COMPILER_VERSION_MAJOR}" "llvm-install-name-tool")
endif ()

if (LLVM_INSTALL_NAME_TOOL_PATH)
    set (CMAKE_INSTALL_NAME_TOOL "${LLVM_INSTALL_NAME_TOOL_PATH}")
endif ()

# Objcopy

if (COMPILER_GCC)
    find_program (OBJCOPY_PATH NAMES "llvm-objcopy" "llvm-objcopy-13" "llvm-objcopy-12" "llvm-objcopy-11" "objcopy")
else ()
    find_program (OBJCOPY_PATH NAMES "llvm-objcopy-${COMPILER_VERSION_MAJOR}" "llvm-objcopy" "objcopy")
endif ()

if (NOT OBJCOPY_PATH AND OS_DARWIN)
    find_program (BREW_PATH NAMES "brew")
    if (BREW_PATH)
        execute_process (COMMAND ${BREW_PATH} --prefix llvm ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE LLVM_PREFIX)
        if (LLVM_PREFIX)
            find_program (OBJCOPY_PATH NAMES "llvm-objcopy" PATHS "${LLVM_PREFIX}/bin" NO_DEFAULT_PATH)
        endif ()
        if (NOT OBJCOPY_PATH)
            execute_process (COMMAND ${BREW_PATH} --prefix binutils ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE BINUTILS_PREFIX)
            if (BINUTILS_PREFIX)
                find_program (OBJCOPY_PATH NAMES "objcopy" PATHS "${BINUTILS_PREFIX}/bin" NO_DEFAULT_PATH)
            endif ()
        endif ()
    endif ()
endif ()

if (OBJCOPY_PATH)
    message (STATUS "Using objcopy: ${OBJCOPY_PATH}")
else ()
    message (FATAL_ERROR "Cannot find objcopy.")
endif ()

# Strip (FIXME copypaste)

if (COMPILER_GCC)
    find_program (STRIP_PATH NAMES "llvm-strip" "llvm-strip-13" "llvm-strip-12" "llvm-strip-11" "strip")
else ()
    find_program (STRIP_PATH NAMES "llvm-strip-${COMPILER_VERSION_MAJOR}" "llvm-strip" "strip")
endif ()

if (NOT STRIP_PATH AND OS_DARWIN)
    find_program (BREW_PATH NAMES "brew")
    if (BREW_PATH)
        execute_process (COMMAND ${BREW_PATH} --prefix llvm ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE LLVM_PREFIX)
        if (LLVM_PREFIX)
            find_program (STRIP_PATH NAMES "llvm-strip" PATHS "${LLVM_PREFIX}/bin" NO_DEFAULT_PATH)
        endif ()
        if (NOT STRIP_PATH)
            execute_process (COMMAND ${BREW_PATH} --prefix binutils ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE BINUTILS_PREFIX)
            if (BINUTILS_PREFIX)
                find_program (STRIP_PATH NAMES "strip" PATHS "${BINUTILS_PREFIX}/bin" NO_DEFAULT_PATH)
            endif ()
        endif ()
    endif ()
endif ()

if (STRIP_PATH)
    message (STATUS "Using strip: ${STRIP_PATH}")
else ()
    message (FATAL_ERROR "Cannot find strip.")
endif ()
