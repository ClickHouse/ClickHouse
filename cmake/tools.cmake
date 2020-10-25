if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    set (COMPILER_CLANG 1) # Safe to treat AppleClang as a regular Clang, in general.
elseif (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    set (COMPILER_CLANG 1)
endif ()

if (COMPILER_GCC)
    # Require minimum version of gcc
    set (GCC_MINIMUM_VERSION 9)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${GCC_MINIMUM_VERSION} AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least ${GCC_MINIMUM_VERSION}. For example, if GCC ${GCC_MINIMUM_VERSION} is available under gcc-${GCC_MINIMUM_VERSION}, g++-${GCC_MINIMUM_VERSION} names, do the following: export CC=gcc-${GCC_MINIMUM_VERSION} CXX=g++-${GCC_MINIMUM_VERSION}; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()
elseif (COMPILER_CLANG)
    # Require minimum version of clang/apple-clang
    if (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
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
        set (CLANG_MINIMUM_VERSION 8)
        if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
            message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
        endif ()
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler. Compilation has only been tested with Clang and GCC.")
endif ()

STRING(REGEX MATCHALL "[0-9]+" COMPILER_VERSION_LIST ${CMAKE_CXX_COMPILER_VERSION})
LIST(GET COMPILER_VERSION_LIST 0 COMPILER_VERSION_MAJOR)

option (LINKER_NAME "Linker name or full path")
if (COMPILER_GCC)
    find_program (LLD_PATH NAMES "ld.lld")
    find_program (GOLD_PATH NAMES "ld.gold")
else ()
    find_program (LLD_PATH NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "lld-${COMPILER_VERSION_MAJOR}" "ld.lld" "lld")
    find_program (GOLD_PATH NAMES "ld.gold" "gold")
endif ()

if (OS_LINUX)
    # We prefer LLD linker over Gold or BFD on Linux.
    if (NOT LINKER_NAME)
        if (LLD_PATH)
            if (COMPILER_GCC)
                # GCC driver requires one of supported linker names like "lld".
                set (LINKER_NAME "lld")
            else ()
                # Clang driver simply allows full linker path.
                set (LINKER_NAME ${LLD_PATH})
            endif ()
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
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")

    message(STATUS "Using custom linker by name: ${LINKER_NAME}")
endif ()
