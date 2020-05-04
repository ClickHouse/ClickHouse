if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
    # It's very difficult to check what is the correspondence between clang and AppleClang versions.
    # There are many complaints that some version of AppleClang does not work, but we are not able to dig into it.
    message(FATAL_ERROR "AppleClang compiler is not supported. You have to use the latest clang version.")
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
    # Require minimum version of clang
    set (CLANG_MINIMUM_VERSION 8)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
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
