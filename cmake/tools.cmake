if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (COMPILER_GCC 1)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set (COMPILER_CLANG 1)
endif ()

if (COMPILER_GCC)
    # Require minimum version of gcc
    set (GCC_MINIMUM_VERSION 8)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${GCC_MINIMUM_VERSION} AND NOT CMAKE_VERSION VERSION_LESS 2.8.9)
        message (FATAL_ERROR "GCC version must be at least ${GCC_MINIMUM_VERSION}. For example, if GCC ${GCC_MINIMUM_VERSION} is available under gcc-${GCC_MINIMUM_VERSION}, g++-${GCC_MINIMUM_VERSION} names, do the following: export CC=gcc-${GCC_MINIMUM_VERSION} CXX=g++-${GCC_MINIMUM_VERSION}; rm -rf CMakeCache.txt CMakeFiles; and re run cmake or ./release.")
    endif ()
elseif (COMPILER_CLANG)
    # Require minimum version of clang
    set (CLANG_MINIMUM_VERSION 7)
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS ${CLANG_MINIMUM_VERSION})
        message (FATAL_ERROR "Clang version must be at least ${CLANG_MINIMUM_VERSION}.")
    endif ()
else ()
    message (WARNING "You are using an unsupported compiler. Compilation has only been tested with Clang 6+ and GCC 7+.")
endif ()

option (LINKER_NAME "Linker name or full path")

find_program (LLD_PATH NAMES "ld.lld" "lld")
find_program (GOLD_PATH NAMES "ld.gold" "gold")

if (NOT LINKER_NAME)
    if (LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")

    message(STATUS "Using custom linker by name: ${LINKER_NAME}")
endif ()
