if (CMAKE_SYSTEM_NAME MATCHES "Linux")
    set (OS_LINUX 1)
    add_definitions(-D OS_LINUX)
elseif (CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    set (OS_FREEBSD 1)
    add_definitions(-D OS_FREEBSD)
elseif (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set (OS_DARWIN 1)
    add_definitions(-D OS_DARWIN)
endif ()

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

string(REGEX MATCH "-?[0-9]+(.[0-9]+)?$" COMPILER_POSTFIX ${CMAKE_CXX_COMPILER})

if (OS_LINUX)
    find_program (LLD_PATH NAMES "lld${COMPILER_POSTFIX}" "lld")
    find_program (GOLD_PATH NAMES "ld.gold" "gold")
endif()

option (LINKER_NAME "Linker name or full path")
if (NOT LINKER_NAME)
    if (COMPILER_CLANG AND LLD_PATH)
        set (LINKER_NAME "lld")
    elseif (GOLD_PATH)
        set (LINKER_NAME "gold")
    endif ()
endif ()

if (LINKER_NAME)
    message(STATUS "Using linker: ${LINKER_NAME} (selected from: LLD_PATH=${LLD_PATH}; GOLD_PATH=${GOLD_PATH}; COMPILER_POSTFIX=${COMPILER_POSTFIX})")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${LINKER_NAME}")
endif ()

if (CMAKE_CROSSCOMPILING)
    if (OS_DARWIN)
        set (CMAKE_SYSTEM_PROCESSOR x86_64)
        set (CMAKE_C_COMPILER_TARGET x86_64-apple-darwin)
        set (CMAKE_CXX_COMPILER_TARGET x86_64-apple-darwin)

        set (HAS_PRE_1970_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
        set (HAS_PRE_1970_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

        set (HAS_POST_2038_EXITCODE "0" CACHE STRING "Result from TRY_RUN" FORCE)
        set (HAS_POST_2038_EXITCODE__TRYRUN_OUTPUT "" CACHE STRING "Output from TRY_RUN" FORCE)

        # FIXME: broken dependencies
        set (USE_SNAPPY OFF CACHE INTERNAL "")
        set (ENABLE_SSL OFF CACHE INTERNAL "")
        set (ENABLE_PROTOBUF OFF CACHE INTERNAL "")
        set (ENABLE_PARQUET OFF CACHE INTERNAL "")
        set (ENABLE_READLINE OFF CACHE INTERNAL "")
        set (ENABLE_ICU OFF CACHE INTERNAL "")
        set (ENABLE_FASTOPS OFF CACHE INTERNAL "")

        message (STATUS "Cross-compiling for Darwin")
    else ()
        message (FATAL_ERROR "Trying to cross-compile to unsupported target: ${CMAKE_SYSTEM_NAME}!")
    endif ()

    # Don't know why but CXX_STANDARD doesn't work for cross-compilation
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
endif ()
