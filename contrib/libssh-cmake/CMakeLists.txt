option (ENABLE_SSH "Enable support for SSH keys and protocol" ON)

if (NOT ENABLE_SSH)
    message(STATUS "Not using SSH")
    return()
endif()

set(LIB_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libssh")
set(LIB_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/libssh")

project(libssh VERSION 0.9.7 LANGUAGES C)

# global needed variable
set(APPLICATION_NAME ${PROJECT_NAME})

# SOVERSION scheme: CURRENT.AGE.REVISION
#   If there was an incompatible interface change:
#     Increment CURRENT. Set AGE and REVISION to 0
#   If there was a compatible interface change:
#     Increment AGE. Set REVISION to 0
#   If the source code was changed, but there were no interface changes:
#     Increment REVISION.
set(LIBRARY_VERSION "4.8.7")
set(LIBRARY_SOVERSION "4")

# Copy library files to a lib sub-directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${LIB_BINARY_DIR}/lib")

set(CMAKE_THREAD_PREFER_PTHREADS ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)

set(WITH_ZLIB OFF)
set(WITH_SYMBOL_VERSIONING OFF)
set(WITH_SERVER ON)

include(IncludeSources.cmake)
if (OS_LINUX)
    if (ARCH_AMD64)
        if (USE_MUSL)
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/x86-64-musl")
        else()
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/x86-64")
        endif ()
    elseif (ARCH_AARCH64)
        if (USE_MUSL)
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/aarch64-musl")
        else()
            target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/aarch64")
        endif ()
    elseif (ARCH_PPC64LE)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/ppc64le")
    elseif (ARCH_S390X)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/s390x")
    elseif (ARCH_RISCV64)
        target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/linux/riscv64")
    else ()
        message(FATAL_ERROR "Platform is not supported")
    endif ()
elseif (OS_DARWIN)
    target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/darwin")
elseif (OS_FREEBSD)
    target_include_directories(_ssh PRIVATE "${ClickHouse_SOURCE_DIR}/contrib/libssh-cmake/freebsd")
else ()
    message(FATAL_ERROR "Platform is not supported")
endif()

configure_file(${LIB_SOURCE_DIR}/include/libssh/libssh_version.h.cmake
               ${LIB_BINARY_DIR}/include/libssh/libssh_version.h
               @ONLY)
