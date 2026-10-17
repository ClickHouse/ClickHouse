# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # disable linkage check - it doesn't work in CMake

set (CMAKE_SYSTEM_NAME "Windows")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")

# We target `windows-gnu` (mingw-w64) and not `windows-msvc`. The produced binaries are
# ordinary native PE executables - there is no emulation layer such as Cygwin involved -
# but they are compiled against the mingw-w64 headers and import libraries. Those are
# freely redistributable and packaged by every Linux distribution, so the cross-build
# needs nothing extracted from a Windows installation and no MSVC license.
set (CMAKE_C_COMPILER_TARGET "x86_64-w64-windows-gnu")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-w64-windows-gnu")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-w64-windows-gnu")

# The Win32 API headers (`windows.h`, ...), the CRT import libraries and `winpthreads`
# come from mingw-w64. Unlike the macOS SDK, which has to be unpacked into
# `cmake/toolchain`, mingw-w64 is a distribution package, so we consume it from its
# install location. The CI image installs it, see `ci/docker/binary-builder/Dockerfile`.
set (MINGW_SYSROOT "/usr/x86_64-w64-mingw32" CACHE PATH "Path to the mingw-w64 sysroot")

if (NOT EXISTS "${MINGW_SYSROOT}/include/windows.h")
    message (FATAL_ERROR
        "The mingw-w64 sysroot was not found at '${MINGW_SYSROOT}' "
        "(no 'include/windows.h' in it). Install it with 'apt-get install mingw-w64' "
        "or point -DMINGW_SYSROOT=... at an existing one.")
endif ()

set (CMAKE_SYSROOT "${MINGW_SYSROOT}")

# The sysroot contains only mingw-w64, so restricting the search to it keeps a stray
# host Linux library or header from being picked up by a `find_library`/`find_path` in
# a contrib. Programs are exempt: the compiler, linker and `llvm-ar` are host binaries.
set (CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set (CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set (CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set (CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

# Make sure to ignore global clang configuration files which could influence the
# build environment using --no-default-config
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")
