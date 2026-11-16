# Toolchain for a fully static binary against musl libc built from sources (see contrib/musl-cmake).
# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-linux-musl")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-linux-musl")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-linux-musl")

# The glibc sysroot is used only for kernel headers (linux/, asm/, asm-generic/);
# musl's own headers are given higher priority via the -isystem flags below.
set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-x86_64")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/x86_64-linux-gnu/libc")

# -nostdlibinc drops the sysroot's glibc userspace headers from the default search
# path; musl's headers are passed explicitly instead, and the kernel (uapi) headers
# are whitelisted through the kernel-headers symlink directory created in
# cmake/musl.cmake. These flags come after any target include directories, so they
# do not disturb the include order while building musl itself.
set (MUSL_SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl")
set (MUSL_STUB_INCLUDE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl-cmake/include")
set (MUSL_INCLUDE_FLAGS "-nostdlibinc -isystem ${MUSL_STUB_INCLUDE_PATH} -isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/include -isystem ${MUSL_SOURCE_PATH}/include -isystem ${MUSL_SOURCE_PATH}/arch/x86_64 -isystem ${MUSL_SOURCE_PATH}/arch/generic -isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/kernel-headers")

# Make sure to ignore global clang configuration files which could influence the
# build environment using --no-default-config
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")

set (USE_MUSL 1)
# musl's name for the target architecture: the arch/<MUSL_ARCH> directory in the musl sources.
set (MUSL_ARCH "x86_64")
add_definitions(-DUSE_MUSL=1 -D__MUSL__=1)
