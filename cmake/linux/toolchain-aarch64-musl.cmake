# Toolchain for a fully static binary against musl libc built from sources (see contrib/musl-cmake).
# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "aarch64")
set (CMAKE_C_COMPILER_TARGET "aarch64-linux-musl")
set (CMAKE_CXX_COMPILER_TARGET "aarch64-linux-musl")
set (CMAKE_ASM_COMPILER_TARGET "aarch64-linux-musl")

# The glibc sysroot is used only for kernel headers (linux/, asm/, asm-generic/);
# musl's own headers are given higher priority via the -isystem flags below.
set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-aarch64")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/aarch64-linux-gnu/libc")

# -nostdlibinc drops the sysroot's glibc userspace headers from the default search
# path; musl's headers are passed explicitly instead, and the kernel (uapi) headers
# are whitelisted through the kernel-headers symlink directory created in
# cmake/musl.cmake. These flags come after any target include directories, so they
# do not disturb the include order while building musl itself.
set (MUSL_SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl")
set (MUSL_STUB_INCLUDE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl-cmake/include")
set (MUSL_INCLUDE_FLAGS "-nostdlibinc -isystem ${MUSL_STUB_INCLUDE_PATH} -isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/include -isystem ${MUSL_SOURCE_PATH}/include -isystem ${MUSL_SOURCE_PATH}/arch/aarch64 -isystem ${MUSL_SOURCE_PATH}/arch/generic -isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/kernel-headers")

# Make sure to ignore global clang configuration files which could influence the
# build environment using --no-default-config
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} ${MUSL_INCLUDE_FLAGS} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")

set (USE_MUSL 1)
# musl's name for the target architecture: the arch/<MUSL_ARCH> directory in the musl sources.
set (MUSL_ARCH "aarch64")
add_definitions(-DUSE_MUSL=1 -D__MUSL__=1)

# On aarch64, the kernel UAPI headers pulled in via <asm/ptrace.h> (asm/sigcontext.h,
# linux/prctl.h) and via <linux/sysctl.h> (linux/sysinfo.h) redefine structs that musl's own
# headers (bits/signal.h, sys/prctl.h, sys/sysinfo.h) already provide with identical layout -
# whichever of the two is included second in a given translation unit fails to compile. This
# does not happen on x86_64: its asm/ptrace.h does not pull in asm/sigcontext.h at all. Rather
# than patching every affected source file (ClickHouse's own, or any contrib, present or
# future) as it is discovered, block the kernel copy everywhere via its own include guards;
# musl's copy, which every such translation unit needs anyway on this toolchain, then wins
# regardless of include order.
add_definitions(-D__ASM_SIGCONTEXT_H -D_LINUX_PRCTL_H -D_LINUX_SYSINFO_H)
