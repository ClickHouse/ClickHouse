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

# Only the generated headers and the main include dir; the arch dirs are added
# per-target where needed, to keep the include order correct while building musl itself.
set (MUSL_SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl")
set (MUSL_INCLUDE_FLAGS "-isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/include -isystem ${MUSL_SOURCE_PATH}/include")

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
