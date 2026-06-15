# Toolchain for building a fully static aarch64 binary against musl libc, with
# musl built from sources (see contrib/musl-cmake). Select it with
# -DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64-musl.cmake; it sets
# USE_MUSL itself, so no separate -DUSE_MUSL=1 is needed.
#
# The generic linux/toolchain-aarch64.cmake stays glibc-only on purpose; all
# musl-specific setup lives here.
#
# See linux/toolchain-x86_64.cmake for details about the double load of the
# toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "aarch64")

set (CMAKE_C_COMPILER_TARGET "aarch64-linux-musl")
set (CMAKE_CXX_COMPILER_TARGET "aarch64-linux-musl")
set (CMAKE_ASM_COMPILER_TARGET "aarch64-linux-musl")

# The glibc sysroot is used only for kernel headers (linux/, asm/,
# asm-generic/). musl's own headers are given higher priority via the -isystem
# flags below.
set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-aarch64")
set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/aarch64-linux-gnu/libc")

set (USE_MUSL 1)
add_definitions(-DUSE_MUSL=1 -D__MUSL__=1)

# Minimal musl include paths: just the generated headers and the main include
# dir. We deliberately do NOT add the arch dirs here — they get deduplicated
# with the musl target's PRIVATE includes, which would flip the include order
# while building musl itself. Libraries that need bits/*.h should link to the
# musl target or add the arch dirs explicitly.
set (MUSL_SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl")
set (MUSL_INCLUDE_FLAGS "-isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/include -isystem ${MUSL_SOURCE_PATH}/include")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MUSL_INCLUDE_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MUSL_INCLUDE_FLAGS}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} ${MUSL_INCLUDE_FLAGS}")

# Ignore global clang configuration files which could influence the build
# environment, using --no-default-config. No --gcc-toolchain: musl builds do
# not use the glibc GCC toolchain layout.
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")
