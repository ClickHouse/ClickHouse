# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "ppc64le")
set (CMAKE_C_COMPILER_TARGET "powerpc64le-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "powerpc64le-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "powerpc64le-linux-gnu")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-powerpc64le")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/powerpc64le-linux-gnu/libc")

# Make sure to ignore global clang configuration files which could influence the
# build environment using --no-default-config
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")
