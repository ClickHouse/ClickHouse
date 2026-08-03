# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-linux-musl")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-linux-musl")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-linux-musl")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-x86_64-musl")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")

set (USE_MUSL 1)
add_definitions(-DUSE_MUSL=1 -D__MUSL__=1)
