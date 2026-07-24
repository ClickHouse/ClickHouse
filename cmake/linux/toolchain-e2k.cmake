# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "e2k")
set (CMAKE_C_COMPILER_TARGET "e2k-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "e2k-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "e2k-linux-gnu")

# Adding `-mcmodel=large` is to handle the link error:
#   relocation R_E2K_32_ABS out of range: 2273892512 is not in [-2147483648, 2147483647]
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcmodel=large")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcmodel=large")

#set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-e2k")

#set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}")

#set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
#set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
