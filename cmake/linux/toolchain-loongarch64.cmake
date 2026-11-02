# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "loongarch64")
set (CMAKE_C_COMPILER_TARGET "loongarch64-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "loongarch64-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "loongarch64-linux-gnu")

# Adding `-mcmodel=extreme` is to handle the link error:
#   relocation R_LARCH_B26 out of range: 194148892 is not in [-134217728, 134217727]
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcmodel=extreme")
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -mcmodel=extreme")

set (CMAKE_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-loongarch64")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-loongarch64/usr")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
