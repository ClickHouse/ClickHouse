# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_SYSTEM_NAME "Darwin")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_OSX_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../toolchain/darwin-x86_64")

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # disable linkage check - it doesn't work in CMake
