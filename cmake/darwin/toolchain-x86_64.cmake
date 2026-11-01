# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_SYSTEM_NAME "Darwin")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-apple-darwin")
set (CMAKE_OSX_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../toolchain/darwin-x86_64")

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # disable linkage check - it doesn't work in CMake

# Make sure to ignore global clang configuration files which could influence the
# build environment using --no-default-config
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --no-default-config")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --no-default-config")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --no-default-config")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --no-default-config")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --no-default-config")
