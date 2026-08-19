# During first run of cmake the toolchain file will be loaded twice,
# - /usr/share/cmake-3.23/Modules/CMakeDetermineSystem.cmake
# - /bld/CMakeFiles/3.23.2/CMakeSystem.cmake
#
# But once you already have non-empty cmake cache it will be loaded only
# once:
# - /bld/CMakeFiles/3.23.2/CMakeSystem.cmake
#
# This has no harm except for double load of toolchain will add
# --gcc-toolchain multiple times that will not allow ccache to reuse the
# cache.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "x86_64")
set (CMAKE_C_COMPILER_TARGET "x86_64-linux-gnu")
set (CMAKE_CXX_COMPILER_TARGET "x86_64-linux-gnu")
set (CMAKE_ASM_COMPILER_TARGET "x86_64-linux-gnu")

set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-x86_64")

set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/x86_64-linux-gnu/libc")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
set (CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
