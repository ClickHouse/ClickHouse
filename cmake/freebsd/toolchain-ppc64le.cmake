# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_SYSTEM_NAME "FreeBSD")
set (CMAKE_SYSTEM_PROCESSOR "ppc64le")
set (CMAKE_C_COMPILER_TARGET "powerpc64le-unknown-freebsd13")
set (CMAKE_CXX_COMPILER_TARGET "powerpc64le-unknown-freebsd13")
set (CMAKE_ASM_COMPILER_TARGET "powerpc64le-unknown-freebsd13")
set (CMAKE_SYSROOT "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/freebsd-ppc64le")

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)  # disable linkage check - it doesn't work in CMake
