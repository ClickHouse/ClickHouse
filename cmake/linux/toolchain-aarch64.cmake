# See linux/toolchain-x86_64.cmake for details about multiple load of toolchain file.
include_guard(GLOBAL)

set (CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

set (CMAKE_SYSTEM_NAME "Linux")
set (CMAKE_SYSTEM_PROCESSOR "aarch64")

# When USE_MUSL is enabled, target musl instead of glibc
if (USE_MUSL)
    set (CMAKE_C_COMPILER_TARGET "aarch64-linux-musl")
    set (CMAKE_CXX_COMPILER_TARGET "aarch64-linux-musl")
    set (CMAKE_ASM_COMPILER_TARGET "aarch64-linux-musl")

    # Use glibc sysroot only for kernel headers (linux/, asm/, asm-generic/)
    # Musl headers are provided with higher priority via -isystem flags below
    set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-aarch64")
    set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/aarch64-linux-gnu/libc")

    # Define musl macros
    add_definitions(-DUSE_MUSL=1 -D__MUSL__=1)

    # Add minimal musl include paths via toolchain - just generated headers and main include
    # NOTE: We do NOT add arch dirs here because they get deduplicated with musl target's
    # PRIVATE includes, causing the wrong include order when building musl itself.
    # Libraries that need arch dirs (for bits/*.h) should link to musl or add them explicitly.
    set (MUSL_SOURCE_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/musl")
    set (MUSL_INCLUDE_FLAGS "-isystem ${CMAKE_BINARY_DIR}/contrib/musl-cmake/include -isystem ${MUSL_SOURCE_PATH}/include")

    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${MUSL_INCLUDE_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MUSL_INCLUDE_FLAGS}")
    set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} ${MUSL_INCLUDE_FLAGS}")
else()
    set (CMAKE_C_COMPILER_TARGET "aarch64-linux-gnu")
    set (CMAKE_CXX_COMPILER_TARGET "aarch64-linux-gnu")
    set (CMAKE_ASM_COMPILER_TARGET "aarch64-linux-gnu")

    set (TOOLCHAIN_PATH "${CMAKE_CURRENT_LIST_DIR}/../../contrib/sysroot/linux-aarch64")
    set (CMAKE_SYSROOT "${TOOLCHAIN_PATH}/aarch64-linux-gnu/libc")

    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
    set (CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} --gcc-toolchain=${TOOLCHAIN_PATH}")
endif()
