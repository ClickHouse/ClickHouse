# Build and link musl libc from source
# Math functions are provided by LLVM-libc (see cmake/libllvmlibc.cmake)
#
# This file should only be included when USE_MUSL is ON.
# It is included from cmake/linux/default_libs.cmake

if (NOT USE_MUSL)
    message(FATAL_ERROR "cmake/musl.cmake should only be included when USE_MUSL is ON")
endif()

# MUSL_ARCH should already be set by default_libs.cmake before this file is included
# This is necessary because cxx.cmake (libcxx/libcxxabi) needs MUSL_ARCH before musl.cmake runs
if (NOT MUSL_ARCH)
    message(FATAL_ERROR "MUSL_ARCH not set - this should be set in default_libs.cmake before including musl.cmake")
endif()

# Link our built musl library
target_link_libraries(global-libs INTERFACE musl)

# ============================================================================
# CRT Objects - Build Only
# ============================================================================
# The CRT objects are built in contrib/musl-cmake/CMakeLists.txt.
# They will be linked by the clickhouse_add_executable macro in the main CMakeLists.txt.
# 
# We just need to make sure the targets are available globally.
# The actual linking happens in clickhouse_add_executable with $<TARGET_OBJECTS:...>
#
# Note: We don't add CRT objects via global-libs because CMake's ninja generator
# has issues with $<TARGET_OBJECTS:...> in INTERFACE_LINK_LIBRARIES.

# Export musl headers for system-wide use
# Priority order (highest to lowest):
# 1. Generated headers (bits/alltypes.h, bits/syscall.h)
# 2. Architecture-specific headers (arch/x86_64/bits/*.h)
# 3. Generic arch headers (arch/generic/bits/*.h)
# 4. Musl public headers (include/*.h)
# 5. Kernel headers from glibc sysroot (linux/, asm/, asm-generic/)

target_include_directories(global-libs SYSTEM BEFORE INTERFACE
    # Generated headers - HIGHEST PRIORITY
    "${ClickHouse_BINARY_DIR}/contrib/musl-cmake/include"
    # Architecture-specific headers
    "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/${MUSL_ARCH}"
    # Generic arch headers (fallback for bits/*.h not in arch-specific)
    "${ClickHouse_SOURCE_DIR}/contrib/musl/arch/generic"
    # Musl public headers
    "${ClickHouse_SOURCE_DIR}/contrib/musl/include"
)

# Kernel headers from glibc sysroot (only linux/, asm/, asm-generic/)
# These are libc-agnostic and won't pull in glibc symbols
if (ARCH_AMD64)
    set(KERNEL_HEADERS_SYSROOT "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-x86_64/x86_64-linux-gnu/libc/usr/include")
    target_include_directories(global-libs SYSTEM INTERFACE
        "${KERNEL_HEADERS_SYSROOT}"
        "${KERNEL_HEADERS_SYSROOT}/x86_64-linux-gnu"
    )
elseif(ARCH_AARCH64)
    set(KERNEL_HEADERS_SYSROOT "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-aarch64/aarch64-linux-gnu/libc/usr/include")
    target_include_directories(global-libs SYSTEM INTERFACE
        "${KERNEL_HEADERS_SYSROOT}"
        "${KERNEL_HEADERS_SYSROOT}/aarch64-linux-gnu"
    )
elseif(ARCH_PPC64LE)
    set(KERNEL_HEADERS_SYSROOT "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-powerpc64le/powerpc64le-linux-gnu/libc/usr/include")
    target_include_directories(global-libs SYSTEM INTERFACE
        "${KERNEL_HEADERS_SYSROOT}"
        "${KERNEL_HEADERS_SYSROOT}/powerpc64le-linux-gnu"
    )
elseif(ARCH_S390X)
    set(KERNEL_HEADERS_SYSROOT "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-s390x/s390x-linux-gnu/libc/usr/include")
    target_include_directories(global-libs SYSTEM INTERFACE
        "${KERNEL_HEADERS_SYSROOT}"
        "${KERNEL_HEADERS_SYSROOT}/s390x-linux-gnu"
    )
elseif(ARCH_RISCV64)
    set(KERNEL_HEADERS_SYSROOT "${ClickHouse_SOURCE_DIR}/contrib/sysroot/linux-riscv64/usr/include")
    target_include_directories(global-libs SYSTEM INTERFACE
        "${KERNEL_HEADERS_SYSROOT}"
    )
else()
    message(WARNING "No kernel headers sysroot configured for this architecture with USE_MUSL")
endif()

message(STATUS "Using built musl libc (math functions from LLVM-libc)")
message(STATUS "  Musl architecture: ${MUSL_ARCH}")
message(STATUS "  Musl headers: ${ClickHouse_SOURCE_DIR}/contrib/musl/include")
message(STATUS "  Generated headers: ${ClickHouse_BINARY_DIR}/contrib/musl-cmake/include")
if (KERNEL_HEADERS_SYSROOT)
    message(STATUS "  Kernel headers: ${KERNEL_HEADERS_SYSROOT}")
endif()
