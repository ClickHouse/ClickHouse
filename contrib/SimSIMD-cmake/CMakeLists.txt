# See contrib/usearch-cmake/CMakeLists.txt, why only enabled on x86 and ARM.
#
# ARM sanitizer builds are excluded: SimSIMD kills the server on ARM CPUs without SVE
# (Scalable Vector Extension, an optional ARM feature).
#
# SimSIMD detects CPU features in `_simsimd_capabilities_arm`. That function sits inside
# `#pragma GCC target("arch=armv8.5-a+sve")` (contrib/SimSIMD/include/simsimd/simsimd.h:475).
# The pragma lets the compiler emit SVE instructions there - in the very function that asks
# whether the CPU supports SVE. Sanitizer codegen emits one. The CPU raises SIGILL and the
# server dies.
#
# Neoverse N1 and Graviton2 have no SVE, and our supported ARM baseline covers both.
# `Stress test (arm_tsan)` hits this through 03369_l2_distance_transposed_variadic.
#
# `SIMSIMD_TARGET_SVE=0` below does not prevent it. That macro gates the SVE kernels, not the
# pragma.
#
# Drop this exclusion once the probe is fixed upstream. Until then the QBit distance functions
# and USearch use their scalar implementations on these builds.
if (ARCH_AMD64 OR (ARCH_AARCH64 AND SANITIZE STREQUAL "" AND NOT NO_ARMV81_OR_HIGHER AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL 19))
    set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
    set(SIMSIMD_SRCS ${SIMSIMD_PROJECT_DIR}/c/lib.c)
    add_library(_simsimd ${SIMSIMD_SRCS})
    target_include_directories(_simsimd SYSTEM PUBLIC "${SIMSIMD_PROJECT_DIR}/include")
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_DYNAMIC_DISPATCH)
    # Disable SVE: LLVM's MemorySanitizer cannot instrument SVE scalable vector types,
    # emitting unconditional `__msan_warning_noreturn` at every SVE function entry.
    target_compile_definitions(_simsimd PRIVATE SIMSIMD_TARGET_SVE=0 SIMSIMD_TARGET_SVE2=0)
    add_library(ch_contrib::simsimd ALIAS _simsimd)
else()
    # The latter requires Clang 19 because older versions had a buggy bf16 implementation.
    message(STATUS "SimSIMD library is only supported on x86_64 with SSE or ARM with ARMv8.1 and Clang 19+, and is disabled for ARM sanitizer builds")
endif()
