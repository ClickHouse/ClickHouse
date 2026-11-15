# See contrib/usearch-cmake/CMakeLists.txt, why only enabled on x86 and ARM
if (ARCH_AMD64 OR (ARCH_AARCH64 AND NOT NO_ARMV81_OR_HIGHER))
    set(SIMSIMD_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SimSIMD")
    set(SIMSIMD_SRCS ${SIMSIMD_PROJECT_DIR}/c/lib.c)
    add_library(_simsimd ${SIMSIMD_SRCS})
    target_include_directories(_simsimd SYSTEM PUBLIC "${SIMSIMD_PROJECT_DIR}/include")
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_DYNAMIC_DISPATCH)
    # No native bfloat16: on x86 SimSIMD spells it `__bfloat16`, which needs `<immintrin.h>`, included only from
    # `SIMSIMD_TARGET_HASWELL` up, so `X86_ARCH_LEVEL` below 3 fails to compile. Turning it off costs nothing and emits
    # the same code: with `SIMSIMD_DYNAMIC_DISPATCH` the bf16 entry points live in `c/lib.c`, which already sets this to
    # 0, so the flag only picks the `simsimd_bf16_t` typedef. Hence off everywhere rather than below level 3 only.
    target_compile_definitions(_simsimd PUBLIC SIMSIMD_NATIVE_BF16=0)
    # Disable SVE: LLVM's MemorySanitizer cannot instrument SVE scalable vector types,
    # emitting unconditional `__msan_warning_noreturn` at every SVE function entry.
    target_compile_definitions(_simsimd PRIVATE SIMSIMD_TARGET_SVE=0 SIMSIMD_TARGET_SVE2=0)
    # No return address signing. SimSIMD compiles some functions for ARMv8.3 and later (`target("arch=armv8.5-a+sve")`
    # and the like), and in a function whose target has `FEAT_PAuth` clang folds `autiasp; ret` from the inherited
    # `-mbranch-protection=pac-ret` into `retaa`. Unlike `paciasp` and `autiasp`, `retaa` is not in the `NOP` space,
    # so it raises SIGILL on cores without `FEAT_PAuth` (Graviton2, Ampere Altra). One of these functions is the CPU
    # probe `_simsimd_capabilities_arm`, which runs on every core, before anything is dispatched.
    # See https://github.com/ClickHouse/ClickHouse/issues/131748 and the check in `ci/jobs/compatibility_check.py`.
    if (ARCH_AARCH64)
        target_compile_options(_simsimd PRIVATE -mbranch-protection=none)
    endif()
    add_library(ch_contrib::simsimd ALIAS _simsimd)
else()
    message(STATUS "SimSIMD library is only supported on x86_64 with SSE or ARM with ARMv8.1")
endif()
