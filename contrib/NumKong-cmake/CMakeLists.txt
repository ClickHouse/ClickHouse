# See contrib/usearch-cmake/CMakeLists.txt, why only enabled on x86 and ARM
if (ARCH_AMD64 OR (ARCH_AARCH64 AND NOT NO_ARMV81_OR_HIGHER))
    set(NUMKONG_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/NumKong")
    set(NUMKONG_SRCS
        ${NUMKONG_PROJECT_DIR}/c/numkong.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_bf16.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_bf16c.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_e2m3.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_e3m2.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_e4m3.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_e5m2.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f16.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f16c.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f32.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f32c.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f64.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_f64c.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_i16.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_i32.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_i4.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_i64.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_i8.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_other.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u1.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u16.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u32.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u4.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u64.c
        ${NUMKONG_PROJECT_DIR}/c/dispatch_u8.c
    )
    add_library(_numkong ${NUMKONG_SRCS})
    target_include_directories(_numkong SYSTEM PUBLIC "${NUMKONG_PROJECT_DIR}/include")
    # Consumers call the dispatched `nk_*` entry points from `c/numkong.c` instead of compiling the kernels inline.
    target_compile_definitions(_numkong PUBLIC NK_DYNAMIC_DISPATCH=1)

    # With dynamic dispatch, the kernels for every instruction set are compiled with per-function `target`
    # attributes and picked at runtime, but only for the instruction sets enabled here: without an explicit
    # `NK_TARGET_*`, `numkong/types.h` falls back to the compiler flags of the build, i.e. the baseline ISA only.
    # Left out:
    # - AMX (`SAPPHIREAMX`, `GRANITEAMX`) and SME: only used by matrix kernels, which ClickHouse does not call;
    # - `DIAMOND` (AVX10.2) and `NEONFP8`: need newer compilers than the minimum supported Clang;
    # - SVE: LLVM's MemorySanitizer cannot instrument SVE scalable vector types and emits an unconditional
    #   `__msan_warning_noreturn` at every SVE function entry.
    if (ARCH_AMD64)
        target_compile_definitions(_numkong PRIVATE
            NK_TARGET_HASWELL=1 NK_TARGET_SKYLAKE=1 NK_TARGET_ICELAKE=1 NK_TARGET_GENOA=1 NK_TARGET_SAPPHIRE=1
            NK_TARGET_TURIN=1 NK_TARGET_ALDER=1 NK_TARGET_SIERRA=1
            NK_TARGET_SAPPHIREAMX=0 NK_TARGET_GRANITEAMX=0 NK_TARGET_DIAMOND=0)
    else ()
        target_compile_definitions(_numkong PRIVATE
            NK_TARGET_NEON=1 NK_TARGET_NEONHALF=1 NK_TARGET_NEONSDOT=1 NK_TARGET_NEONBFDOT=1 NK_TARGET_NEONFHM=1
            NK_TARGET_NEONFP8=0
            NK_TARGET_SVE=0 NK_TARGET_SVEHALF=0 NK_TARGET_SVEBFDOT=0 NK_TARGET_SVESDOT=0 NK_TARGET_SVE2=0 NK_TARGET_SVE2P1=0
            NK_TARGET_SME=0 NK_TARGET_SME2=0 NK_TARGET_SME2P1=0 NK_TARGET_SMEF64=0 NK_TARGET_SMEHALF=0
            NK_TARGET_SMEBF16=0 NK_TARGET_SMEBI32=0 NK_TARGET_SMELUT2=0 NK_TARGET_SMEFA64=0)
    endif ()
    add_library(ch_contrib::numkong ALIAS _numkong)
else()
    message(STATUS "NumKong library is only supported on x86_64 with SSE or ARM with ARMv8.1")
endif()
