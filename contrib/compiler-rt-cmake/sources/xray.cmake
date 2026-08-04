# XRay sources are split across modes (FDR, basic, profiling) and architectures.
#
# Source: contrib/llvm-project/compiler-rt/lib/xray/CMakeLists.txt
#   - XRAY_COMMON_SOURCES   ← upstream XRAY_SOURCES (the common runtime)
#   - XRAY_FDR_SOURCES      ← upstream XRAY_FDR_MODE_SOURCES
#   - XRAY_BASIC_SOURCES    ← upstream XRAY_BASIC_MODE_SOURCES
#   - XRAY_PROFILING_SOURCES ← upstream XRAY_PROFILING_MODE_SOURCES
#   - XRAY_<ARCH>_SOURCES / XRAY_<ARCH>_ASM_SOURCES ← upstream
#     x86_64_SOURCES / arm64_SOURCES / etc., one per supported arch.
# Each runtime mode is bundled into a separate static archive in upstream
# (clang_rt.xray-fdr, clang_rt.xray-basic, ...). ClickHouse links a single
# combined libclang_rt_xray.a that includes all of them.

set(XRAY_COMMON_SOURCES
    xray_buffer_queue.cpp
    xray_init.cpp
    xray_flags.cpp
    xray_interface.cpp
    xray_log_interface.cpp
    xray_utils.cpp
)

set(XRAY_FDR_SOURCES
    xray_fdr_flags.cpp
    xray_fdr_logging.cpp
)

set(XRAY_BASIC_SOURCES
    xray_basic_flags.cpp
    xray_basic_logging.cpp
)

set(XRAY_PROFILING_SOURCES
    xray_profile_collector.cpp
    xray_profiling.cpp
    xray_profiling_flags.cpp
)

# Per-arch trampolines and arch entry points.
set(XRAY_X86_64_SOURCES xray_x86_64.cpp)
set(XRAY_X86_64_ASM_SOURCES xray_trampoline_x86_64.S)

set(XRAY_AARCH64_SOURCES xray_AArch64.cpp)
set(XRAY_AARCH64_ASM_SOURCES xray_trampoline_AArch64.S)

set(XRAY_PPC64LE_SOURCES
    xray_powerpc64.cpp
    xray_trampoline_powerpc64.cpp
)
set(XRAY_PPC64LE_ASM_SOURCES xray_trampoline_powerpc64_asm.S)

set(XRAY_RISCV64_SOURCES xray_riscv.cpp)
set(XRAY_RISCV64_ASM_SOURCES xray_trampoline_riscv64.S)

set(XRAY_S390X_SOURCES xray_s390x.cpp)
set(XRAY_S390X_ASM_SOURCES xray_trampoline_s390x.S)

set(XRAY_LOONGARCH64_SOURCES xray_loongarch64.cpp)
set(XRAY_LOONGARCH64_ASM_SOURCES xray_trampoline_loongarch64.S)
