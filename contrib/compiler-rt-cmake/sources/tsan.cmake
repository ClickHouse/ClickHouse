# TSan core sources live in lib/tsan/rtl/ (relative paths below).
#
# Source: contrib/llvm-project/compiler-rt/lib/tsan/rtl/CMakeLists.txt
#   - TSAN_SOURCES is the merger of upstream TSAN_SOURCES + the per-platform
#     additions; we always pick the Unix/Linux additions (tsan_platform_linux.cpp,
#     tsan_platform_posix.cpp). The Apple-only files are intentionally omitted —
#     they reference Mach VM headers and don't contain platform guards that make
#     them no-ops on Linux.
#   - TSAN_PREINIT_SOURCES (tsan_preinit.cpp)
#   - TSAN_CXX_SOURCES (tsan_new_delete.cpp)
#   - Per-arch tsan_rtl_<arch>.S files are listed below; the upstream cmake
#     selects exactly one per build via TSAN_ASM_SOURCES.

set(TSAN_SOURCES
    tsan_debugging.cpp
    tsan_external.cpp
    tsan_fd.cpp
    tsan_flags.cpp
    tsan_ignoreset.cpp
    tsan_interceptors_memintrinsics.cpp
    tsan_interceptors_posix.cpp
    tsan_interface.cpp
    tsan_interface_ann.cpp
    tsan_interface_atomic.cpp
    tsan_interface_java.cpp
    tsan_malloc_mac.cpp
    tsan_md5.cpp
    tsan_mman.cpp
    tsan_mutexset.cpp
    tsan_report.cpp
    tsan_rtl.cpp
    tsan_rtl_access.cpp
    tsan_rtl_mutex.cpp
    tsan_rtl_proc.cpp
    tsan_rtl_report.cpp
    tsan_rtl_thread.cpp
    tsan_stack_trace.cpp
    tsan_suppressions.cpp
    tsan_symbolize.cpp
    tsan_sync.cpp
    tsan_vector_clock.cpp
    # Unix/Linux-specific platform sources (the file has internal #ifdefs and
    # compiles to no-ops on other platforms).
    tsan_platform_linux.cpp
    tsan_platform_posix.cpp
)

set(TSAN_PREINIT_SOURCES tsan_preinit.cpp)

set(TSAN_CXX_SOURCES tsan_new_delete.cpp)

# Per-arch low-level TLS / signal handling assembly.
set(TSAN_X86_64_ASM_SOURCES tsan_rtl_amd64.S)
set(TSAN_AARCH64_ASM_SOURCES tsan_rtl_aarch64.S)
set(TSAN_PPC64LE_ASM_SOURCES tsan_rtl_ppc64.S)
set(TSAN_LOONGARCH64_ASM_SOURCES tsan_rtl_loongarch64.S)
set(TSAN_RISCV64_ASM_SOURCES tsan_rtl_riscv64.S)
set(TSAN_S390X_ASM_SOURCES tsan_rtl_s390x.S)
