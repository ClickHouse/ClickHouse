# ASan is split across multiple static archives matching the upstream layout:
#   libclang_rt.asan.a        — preinit + main runtime + sanitizer_common bundled
#   libclang_rt.asan_static.a — only asan_rtl_static.cpp + asan_rtl_x86_64.S (x86_64)
#   libclang_rt.asan_cxx.a    — asan_new_delete.cpp
#
# Source: contrib/llvm-project/compiler-rt/lib/asan/CMakeLists.txt
#   - ASAN_SOURCES (the per-arch RTAsan source list)
#   - ASAN_PREINIT_SOURCES (asan_preinit.cpp)
#   - ASAN_CXX_SOURCES (asan_new_delete.cpp)
#   - ASAN_STATIC_SOURCES (asan_rtl_static.cpp; the x86_64 .S is only added if
#     CMAKE_ASM_FLAGS supports `-x assembler-with-cpp`)
#   - asan_interceptors_vfork.S is added by the upstream cmake when
#     "NOT WIN32 AND NOT APPLE".

set(ASAN_SOURCES
    asan_aix.cpp
    asan_allocator.cpp
    asan_activation.cpp
    asan_debugging.cpp
    asan_descriptions.cpp
    asan_errors.cpp
    asan_fake_stack.cpp
    asan_flags.cpp
    asan_fuchsia.cpp
    asan_globals.cpp
    asan_globals_win.cpp
    asan_interceptors.cpp
    asan_interceptors_memintrinsics.cpp
    asan_linux.cpp
    asan_mac.cpp
    asan_malloc_linux.cpp
    asan_malloc_mac.cpp
    asan_malloc_win.cpp
    asan_memory_profile.cpp
    asan_poisoning.cpp
    asan_posix.cpp
    asan_premap_shadow.cpp
    asan_report.cpp
    asan_rtl.cpp
    asan_shadow_setup.cpp
    asan_stack.cpp
    asan_stats.cpp
    asan_suppressions.cpp
    asan_thread.cpp
    asan_win.cpp
)

# vfork interception trampoline. Upstream gates it on "NOT WIN32 AND NOT APPLE",
# so it is included on every Unix-like target we build for. The same gate is
# applied where this list is consumed in CMakeLists.txt.
set(ASAN_ASM_SOURCES asan_interceptors_vfork.S)

set(ASAN_PREINIT_SOURCES asan_preinit.cpp)

set(ASAN_CXX_SOURCES asan_new_delete.cpp)

set(ASAN_STATIC_SOURCES asan_rtl_static.cpp)

# x86_64-only static asan_rtl assembly.
set(ASAN_STATIC_X86_64_ASM_SOURCES asan_rtl_x86_64.S)
