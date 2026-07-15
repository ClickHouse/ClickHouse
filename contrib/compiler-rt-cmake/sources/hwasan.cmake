# HWASan source lists, from contrib/llvm-project/compiler-rt/lib/hwasan/CMakeLists.txt
# (HWASAN_RTL_SOURCES / HWASAN_RTL_CXX_SOURCES / HWASAN_RTL_PREINIT_SOURCES). We only build
# for aarch64, so the aarch64-only asm (hwasan_setjmp/tag_mismatch_aarch64.S) is always included.

set(HWASAN_SOURCES
    hwasan.cpp
    hwasan_allocator.cpp
    hwasan_allocation_functions.cpp
    hwasan_dynamic_shadow.cpp
    hwasan_exceptions.cpp
    hwasan_fuchsia.cpp
    hwasan_globals.cpp
    hwasan_interceptors.cpp
    hwasan_interceptors_vfork.S
    hwasan_linux.cpp
    hwasan_memintrinsics.cpp
    hwasan_poisoning.cpp
    hwasan_report.cpp
    hwasan_thread.cpp
    hwasan_thread_list.cpp
    hwasan_type_test.cpp
    hwasan_setjmp_aarch64.S
    hwasan_tag_mismatch_aarch64.S
)

set(HWASAN_CXX_SOURCES hwasan_new_delete.cpp)

set(HWASAN_PREINIT_SOURCES hwasan_preinit.cpp)
