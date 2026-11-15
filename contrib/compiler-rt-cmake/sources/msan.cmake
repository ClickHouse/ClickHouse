# Source: contrib/llvm-project/compiler-rt/lib/msan/CMakeLists.txt
#   - MSAN_SOURCES         ← upstream MSAN_RTL_SOURCES
#   - MSAN_CXX_SOURCES     ← upstream MSAN_RTL_CXX_SOURCES (msan_new_delete.cpp)
#
# No arch-specific files for x86_64/aarch64 in MSan.

set(MSAN_SOURCES
    msan.cpp
    msan_allocator.cpp
    msan_chained_origin_depot.cpp
    msan_dl.cpp
    msan_interceptors.cpp
    msan_linux.cpp
    msan_report.cpp
    msan_thread.cpp
    msan_poisoning.cpp
)

set(MSAN_CXX_SOURCES msan_new_delete.cpp)
