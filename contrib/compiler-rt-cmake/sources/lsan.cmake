# Source: contrib/llvm-project/compiler-rt/lib/lsan/CMakeLists.txt
#   - LSAN_COMMON_SOURCES is the upstream variable name.
#
# The "main" lsan runtime (lsan.cpp, lsan_allocator.cpp, ...) is *not* built —
# ClickHouse only enables LSan as part of ASan, where these helpers are
# already pulled into clang_rt.asan via the LSAN common helpers below.

set(LSAN_COMMON_SOURCES
    lsan_common.cpp
    lsan_common_fuchsia.cpp
    lsan_common_linux.cpp
    lsan_common_mac.cpp
)
