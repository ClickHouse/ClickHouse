// Vendored LLVM-libc memory functions (memcmp, memcpy, memmove, memset, bcmp).
//
// Each function's exported C symbol is marked weak so sanitizer interceptors
// can override it without multiple-definition errors in static builds.
// Wiring works via LLVM-libc's per-function attribute hook:
//   LLVM_LIBC_FUNCTION_ATTR_<name> := LLVM_LIBC_EMPTY, <attribute>
// The leading LLVM_LIBC_EMPTY is the sentinel first argument consumed by the
// LLVM-libc macro pipeline; the second token after the comma is the attribute
// that ends up on the public C symbol.
//
// SIMD dispatch (__AVX512BW__ / __AVX2__ / __SSE4_1__ on x86_64, NEON on
// aarch64) is selected at compile time from the inherited -march= flag.
//
// The .cpp files included below come from contrib/llvm-project/libc/src/string/.
// clang-tidy flags the .cpp includes as suspicious; that's the intended
// pattern here — including the implementation files lets each function pick
// up the attribute override above before its LLVM_LIBC_FUNCTION expansion.

#define LLVM_LIBC_FUNCTION_ATTR_bcmp    LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcmp  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcpy  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memmove LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memset  LLVM_LIBC_EMPTY, __attribute__((weak))

// NOLINTBEGIN(bugprone-suspicious-include)
#include "src/strings/bcmp.cpp" // bcmp lives under <strings.h> (POSIX), not <string.h>
#include "src/string/memcmp.cpp"
#if defined(__x86_64__)
// On x86_64 we replace memcpy/memmove/memset together: the AVX-512 paths in
// `x86_64_mem_functions.cpp` are interlocked (memmove's disjoint fast path
// must reach our memcpy dispatcher, otherwise it would silently fall back to
// upstream's AVX-only one). For non-AVX-512 builds the dispatchers delegate
// to the same upstream helpers as the stock build, so v3 codegen is
// byte-equivalent to upstream.
#include "x86_64_mem_functions.cpp"
#else
#include "src/string/memcpy.cpp"
#include "src/string/memmove.cpp"
#include "src/string/memset.cpp"
#endif
// NOLINTEND(bugprone-suspicious-include)
