// Weak `memcmp`, `memcpy`, `memmove`, `memset`, `bcmp` from LLVM-libc.
//
// Each exported C symbol is marked weak via LLVM-libc's per-function attribute
// hook (LLVM_LIBC_FUNCTION_ATTR_<name>), so sanitizer interceptors can override
// them (the interceptor's strong definition wins the link). As the only
// non-sanitizer definitions, the weak symbols resolve normally otherwise. The
// first token after the comma is the attribute placed on the public C symbol;
// the leading LLVM_LIBC_EMPTY is the sentinel the LLVM-libc macro pipeline
// consumes.

// Upgrade LIBC_INLINE from plain `inline` to `always_inline` for this TU, so
// the whole LLVM-libc helper tree collapses into the exported functions even
// at -O0, where the `flatten` attribute on the AVX-512 clones is lowered as a
// single level of inlining. Without this, the v4-attributed clones call
// out-of-line baseline-target helpers that pass 64-byte vectors by value - an
// ABI mismatch (zmm0 vs ymm0:ymm1) that corrupts overlapping `memmove` of
// more than 128 bytes on AVX-512 machines in -O0 builds.
#include "src/__support/macros/attributes.h"
#undef LIBC_INLINE
#define LIBC_INLINE inline __attribute__((always_inline))

// The generic `load`/`store`/`splat` in op_generic.h lack LIBC_INLINE
// upstream, yet they are exactly the functions that pass vectors by value.
// Redeclare them with `always_inline` before the definitions are parsed;
// attributes merge across redeclarations.
#include "src/string/memory_utils/utils.h"

#include <stdint.h>

namespace LIBC_NAMESPACE_DECL {
namespace generic {
template <typename T> __attribute__((always_inline)) T load(CPtr src);
template <typename T> __attribute__((always_inline)) void store(Ptr dst, T value);
template <typename T> __attribute__((always_inline)) T splat(uint8_t value);
} // namespace generic
} // namespace LIBC_NAMESPACE_DECL

#define LLVM_LIBC_FUNCTION_ATTR_bcmp    LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcmp  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcpy  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memmove LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memset  LLVM_LIBC_EMPTY, __attribute__((weak))

// Include the implementation .cpp files so each picks up the attribute override
// above before its LLVM_LIBC_FUNCTION expansion.
// NOLINTBEGIN(bugprone-suspicious-include)
#include "src/strings/bcmp.cpp" // bcmp lives under <strings.h>, not <string.h>
#include "src/string/memcmp.cpp"
#if defined(__x86_64__)
#include "x86_64_mem_functions.cpp"
#elif defined(__aarch64__)
// aarch64 `memcpy`/`memset` come from musl's Arm Optimized Routines assembly;
// this only adds `memmove`.
#include "aarch64_mem_functions.cpp"
#else
#include "src/string/memcpy.cpp"
#include "src/string/memmove.cpp"
#include "src/string/memset.cpp"
#endif
// NOLINTEND(bugprone-suspicious-include)
