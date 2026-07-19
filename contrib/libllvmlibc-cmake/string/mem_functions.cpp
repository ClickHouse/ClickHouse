// Weak `memcmp`, `memcpy`, `memmove`, `memset`, `bcmp` from LLVM-libc.
//
// Each exported C symbol is marked weak via LLVM-libc's per-function attribute
// hook (LLVM_LIBC_FUNCTION_ATTR_<name>), so sanitizer interceptors can override
// them (the interceptor's strong definition wins the link). As the only
// non-sanitizer definitions, the weak symbols resolve normally otherwise. The
// first token after the comma is the attribute placed on the public C symbol;
// the leading LLVM_LIBC_EMPTY is the sentinel the LLVM-libc macro pipeline
// consumes.

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
