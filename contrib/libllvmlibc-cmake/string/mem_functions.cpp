// Weak `memcmp`, `memcpy`, `memmove`, `memset`, `bcmp` from LLVM-libc.
//
// Each exported C symbol is marked weak via LLVM-libc's per-function attribute
// hook (LLVM_LIBC_FUNCTION_ATTR_<name>) so that sanitizer interceptors can
// override them and so the strong glibc-compatibility `memcpy` keeps precedence
// (libllvmlibc is linked last, so when the linker reaches it `memcpy` is
// already resolved to the strong definition and the weak one is a harmless
// duplicate). `memmem` is strong and lives in its own TU (memmem.cpp) so that a
// reference to it does not drag this object — and its weak `memcpy` — in.
//
// The first token after the comma in each macro is the attribute that lands on
// the public C symbol; the leading LLVM_LIBC_EMPTY is the sentinel argument the
// LLVM-libc macro pipeline consumes.

#define LLVM_LIBC_FUNCTION_ATTR_bcmp    LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcmp  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memcpy  LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memmove LLVM_LIBC_EMPTY, __attribute__((weak))
#define LLVM_LIBC_FUNCTION_ATTR_memset  LLVM_LIBC_EMPTY, __attribute__((weak))

// Including the implementation .cpp files lets each function pick up the
// attribute override above before its LLVM_LIBC_FUNCTION expansion.
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
