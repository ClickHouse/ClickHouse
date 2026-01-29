/* This is a wrapper that ensures the correct arch-specific ksigaction.h is used.
 * On x86_64, the arch-specific version defines __restore as an alias to __restore_rt.
 * The generic src/internal/ksigaction.h declares them as separate symbols.
 */
#if defined(__x86_64__)
#include "../../../musl/arch/x86_64/ksigaction.h"
#elif defined(__aarch64__)
#include "../../../musl/arch/aarch64/ksigaction.h"
#elif defined(__powerpc64__)
#include "../../../musl/arch/powerpc64/ksigaction.h"
#elif defined(__s390x__)
#include "../../../musl/arch/s390x/ksigaction.h"
#elif defined(__riscv) && __riscv_xlen == 64
#include "../../../musl/arch/riscv64/ksigaction.h"
#elif defined(__loongarch64)
#include "../../../musl/arch/loongarch64/ksigaction.h"
#else
/* Fallback to generic */
#include "../../../musl/src/internal/ksigaction.h"
#endif
