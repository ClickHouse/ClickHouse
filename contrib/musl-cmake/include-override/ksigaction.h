/* This is a wrapper that ensures the correct arch-specific ksigaction.h is used.
 * On x86_64, the arch-specific version defines __restore as an alias to __restore_rt.
 * Most other architectures (aarch64, powerpc64, s390x, riscv64, loongarch64) do not
 * have arch-specific ksigaction.h and use the generic one from src/internal/.
 */
#if defined(__x86_64__)
#include "../../../musl/arch/x86_64/ksigaction.h"
#else
/* Fallback to generic - used by aarch64, powerpc64, s390x, riscv64, loongarch64, etc. */
#include "../../../musl/src/internal/ksigaction.h"
#endif
