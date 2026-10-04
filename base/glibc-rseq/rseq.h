#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Return cpuid from registered rseq
///
/// Return:
/// - On success >= 0
/// - -1 if uninitialized (or unavailable, i.e. old glibc)
/// - -2 registration failed
int32_t rseq_cpu_id(void);

#ifdef __cplusplus
}
#endif
