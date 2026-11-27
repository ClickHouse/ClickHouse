// Every tier of the x86_64 `memchr` and `strlen` from str_functions.cpp under
// its own name, for `gtest_memchr_strlen_page_boundary`. The exported `memchr`
// and `strlen` run only one tier on a given CPU and are not built at all under
// sanitizers, which are the only builds that run the unit tests in CI; these
// copies let the test reach the SSE2, AVX2 and AVX-512 bodies in every build.
//
// Built with -fno-sanitize=all: the bodies read whole aligned vectors past the
// end of the object (never past its page), which is what an instrumented build
// would report. Linked only into the unit tests.

#if defined(__x86_64__)

#define CHSTR_AVX2_ATTR                                                        \
  __attribute__((target("avx2,bmi,bmi2"), no_stack_protector))
#define CHSTR_SSE2_ATTR CH_NO_SSP
#include "str_functions_tiers.h"

extern "C" {

void *ch_test_memchr_sse2(const void *s, int c, size_t n) noexcept {
  return memchr_sse2(s, c, n);
}
size_t ch_test_strlen_sse2(const char *s) noexcept { return strlen_sse2(s); }

void *ch_test_memchr_avx2(const void *s, int c, size_t n) noexcept {
  return memchr_avx2(s, c, n);
}
size_t ch_test_strlen_avx2(const char *s) noexcept { return strlen_avx2(s); }

void *ch_test_memchr_avx512(const void *s, int c, size_t n) noexcept {
  return memchr_avx512(s, c, n);
}
size_t ch_test_strlen_avx512(const char *s) noexcept {
  return strlen_avx512(s);
}

} // extern "C"

#endif // __x86_64__
