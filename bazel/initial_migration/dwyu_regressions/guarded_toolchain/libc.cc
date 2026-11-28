#include <cstdint>
#include <cstdint>
#include <cstdlib>
#include <stdio.h>
#include <features.h>
#include <features.h>
#include <stdint.h>
#include <stdint.h>

#if !defined(__GLIBC__) || __GLIBC__ != 2
#error Expected the configured glibc header
#endif
static_assert(sizeof(std::uint64_t) == 8);
int dwyu_glibc_cxx() { return __GLIBC__; }
