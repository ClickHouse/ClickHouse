/* Public search entry; compiler-generated unwind data is retained. */
#include "kernels.h"
#include <sys/auxv.h>

typedef void *(*find_fn)(const void *, int, size_t);
void *ch_str_sve_memchr_aligned16(const void *, int, size_t);

/* A valid NEON implementation is available before constructors run. */
static find_fn find_impl = kl_memchr_neon_blocks;

__attribute__((constructor(101))) static void select_search(void)
{
    if (getauxval(AT_HWCAP) & (1UL << 22))
        __atomic_store_n(&find_impl, ch_str_sve_memchr_aligned16, __ATOMIC_RELAXED);
}

void *str_memchr(const void *s, int c, size_t n)
{
    return __atomic_load_n(&find_impl, __ATOMIC_RELAXED)(s, c, n);
}
