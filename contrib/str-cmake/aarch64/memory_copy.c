/* Public memory-copy entries; compiler-generated unwind data is retained. */
#include "kernels.h"
#include "aarch64_helpers.h"
#include <sys/auxv.h>

void *ch_str_sve_copy_tiny16(void *, const void *, size_t);
static unsigned char ch_str_copy_sve_ready;

/* Keep the original scalar/NEON path available before constructor execution. */
__attribute__((constructor(110))) static void select_tiny_copy(void)
{
    __atomic_store_n(&ch_str_copy_sve_ready,
        (getauxval(AT_HWCAP) & (1UL << 22)) != 0, __ATOMIC_RELAXED);
}

__attribute__((flatten)) void *str_memcpy(void *d, const void *s, size_t n)
{
    if (n <= 16)
    {
        if (n == 16)
        {
            kl_copy_tier(d, s, 16);
            return d;
        }
        if (__atomic_load_n(&ch_str_copy_sve_ready, __ATOMIC_RELAXED))
            return ch_str_sve_copy_tiny16(d, s, n);
        kl_copy_tier(d, s, n);
        return d;
    }
    /* Keep the overlap-safe short tier in the public entry point. At VL16,
       dispatch and predicate setup cost more than these NEON loads/stores. */
    if (n <= 128)
    {
        kl_copy_tier(d, s, n);
        return d;
    }
    return kl_memcpy_neon_bulk128(d, s, n);
}
__attribute__((flatten)) void *str_memmove(void *d, const void *s, size_t n)
{
    if (n <= 16)
    {
        kl_copy_tier(d, s, n);
        return d;
    }
    if (n <= 128)
    {
        kl_copy_tier(d, s, n);
        return d;
    }
    return kl_memmove_neon_bulk64(d, s, n);
}
