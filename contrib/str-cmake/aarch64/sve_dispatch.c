/* SVE VL is per-thread and can change after startup. Check on every entry. */
#include "kernels.h"
#include <arm_sve.h>

void *ch_str_sve_memcpy(void *d, const void *s, size_t n)
{
    return svcntb() == 16 ? kl_memcpy_sve_bulk128(d, s, n) : kl_memcpy_neon_bulk128(d, s, n);
}

void *ch_str_sve_memmove(void *d, const void *s, size_t n)
{
    return svcntb() == 16 ? kl_memmove_sve_bulk64(d, s, n) : kl_memmove_neon_bulk64(d, s, n);
}

void *ch_str_sve_memset(void *d, int c, size_t n)
{
    return svcntb() == 16 ? kl_memset_sve_bulk128(d, c, n) : kl_memset_neon_bulk128(d, c, n);
}

void *ch_str_sve_memchr(const void *s, int c, size_t n)
{
    return svcntb() == 16 ? kl_memchr_sve_pred1(s, c, n) : kl_memchr_neon_blocks(s, c, n);
}

char *ch_str_sve_strcpy(char *d, const char *s)
{
    return svcntb() == 16 ? kl_strcpy_sve_split64(d, s) : kl_strcpy_neon_first16(d, s);
}

char *ch_str_sve_strcat(char *d, const char *s)
{
    if (svcntb() == 16)
        return kl_strcat_sve_split64(d, s);
    kl_strcpy_neon_first16(d + kl_strlen_neon_prefix32_tail128(d), s);
    return d;
}
