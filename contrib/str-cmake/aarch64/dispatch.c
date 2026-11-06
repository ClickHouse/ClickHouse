/* Runtime ISA selection for the experimental pg83/str ARM provider. */
#include "kernels.h"
#include "aarch64_helpers.h"
#include <sys/auxv.h>

#define STR_HWCAP_SVE (1UL << 22)

typedef void *(*copy_fn)(void *, const void *, size_t);
typedef void *(*fill_fn)(void *, int, size_t);
typedef void *(*find_fn)(const void *, int, size_t);
typedef char *(*string_fn)(char *, const char *);

static char *cat_neon(char *d, const char *s)
{
    kl_strcpy_neon_first16(d + kl_strlen_neon_prefix32_tail128(d), s);
    return d;
}

static copy_fn copy_impl = kl_memcpy_neon_bulk128;
static copy_fn move_impl = kl_memmove_neon_bulk64;
static fill_fn fill_impl = kl_memset_neon_bulk128;
static find_fn find_impl = kl_memchr_neon_blocks;
static string_fn strcpy_impl = kl_strcpy_neon_first16;
static string_fn strcat_impl = cat_neon;

void *ch_str_sve_memcpy(void *, const void *, size_t);
void *ch_str_sve_memmove(void *, const void *, size_t);
void *ch_str_sve_memset(void *, int, size_t);
void *ch_str_sve_memchr(const void *, int, size_t);
char *ch_str_sve_strcpy(char *, const char *);
char *ch_str_sve_strcat(char *, const char *);

/* Valid NEON entry points exist before any constructor or thread runs. */
__attribute__((constructor(101))) static void select_implementations(void)
{
    if (getauxval(AT_HWCAP) & STR_HWCAP_SVE)
    {
        __atomic_store_n(&copy_impl, ch_str_sve_memcpy, __ATOMIC_RELAXED);
        __atomic_store_n(&move_impl, ch_str_sve_memmove, __ATOMIC_RELAXED);
        __atomic_store_n(&fill_impl, ch_str_sve_memset, __ATOMIC_RELAXED);
        __atomic_store_n(&find_impl, ch_str_sve_memchr, __ATOMIC_RELAXED);
        __atomic_store_n(&strcpy_impl, ch_str_sve_strcpy, __ATOMIC_RELAXED);
        __atomic_store_n(&strcat_impl, ch_str_sve_strcat, __ATOMIC_RELAXED);
    }
}

__attribute__((flatten)) void *str_memcpy(void *d, const void *s, size_t n)
{
    if (n <= 16)
    {
        kl_copy_tier(d, s, n);
        return d;
    }
    return __atomic_load_n(&copy_impl, __ATOMIC_RELAXED)(d, s, n);
}
__attribute__((flatten)) void *str_memmove(void *d, const void *s, size_t n)
{
    if (n <= 16)
    {
        kl_copy_tier(d, s, n);
        return d;
    }
    return __atomic_load_n(&move_impl, __ATOMIC_RELAXED)(d, s, n);
}
void *str_memset(void *d, int c, size_t n)
{
    if (n <= 16)
    {
        u8 *bytes = d;
        uint64_t value = (u8)c * UINT64_C(0x0101010101010101);
        if (n >= 8)
        {
            kl_store64(bytes, value);
            kl_store64(bytes + n - 8, value);
        }
        else if (n >= 4)
        {
            kl_store32(bytes, (uint32_t)value);
            kl_store32(bytes + n - 4, (uint32_t)value);
        }
        else if (n)
        {
            bytes[0] = (u8)c;
            bytes[n / 2] = (u8)c;
            bytes[n - 1] = (u8)c;
        }
        return d;
    }
    return __atomic_load_n(&fill_impl, __ATOMIC_RELAXED)(d, c, n);
}
void *str_memchr(const void *s, int c, size_t n)
{
    return __atomic_load_n(&find_impl, __ATOMIC_RELAXED)(s, c, n);
}
char *str_strcpy(char *d, const char *s)
{
    return __atomic_load_n(&strcpy_impl, __ATOMIC_RELAXED)(d, s);
}
char *str_strcat(char *d, const char *s)
{
    return __atomic_load_n(&strcat_impl, __ATOMIC_RELAXED)(d, s);
}
int str_memcmp(const void *a, const void *b, size_t n)
{
    return kl_memcmp_neon_words(a, b, n);
}
size_t str_strlen(const char *s)
{
    return kl_strlen_neon_prefix32_tail128(s);
}
int str_strcmp(const char *a, const char *b)
{
    return kl_strcmp_neon_first16_tail64(a, b);
}
size_t str_strspn(const char *s, const char *set)
{
    return kl_strspn_neon_adaptive(s, set);
}
size_t str_strcspn(const char *s, const char *set)
{
    return kl_strcspn_neon_adaptive(s, set);
}
