/* Runtime ISA selection for the experimental pg83/str ARM provider. */
#include "kernels.h"
#include "aarch64_helpers.h"
#include <sys/auxv.h>

#define STR_HWCAP_SVE (1UL << 22)

typedef char *(*string_fn)(char *, const char *);

static char *cat_neon(char *d, const char *s)
{
    kl_strcpy_neon_first16(d + kl_strlen_neon_prefix32_tail128(d), s);
    return d;
}

static string_fn strcpy_impl = kl_strcpy_neon_first16;
static string_fn strcat_impl = cat_neon;

char *ch_str_sve_strcpy(char *, const char *);
char *ch_str_sve_strcat(char *, const char *);

/* Valid NEON entry points exist before any constructor or thread runs. */
__attribute__((constructor(101))) static void select_implementations(void)
{
    if (getauxval(AT_HWCAP) & STR_HWCAP_SVE)
    {
        __atomic_store_n(&strcpy_impl, ch_str_sve_strcpy, __ATOMIC_RELAXED);
        __atomic_store_n(&strcat_impl, ch_str_sve_strcat, __ATOMIC_RELAXED);
    }
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
