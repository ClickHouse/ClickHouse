#pragma once
#include <stddef.h>

int kl_memcmp_neon_words(const void *x, const void *y, size_t n);
void *kl_memchr_neon_blocks(const void *src, int c, size_t n);
char *kl_strcpy_neon_first16(char *d, const char *s);
size_t kl_strspn_neon_adaptive(const char *s, const char *set);
size_t kl_strcspn_neon_adaptive(const char *s, const char *set);
void *kl_memcpy_neon_bulk128(void *d, const void *s, size_t n);
void *kl_memmove_neon_bulk64(void *dst, const void *src, size_t n);
void *kl_memset_neon_bulk128(void *d, int c, size_t n);
void *kl_memchr_sve_pred1(const void *src, int c, size_t n);
char *kl_strcpy_sve_split64(char *d, const char *s);
char *kl_strcat_sve_split64(char *d, const char *s);
int kl_strcmp_neon_block16(const char *x, const char *y);
int kl_strcmp_neon_first16_tail64(const char *x, const char *y);
size_t kl_strlen_neon_prefix32_tail128(const char *text);
size_t kl_strspn_neon_table(const char *s, const char *set);
size_t kl_strcspn_neon_table(const char *s, const char *set);
void *kl_memcpy_sve_bulk128(void *d, const void *s, size_t n);
void *kl_memmove_sve_bulk64(void *d, const void *s, size_t n);
void *kl_memset_sve_bulk128(void *d, int c, size_t n);
