/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"
extern size_t kl_strlen_neon_unaligned(const char *);
extern size_t kl_strspn_neon_table(const char *, const char *);
extern size_t kl_strcspn_neon_table(const char *, const char *);

static inline uint64_t difference_mask(uint8x16_t a, uint8x16_t b) {
    uint8x16_t stop = vmvnq_u8(vceqq_u8(a, b));
    return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(stop), 4)), 0);
}

static inline int compare_block(const u8 *a, const u8 *b, uint64_t mask) {
    size_t i = kl_first(mask);
    return (int)a[i] - b[i];
}

static inline int compare_word(uint64_t a, uint64_t b) {
    a = __builtin_bswap64(a);
    b = __builtin_bswap64(b);
    return (a > b) - (a < b);
}

int kl_memcmp_neon_words(const void *x, const void *y, size_t n) {
    const u8 *a = x, *b = y;

    if (n <= 16) {
        if (n >= 8) {
            uint64_t ah = kl_load64(a), bh = kl_load64(b);
            uint64_t at = kl_load64(a + n - 8), bt = kl_load64(b + n - 8);
            return compare_word(ah != bh ? ah : at, ah != bh ? bh : bt);
        }

        if (n >= 4) {
            uint32_t ah = __builtin_bswap32(kl_load32(a)), bh = __builtin_bswap32(kl_load32(b));
            uint32_t at = __builtin_bswap32(kl_load32(a + n - 4)), bt = __builtin_bswap32(kl_load32(b + n - 4));
            uint32_t av = ah != bh ? ah : at, bv = ah != bh ? bh : bt;
            return (av > bv) - (av < bv);
        }

        if (!n) {
            return 0;
        }

        int h = (int)a[0] - b[0], m = (int)a[n / 2] - b[n / 2], t = (int)a[n - 1] - b[n - 1];
        return h ? h : m ? m : t;
    }

    size_t i = 0;

    while (n - i > 16) {
        uint64_t mask = difference_mask(vld1q_u8(a + i), vld1q_u8(b + i));

        if (mask) {
            return compare_block(a + i, b + i, mask);
        }

        i += 16;
    }

    uint64_t mask = difference_mask(vld1q_u8(a + n - 16), vld1q_u8(b + n - 16));
    return mask ? compare_block(a + n - 16, b + n - 16, mask) : 0;
}

void *kl_memchr_neon_blocks(const void *src, int c, size_t n) {
    const u8 *s = src;
    uint8x16_t v = vdupq_n_u8((u8)c);
    size_t i = 0;

    while (i < n) {
        if (n - i >= 16 && (((uintptr_t)s + i) & 4095) <= 4080) {
            uint8x16_t eq = vceqq_u8(vld1q_u8(s + i), v);
            uint64_t mask = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4)), 0);

            if (mask) {
                return (void *)(s + i + kl_first(mask));
            }

            i += 16;
        } else {
            if (s[i] == (u8)c) {
                return (void *)(s + i);
            }

            ++i;
        }
    }

    return 0;
}

static inline char *copy_string(char *dst, const char *src) {
    u8 *d = (u8 *)dst;
    const u8 *s = (const u8 *)src;

    for (;;) {
        if (((uintptr_t)s & 4095) <= 4080) {
            uint8x16_t v = vld1q_u8(s);
            uint64_t mask = kl_zero_syndrome(v);

            if (mask) {
                kl_copy_tier(d, s, kl_first(mask) + 1);
                return dst;
            }

            vst1q_u8(d, v);
            s += 16;
            d += 16;
        } else {
            u8 c = *s++;
            *d++ = c;

            if (!c) {
                return dst;
            }
        }
    }
}

char *kl_strcpy_neon_first16(char *d, const char *s) {
    return copy_string(d, s);
}

static inline size_t span_bitmap(const char *text, const char *set, int accept) {
    uint64_t map[4] = {0};
    const u8 *p = (const u8 *)set;

    while (*p) {
        map[*p >> 6] |= UINT64_C(1) << (*p & 63);
        ++p;
    }

    const u8 *s = (const u8 *)text;
    size_t i = 0;

    while (s[i] && (int)((map[s[i] >> 6] >> (s[i] & 63)) & 1) == accept) {
        ++i;
    }

    return i;
}

static inline size_t span_small(const char *text, const char *set, int accept, int table) {
    const u8 *s = (const u8 *)text;
    const u8 *p = (const u8 *)set;
    u8 a = p[0], b = 0, c = 0;

    if (a) {
        b = p[1];

        if (b) {
            c = p[2];

            if (c && p[3]) {
                if (table) {
                    return accept ? kl_strspn_neon_table(text, set) : kl_strcspn_neon_table(text, set);
                }

                return span_bitmap(text, set, accept);
            }
        }
    }

    uint8x16_t va = vdupq_n_u8(a), vb = vdupq_n_u8(b), vc = vdupq_n_u8(c);
    size_t i = 0;

    for (;;) {
        if ((((uintptr_t)s + i) & 4095) <= 4080) {
            uint8x16_t v = vld1q_u8(s + i);
            uint8x16_t zero = vceqq_u8(v, vdupq_n_u8(0));
            uint8x16_t hit = vorrq_u8(vorrq_u8(vceqq_u8(v, va), vceqq_u8(v, vb)), vceqq_u8(v, vc));
            uint8x16_t stop = vorrq_u8(accept ? vmvnq_u8(hit) : hit, zero);
            uint64_t mask = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(stop), 4)), 0);

            if (mask) {
                return i + kl_first(mask);
            }

            i += 16;
        } else {
            u8 value = s[i];
            int hit = (value == a) | (value == b) | (value == c);

            if (!value || hit != accept) {
                return i;
            }

            ++i;
        }
    }
}

size_t kl_strspn_neon_adaptive(const char *s, const char *set) {
    return span_small(s, set, 1, 1);
}

size_t kl_strcspn_neon_adaptive(const char *s, const char *set) {
    return span_small(s, set, 0, 1);
}
