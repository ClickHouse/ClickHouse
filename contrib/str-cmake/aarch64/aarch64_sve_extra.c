/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"
#include <arm_sve.h>
extern int kl_memcmp_neon_words(const void *, const void *, size_t);
extern size_t kl_strlen_neon_unaligned(const char *);
extern size_t kl_strlen_neon_prefix32_tail128(const char *);

static inline size_t first_true(svbool_t p) {
    svbool_t all = svptrue_b8();
    return svcntp_b8(all, svbrkb_z(all, p));
}

void *kl_memchr_sve_pred1(const void *src, int c, size_t n) {
    const u8 *s = src;
    size_t i = 0;

    for (;;) {
        size_t page = 4096 - (((uintptr_t)s + i) & 4095);
        size_t left = n - i;
        svbool_t p = svwhilelt_b8_u64(0, left < page ? left : page);
        svuint8_t v = svld1_u8(p, (const u8 *)((uintptr_t)s + i));
        svbool_t hit = svcmpeq_n_u8(p, v, (u8)c);

        if (svptest_any(svptrue_b8(), hit)) {
            return (void *)(s + i + first_true(hit));
        }

        size_t count = svcntp_b8(svptrue_b8(), p);

        if (left <= count) {
            return 0;
        }

        i += count;
    }
}

static inline char *copy_predicated(char *dst, const char *src) {
    u8 *d = (u8 *)dst;
    const u8 *s = (const u8 *)src;
    svbool_t all = svptrue_b8();

    for (;;) {
        size_t left = 4096 - ((uintptr_t)s & 4095);
        svbool_t active = svwhilelt_b8_u64(0, left);
        svuint8_t v = svld1_u8(active, s);
        svbool_t zero = svcmpeq_n_u8(active, v, 0);
        svst1_u8(svbrka_z(active, zero), d, v);

        if (svptest_any(all, zero)) {
            return dst;
        }

        size_t count = svcntp_b8(all, active);
        s += count;
        d += count;
    }
}

static inline void finish_vector(u8 *d, const u8 *s, uint64_t bits) {
    svbool_t active = svwhilelt_b8_u64(0, kl_first(bits) + 1);
    svst1_u8(active, d, svld1_u8(active, s));
}

static __attribute__((noinline)) void copy_tail64(u8 *d, const u8 *s) {
    while ((uintptr_t)s & 63) {
        uint8x16_t v = vld1q_u8(s);
        uint64_t bits = kl_zero_syndrome(v);

        if (bits) {
            finish_vector(d, s, bits);
            return;
        }

        vst1q_u8(d, v);
        s += 16;
        d += 16;
    }

    for (;;) {
        uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + 16), c = vld1q_u8(s + 32), e = vld1q_u8(s + 48);
        uint8x16_t low = vminq_u8(vminq_u8(a, b), vminq_u8(c, e));

        if (!vminvq_u8(low)) {
            uint64_t mask = kl_zero_syndrome(a);

            if (mask) {
                finish_vector(d, s, mask);
                return;
            }

            vst1q_u8(d, a);
            mask = kl_zero_syndrome(b);

            if (mask) {
                finish_vector(d + 16, s + 16, mask);
                return;
            }

            vst1q_u8(d + 16, b);
            mask = kl_zero_syndrome(c);

            if (mask) {
                finish_vector(d + 32, s + 32, mask);
                return;
            }

            vst1q_u8(d + 32, c);
            finish_vector(d + 48, s + 48, kl_zero_syndrome(e));
            return;
        }

        vst1q_u8(d, a); vst1q_u8(d + 16, b); vst1q_u8(d + 32, c); vst1q_u8(d + 48, e);
        s += 64;
        d += 64;
    }
}

static inline char *copy_split(char *dst, const char *src) {
    const u8 *s = (const u8 *)src;
    u8 *d = (u8 *)dst;
    #pragma unroll
    for (int i = 0; i < 2; ++i) {
        svbool_t active = svwhilelt_b8_u64(0, 4096 - ((uintptr_t)s & 4095));
        svuint8_t v = svld1_u8(active, s);
        svbool_t zero = svcmpeq_n_u8(active, v, 0);
        svst1_u8(svbrka_z(active, zero), d, v);

        if (svptest_any(svptrue_b8(), zero)) {
            return dst;
        }

        size_t count = svcntp_b8(svptrue_b8(), active);
        s += count;
        d += count;
    }

    size_t skew = (uintptr_t)s & 15;
    copy_tail64(d - skew, s - skew);
    return dst;
}

char *kl_strcpy_sve_split64(char *d, const char *s) {
    return copy_split(d, s);
}

char *kl_strcat_sve_split64(char *d, const char *s) {
    copy_split(d + kl_strlen_neon_prefix32_tail128(d), s);
    return d;
}
