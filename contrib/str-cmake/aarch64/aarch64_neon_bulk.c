/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"

static inline void copy64(u8 *d, const u8 *s) {
    uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + 16), c = vld1q_u8(s + 32), e = vld1q_u8(s + 48);
    vst1q_u8(d, a); vst1q_u8(d + 16, b); vst1q_u8(d + 32, c); vst1q_u8(d + 48, e);
}

static inline __attribute__((always_inline)) void *copy_bulk(void *dst, const void *src, size_t n, size_t stride) {
    u8 *d = dst;
    const u8 *s = src;

    if (kl_copy_tier(d, s, n)) {
        return dst;
    }

    copy64(d, s);
    copy64(d + 64, s + 64);
    size_t i = 128 - ((uintptr_t)d & 15);

    while (n - i >= stride) {
        copy64(d + i, s + i);

        if (stride == 128) {
            copy64(d + i + 64, s + i + 64);
        }

        i += stride;
    }

    copy64(d + n - 128, s + n - 128);
    copy64(d + n - 64, s + n - 64);
    return dst;
}

void *kl_memcpy_neon_bulk128(void *d, const void *s, size_t n) {
    return copy_bulk(d, s, n, 128);
}

void *kl_memmove_neon_bulk64(void *dst, const void *src, size_t n) {
    u8 *d = dst;
    const u8 *s = src;

    if (kl_copy_tier(d, s, n)) {
        return dst;
    }

    if ((uintptr_t)d > (uintptr_t)s && (uintptr_t)d - (uintptr_t)s < n) {
        while (n >= 64) {
            n -= 64;
            copy64(d + n, s + n);
        }

        kl_copy_tier(d, s, n);
    } else {
        size_t i = 0;

        while (n - i >= 64) {
            copy64(d + i, s + i);
            i += 64;
        }

        kl_copy_tier(d + i, s + i, n - i);
    }

    return dst;
}

static inline void fill64(u8 *d, uint8x16_t v) {
    vst1q_u8(d, v); vst1q_u8(d + 16, v); vst1q_u8(d + 32, v); vst1q_u8(d + 48, v);
}

static inline __attribute__((always_inline)) void *fill_bulk(void *dst, int c, size_t n, size_t stride) {
    u8 *d = dst;
    uint8x16_t v = vdupq_n_u8((u8)c);

    if (n < 16) {
        uint64_t q = (u8)c * UINT64_C(0x0101010101010101);

        if (n >= 8) {
            kl_store64(d, q); kl_store64(d + n - 8, q);
        } else if (n >= 4) {
            kl_store32(d, (uint32_t)q); kl_store32(d + n - 4, (uint32_t)q);
        } else if (n) {
            d[0] = (u8)c; d[n / 2] = (u8)c; d[n - 1] = (u8)c;
        }

        return dst;
    }

    if (n <= 32) {
        vst1q_u8(d, v); vst1q_u8(d + n - 16, v);
    } else if (n <= 64) {
        vst1q_u8(d, v); vst1q_u8(d + 16, v); vst1q_u8(d + n - 32, v); vst1q_u8(d + n - 16, v);
    } else if (n <= 128) {
        fill64(d, v); fill64(d + n - 64, v);
    } else {
        fill64(d, v); fill64(d + 64, v);
        size_t i = 128 - ((uintptr_t)d & 15);

        while (n - i >= stride) {
            fill64(d + i, v);

            if (stride == 128) {
                fill64(d + i + 64, v);
            }

            i += stride;
        }

        fill64(d + n - 128, v); fill64(d + n - 64, v);
    }

    return dst;
}

void *kl_memset_neon_bulk128(void *d, int c, size_t n) {
    return fill_bulk(d, c, n, 128);
}
