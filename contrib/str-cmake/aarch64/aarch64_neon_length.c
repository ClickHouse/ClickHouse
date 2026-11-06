/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"

static inline uint64_t zero32(const u8 *s) {
    uint8x16_t pattern = vreinterpretq_u8_u32(vdupq_n_u32(UINT32_C(0xc0300c03)));
    uint8x16_t a = vandq_u8(vceqq_u8(vld1q_u8(s), vdupq_n_u8(0)), pattern);
    uint8x16_t b = vandq_u8(vceqq_u8(vld1q_u8(s + 16), vdupq_n_u8(0)), pattern);
    uint8x16_t pair = vpaddq_u8(a, b);
    return vgetq_lane_u64(vreinterpretq_u64_u8(vpaddq_u8(pair, pair)), 0);
}

static inline size_t first32(uint64_t mask) {
    return (size_t)__builtin_ctzll(mask) / 2;
}

static inline __attribute__((always_inline)) size_t length32(const char *text, int grouped) {
    const u8 *s = (const u8 *)text;
    const u8 *p = s;
    uint64_t bits;

    if (((uintptr_t)s & 4095) <= 4064) {
        bits = zero32(s);
    } else {
        p = (const u8 *)((uintptr_t)s & ~(uintptr_t)31);
        bits = zero32(p) >> (2 * ((uintptr_t)s & 31));
    }

    if (bits) {
        return first32(bits);
    }

    p = (const u8 *)(((uintptr_t)s & ~(uintptr_t)31) + 32);

    for (;;) {
        if (grouped && !((uintptr_t)p & 127)) {
            uint8x16_t a0 = vld1q_u8(p), a1 = vld1q_u8(p + 16), a2 = vld1q_u8(p + 32), a3 = vld1q_u8(p + 48);
            uint8x16_t a4 = vld1q_u8(p + 64), a5 = vld1q_u8(p + 80), a6 = vld1q_u8(p + 96), a7 = vld1q_u8(p + 112);
            uint8x16_t lo = vminq_u8(vminq_u8(vminq_u8(a0, a1), vminq_u8(a2, a3)), vminq_u8(vminq_u8(a4, a5), vminq_u8(a6, a7)));

            if (vminvq_u8(lo)) {
                p += 128;
                continue;
            }
        }

        bits = zero32(p);

        if (bits) {
            return (size_t)(p - s) + first32(bits);
        }

        p += 32;
    }
}

static __attribute__((noinline)) size_t tail128(const u8 *p, const u8 *s) {
    for (;;) {
        if (!((uintptr_t)p & 127)) {
            uint8x16_t a0 = vld1q_u8(p), a1 = vld1q_u8(p + 16), a2 = vld1q_u8(p + 32), a3 = vld1q_u8(p + 48);
            uint8x16_t a4 = vld1q_u8(p + 64), a5 = vld1q_u8(p + 80), a6 = vld1q_u8(p + 96), a7 = vld1q_u8(p + 112);
            uint8x16_t lo = vminq_u8(vminq_u8(vminq_u8(a0, a1), vminq_u8(a2, a3)), vminq_u8(vminq_u8(a4, a5), vminq_u8(a6, a7)));

            if (vminvq_u8(lo)) {
                p += 128;
                continue;
            }
        }

        uint64_t bits = zero32(p);

        if (bits) {
            return (size_t)(p - s) + first32(bits);
        }

        p += 32;
    }
}

size_t kl_strlen_neon_prefix32_tail128(const char *text) {
    const u8 *s = (const u8 *)text;
    uint64_t bits;

    if (((uintptr_t)s & 4095) <= 4064) {
        bits = zero32(s);
    } else {
        const u8 *p = (const u8 *)((uintptr_t)s & ~(uintptr_t)31);
        bits = zero32(p) >> (2 * ((uintptr_t)s & 31));
    }

    if (bits) {
        return first32(bits);
    }

    return tail128((const u8 *)(((uintptr_t)s & ~(uintptr_t)31) + 32), s);
}
