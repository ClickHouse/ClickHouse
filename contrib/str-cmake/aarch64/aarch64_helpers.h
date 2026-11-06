#ifndef STR_ARM_LAB_HELPERS_H
#define STR_ARM_LAB_HELPERS_H
#include <arm_neon.h>
#include <stddef.h>
#include <stdint.h>
typedef unsigned char u8;

static inline uint64_t kl_zero_syndrome(uint8x16_t value) {
    uint8x16_t bits = vceqq_u8(value, vdupq_n_u8(0));
    uint8x8_t packed = vshrn_n_u16(vreinterpretq_u16_u8(bits), 4);
    return vget_lane_u64(vreinterpret_u64_u8(packed), 0);
}

static inline uint64_t kl_stop_syndrome(uint8x16_t a, uint8x16_t b) {
    uint8x16_t stop = vorrq_u8(vmvnq_u8(vceqq_u8(a, b)), vceqq_u8(a, vdupq_n_u8(0)));
    uint8x8_t packed = vshrn_n_u16(vreinterpretq_u16_u8(stop), 4);
    return vget_lane_u64(vreinterpret_u64_u8(packed), 0);
}

static inline size_t kl_first(uint64_t syndrome) {
    return (size_t)__builtin_ctzll(syndrome) / 4;
}

static inline uint64_t kl_load64(const void *p) {
    uint64_t value;
    __builtin_memcpy_inline(&value, p, 8);
    return value;
}

static inline uint32_t kl_load32(const void *p) {
    uint32_t value;
    __builtin_memcpy_inline(&value, p, 4);
    return value;
}

static inline void kl_store64(void *p, uint64_t value) {
    __builtin_memcpy_inline(p, &value, 8);
}

static inline void kl_store32(void *p, uint32_t value) {
    __builtin_memcpy_inline(p, &value, 4);
}

/* All loads precede every store so the short path also accepts overlap. */
static inline int kl_copy_tier(u8 *d, const u8 *s, size_t n) {
    if (n > 128) {
        return 0;
    }

    if (n > 64) {
        uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + 16), c = vld1q_u8(s + 32), e = vld1q_u8(s + 48);
        uint8x16_t f = vld1q_u8(s + n - 64), g = vld1q_u8(s + n - 48), h = vld1q_u8(s + n - 32), i = vld1q_u8(s + n - 16);
        vst1q_u8(d, a); vst1q_u8(d + 16, b); vst1q_u8(d + 32, c); vst1q_u8(d + 48, e);
        vst1q_u8(d + n - 64, f); vst1q_u8(d + n - 48, g); vst1q_u8(d + n - 32, h); vst1q_u8(d + n - 16, i);
    } else if (n > 32) {
        uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + 16), c = vld1q_u8(s + n - 32), e = vld1q_u8(s + n - 16);
        vst1q_u8(d, a); vst1q_u8(d + 16, b); vst1q_u8(d + n - 32, c); vst1q_u8(d + n - 16, e);
    } else if (n >= 16) {
        uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + n - 16);
        vst1q_u8(d, a); vst1q_u8(d + n - 16, b);
    } else if (n >= 8) {
        uint64_t a = kl_load64(s), b = kl_load64(s + n - 8);
        kl_store64(d, a); kl_store64(d + n - 8, b);
    } else if (n >= 4) {
        uint32_t a = kl_load32(s), b = kl_load32(s + n - 4);
        kl_store32(d, a); kl_store32(d + n - 4, b);
    } else if (n) {
        u8 a = s[0], b = s[n / 2], c = s[n - 1];
        d[0] = a; d[n / 2] = b; d[n - 1] = c;
    }

    return 1;
}
#endif
