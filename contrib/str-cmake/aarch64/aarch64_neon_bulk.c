/* Selected kernels from the pg83/str AArch64 workbench. See provenance.json. */
#include "aarch64_helpers.h"

static inline void copy64(u8 *d, const u8 *s) {
    uint8x16_t a = vld1q_u8(s), b = vld1q_u8(s + 16), c = vld1q_u8(s + 32), e = vld1q_u8(s + 48);
    vst1q_u8(d, a); vst1q_u8(d + 16, b); vst1q_u8(d + 32, c); vst1q_u8(d + 48, e);
}

/* The medium `memcpy` tier uses fewer repeated stores than the streaming loop.
   `memmove` retains its directional loop. */
static inline void copy_medium(u8 *d, const u8 *s, size_t n)
{
    uint8x16_t a0 = vld1q_u8(s);
    uint8x16_t a1 = vld1q_u8(s + 16);
    uint8x16_t a2 = vld1q_u8(s + 32);
    uint8x16_t a3 = vld1q_u8(s + 48);
    uint8x16_t a4 = vld1q_u8(s + 64);
    uint8x16_t a5 = vld1q_u8(s + 80);
    uint8x16_t a6 = vld1q_u8(s + 96);
    uint8x16_t a7 = vld1q_u8(s + 112);
    if (n <= 192)
    {
        uint8x16_t b0 = vld1q_u8(s + n - 64);
        uint8x16_t b1 = vld1q_u8(s + n - 48);
        uint8x16_t b2 = vld1q_u8(s + n - 32);
        uint8x16_t b3 = vld1q_u8(s + n - 16);
        vst1q_u8(d, a0);
        vst1q_u8(d + 16, a1);
        vst1q_u8(d + 32, a2);
        vst1q_u8(d + 48, a3);
        vst1q_u8(d + 64, a4);
        vst1q_u8(d + 80, a5);
        vst1q_u8(d + 96, a6);
        vst1q_u8(d + 112, a7);
        vst1q_u8(d + n - 64, b0);
        vst1q_u8(d + n - 48, b1);
        vst1q_u8(d + n - 32, b2);
        vst1q_u8(d + n - 16, b3);
    }
    else
    {
        uint8x16_t b0 = vld1q_u8(s + n - 128);
        uint8x16_t b1 = vld1q_u8(s + n - 112);
        uint8x16_t b2 = vld1q_u8(s + n - 96);
        uint8x16_t b3 = vld1q_u8(s + n - 80);
        uint8x16_t b4 = vld1q_u8(s + n - 64);
        uint8x16_t b5 = vld1q_u8(s + n - 48);
        uint8x16_t b6 = vld1q_u8(s + n - 32);
        uint8x16_t b7 = vld1q_u8(s + n - 16);
        vst1q_u8(d, a0);
        vst1q_u8(d + 16, a1);
        vst1q_u8(d + 32, a2);
        vst1q_u8(d + 48, a3);
        vst1q_u8(d + 64, a4);
        vst1q_u8(d + 80, a5);
        vst1q_u8(d + 96, a6);
        vst1q_u8(d + 112, a7);
        vst1q_u8(d + n - 128, b0);
        vst1q_u8(d + n - 112, b1);
        vst1q_u8(d + n - 96, b2);
        vst1q_u8(d + n - 80, b3);
        vst1q_u8(d + n - 64, b4);
        vst1q_u8(d + n - 48, b5);
        vst1q_u8(d + n - 32, b6);
        vst1q_u8(d + n - 16, b7);
    }
}

static inline __attribute__((always_inline)) void *copy_bulk(void *dst, const void *src, size_t n, size_t stride) {
    u8 *d = dst;
    const u8 *s = src;

    if (kl_copy_tier(d, s, n)) {
        return dst;
    }

    if (n <= 256)
    {
        copy_medium(d, s, n);
        return dst;
    }

    copy64(d, s);
    copy64(d + 64, s + 64);
    /* Choose source alignment only when it removes one body iteration. */
    size_t destination_phase = (uintptr_t)d & 15;
    size_t gap = (-n) & 127;
    size_t phase = n <= 1024 && ((uintptr_t)s & 15) == 0 && destination_phase > gap ? 0 : destination_phase;
    size_t i = 128 - phase;

    while (n - i > stride) {
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

static inline void zero_edge(u8 *d, size_t n)
{
    uint8x16_t zero = vdupq_n_u8(0);
    while (n >= 16)
    {
        vst1q_u8(d, zero);
        d += 16;
        n -= 16;
    }
    while (n)
    {
        *d++ = 0;
        --n;
    }
}

static inline void zero_blocks(u8 *d, size_t n, size_t block)
{
    size_t prefix = (-(uintptr_t)d) & (block - 1);
    zero_edge(d, prefix);
    d += prefix;
    n -= prefix;

    while (n >= 4 * block)
    {
        __asm__ volatile("dc zva, %0" :: "r"(d) : "memory");
        __asm__ volatile("dc zva, %0" :: "r"(d + block) : "memory");
        __asm__ volatile("dc zva, %0" :: "r"(d + 2 * block) : "memory");
        __asm__ volatile("dc zva, %0" :: "r"(d + 3 * block) : "memory");
        d += 4 * block;
        n -= 4 * block;
    }
    while (n >= block)
    {
        __asm__ volatile("dc zva, %0" :: "r"(d) : "memory");
        d += block;
        n -= block;
    }
    zero_edge(d, n);
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
        /* Repeated cache-resident clears benefit from zero-by-address. Keep
           streaming fills on NEON: the measured crossover is above 2 MiB. */
        if ((u8)c == 0 && n >= 512 && n <= 2 * 1024 * 1024)
        {
            uint64_t dczid;
            __asm__ volatile("mrs %0, dczid_el0" : "=r"(dczid));
            if (!(dczid & 16))
            {
                size_t block = (size_t)4 << (dczid & 15);
                if (n >= 2 * block)
                {
                    zero_blocks(d, n, block);
                    return dst;
                }
            }
        }
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
