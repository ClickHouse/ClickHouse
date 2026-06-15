#include <stddef.h>
#include <stdint.h>

#if defined(__x86_64__) || defined(__i386__)
#  include <emmintrin.h> /* SSE2 */
#endif

#if defined(__aarch64__)
#  include <arm_neon.h>
#endif


/** Custom memcmp implementation for ClickHouse.
  *
  * The motivation is the same as for the custom memcpy (see ../memcpy/memcpy.h):
  * to avoid a dependency on a specific libc symbol version and to allow inlining
  * inline_memcmp directly at call sites.
  *
  * Some libc implementations (notably musl) ship a naive byte-by-byte memcmp that
  * the compiler cannot auto-vectorize because of the early-exit on mismatch. For
  * typical ClickHouse workloads (hash aggregation / join on string keys) that loop
  * dominates the profile. This implementation compares 16 bytes at a time using
  * SSE2 on x86_64 and NEON on aarch64, with word-at-a-time and byte-at-a-time tails.
  *
  * It matches the C memcmp contract: returns the signed difference of the first
  * pair of differing bytes (as unsigned char), or 0 if the ranges are equal.
  */
inline int inline_memcmp(const void * vl, const void * vr, size_t n)
{
    const unsigned char * l = reinterpret_cast<const unsigned char *>(vl);
    const unsigned char * r = reinterpret_cast<const unsigned char *>(vr);

#if defined(__x86_64__) || defined(__i386__)
    while (n >= 16)
    {
        __m128i a = _mm_loadu_si128(reinterpret_cast<const __m128i *>(l));
        __m128i b = _mm_loadu_si128(reinterpret_cast<const __m128i *>(r));
        __m128i eq = _mm_cmpeq_epi8(a, b);
        unsigned mask = static_cast<unsigned>(_mm_movemask_epi8(eq)) ^ 0xFFFFu;
        if (mask)
        {
            unsigned idx = static_cast<unsigned>(__builtin_ctz(mask));
            return static_cast<int>(l[idx]) - static_cast<int>(r[idx]);
        }
        l += 16; r += 16; n -= 16;
    }
#elif defined(__aarch64__)
    while (n >= 16)
    {
        uint8x16_t a = vld1q_u8(l);
        uint8x16_t b = vld1q_u8(r);
        uint8x16_t eq = vceqq_u8(a, b);
        /* Compress each byte's top bit to a 64-bit mask (4 bits per lane). */
        uint64_t mask = vgetq_lane_u64(vreinterpretq_u64_u8(
            vcombine_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4), vdup_n_u8(0))), 0);
        if (mask != UINT64_C(0xFFFFFFFFFFFFFFFF))
        {
            unsigned idx = static_cast<unsigned>(__builtin_ctzll(~mask) >> 2);
            return static_cast<int>(l[idx]) - static_cast<int>(r[idx]);
        }
        l += 16; r += 16; n -= 16;
    }
#endif

    /* Word-at-a-time for the middle (useful on platforms without SIMD, and
       for the 8..15 byte tail on SIMD platforms). */
    while (n >= sizeof(size_t))
    {
        size_t wl = 0;
        size_t wr = 0;
        __builtin_memcpy(&wl, l, sizeof(size_t));
        __builtin_memcpy(&wr, r, sizeof(size_t));
        if (wl != wr)
            break;
        l += sizeof(size_t); r += sizeof(size_t); n -= sizeof(size_t);
    }

    for (; n && *l == *r; n--, l++, r++)
        ;
    return n ? static_cast<int>(*l) - static_cast<int>(*r) : 0;
}
