#include <cstddef>

#include <emmintrin.h>

#if defined(__clang__) && defined(__has_feature)
#    define ch_has_feature __has_feature
#endif
#if !defined(MEMORY_SANITIZER)
#    if defined(ch_has_feature)
#        if ch_has_feature(memory_sanitizer)
#            define MEMORY_SANITIZER 1
#        endif
#    elif defined(__MEMORY_SANITIZER__)
#        define MEMORY_SANITIZER 1
#    endif
#endif


extern bool have_avx;
void init_memcpy();


static inline char * inline_memcpy(char * __restrict dst, const char * __restrict src, size_t size)
{
    char * ret = dst;

tail:
    if (size <= 16)
    {
        if (size >= 8)
        {
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        }
        else if (size >= 1)
        {
            *dst = *src;
        }
    }
#if !defined(MEMORY_SANITIZER)  /// Asm code is not instrumented by MSan, skip this branch
    else if (have_avx)
    {
        if (size <= 32)
        {
            __builtin_memcpy(dst, src, 8);
            __builtin_memcpy(dst + 8, src + 8, 8);

            dst += 16;
            src += 16;
            size -= 16;

            goto tail;
        }

        if (size <= 256)
        {
            __asm__(
                "vmovups    -0x20(%[s],%[size],1), %%ymm0\n"
                "vmovups    %%ymm0, -0x20(%[d],%[size],1)\n"
                : [d]"+r"(dst), [s]"+r"(src)
                : [size]"r"(size)
                : "ymm0", "memory");

            while (size > 32)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    %%ymm0, (%[d])\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "memory");

                dst += 32;
                src += 32;
                size -= 32;
            }
        }
        else
        {
            size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

            if (padding > 0)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    %%ymm0, (%[d])\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "memory");

                dst += padding;
                src += padding;
                size -= padding;
            }

            while (size >= 256)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    0x20(%[s]), %%ymm1\n"
                    "vmovups    0x40(%[s]), %%ymm2\n"
                    "vmovups    0x60(%[s]), %%ymm3\n"
                    "vmovups    0x80(%[s]), %%ymm4\n"
                    "vmovups    0xa0(%[s]), %%ymm5\n"
                    "vmovups    0xc0(%[s]), %%ymm6\n"
                    "vmovups    0xe0(%[s]), %%ymm7\n"
                    "add        $0x100,%[s]\n"
                    "vmovaps    %%ymm0, (%[d])\n"
                    "vmovaps    %%ymm1, 0x20(%[d])\n"
                    "vmovaps    %%ymm2, 0x40(%[d])\n"
                    "vmovaps    %%ymm3, 0x60(%[d])\n"
                    "vmovaps    %%ymm4, 0x80(%[d])\n"
                    "vmovaps    %%ymm5, 0xa0(%[d])\n"
                    "vmovaps    %%ymm6, 0xc0(%[d])\n"
                    "vmovaps    %%ymm7, 0xe0(%[d])\n"
                    "add        $0x100, %[d]\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "ymm1", "ymm2", "ymm3", "ymm4", "ymm5", "ymm6", "ymm7", "memory");

                size -= 256;
            }

            goto tail;
        }
    }
#endif
    else
    {
        if (size <= 128)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

            while (size > 16)
            {
                _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
                dst += 16;
                src += 16;
                size -= 16;
            }
        }
        else
        {
            /// Align destination to 16 bytes boundary.
            size_t padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

            if (padding > 0)
            {
                __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
                _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
                dst += padding;
                src += padding;
                size -= padding;
            }

            /// Aligned unrolled copy.
            __m128i c0, c1, c2, c3, c4, c5, c6, c7;

            while (size >= 128)
            {
                c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
                c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
                c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 2);
                c3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 3);
                c4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 4);
                c5 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 5);
                c6 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 6);
                c7 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 7);
                src += 128;
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 2), c2);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 3), c3);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 4), c4);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 5), c5);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 6), c6);
                _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 7), c7);
                dst += 128;

                size -= 128;
            }

            goto tail;
        }
    }

    return ret;
}

