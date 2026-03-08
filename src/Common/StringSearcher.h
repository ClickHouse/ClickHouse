#pragma once

#include <base/types.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <memory>

#include <Common/TargetSpecific.h>


#ifdef __SSE4_1__
#    include <smmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#endif

#if USE_MULTITARGET_CODE
#    include <immintrin.h>
#endif

#include <stringzilla/stringzilla.h>

namespace DB
{

/** Variants for searching a substring in a string.
  * In most cases, performance is less than Volnitsky (see Volnitsky.h).
  */

/// Case-sensitive searcher (delegates to StringZilla)
class CaseSensitiveStringSearcher final
{
    /// string to be searched for
    sz_cptr_t const needle;
    sz_cptr_t const needle_end;

public:
    CaseSensitiveStringSearcher(const UInt8 * needle_, size_t needle_size)
        : needle(reinterpret_cast<sz_cptr_t>(needle_))
        , needle_end(needle + needle_size)
    {
    }

    ALWAYS_INLINE bool compare(const UInt8 * /*haystack*/, const UInt8 * /*haystack_end*/, const UInt8 * pos) const
    {
        sz_cptr_t pos_cptr = reinterpret_cast<sz_cptr_t>(pos);
        size_t needle_size = needle_end - needle;

        if (needle_size < 32)
        {
            /// For short needles we can use a simple loop and avoid function calls and the mask preparation
            sz_cptr_t c = needle;
            while (c != needle_end && *c == *pos_cptr)
            {
                c++;
                pos_cptr++;
            }
            return c == needle_end;
        }
        return sz_equal(pos_cptr, needle, needle_size);
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (needle == needle_end)
            return haystack;

        sz_cptr_t haystack_cptr = reinterpret_cast<sz_cptr_t>(haystack);
        size_t haystack_size = haystack_end - haystack;
        size_t needle_size = needle_end - needle;

        const char * res = sz_find(haystack_cptr, haystack_size, needle, needle_size);

        if (!res)
            return haystack_end;
        return reinterpret_cast<const UInt8 *>(res);
    }

    const UInt8 * search(const UInt8 * haystack, size_t haystack_size) const { return search(haystack, haystack + haystack_size); }
};

/// Case-insensitive ASCII searcher
class ASCIICaseInsensitiveStringSearcher final
{
private:
    /// string to be searched for
    const uint8_t * const needle;
    const uint8_t * const needle_end;
    /// lower and uppercase variants of the first character in `needle`
    uint8_t l = 0;
    uint8_t u = 0;

#if defined(__SSE4_1__) || (defined(__aarch64__) && defined(__ARM_NEON))
#ifdef __SSE4_1__
    using Vec = __m128i;
    static Vec vecLoad(const void * p) { return _mm_loadu_si128(reinterpret_cast<const __m128i *>(p)); }
    static Vec vecCmpeq(Vec a, Vec b) { return _mm_cmpeq_epi8(a, b); }
    static Vec vecOr(Vec a, Vec b) { return _mm_or_si128(a, b); }
    static int vecMovemask(Vec v) { return _mm_movemask_epi8(v); }
    static Vec vecSet1(uint8_t v) { return _mm_set1_epi8(static_cast<int8_t>(v)); }
#elif defined(__aarch64__) && defined(__ARM_NEON)
    using Vec = uint8x16_t;
    static Vec vecLoad(const void * p) { return vld1q_u8(reinterpret_cast<const uint8_t *>(p)); }
    static Vec vecCmpeq(Vec a, Vec b) { return vceqq_u8(a, b); }
    static Vec vecOr(Vec a, Vec b) { return vorrq_u8(a, b); }
    static int vecMovemask(Vec v)
    {
        const uint8x16_t bitmask = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
        uint8x16_t masked = vandq_u8(v, bitmask);
        uint8x16_t paired = vpaddq_u8(masked, masked);
        paired = vpaddq_u8(paired, paired);
        paired = vpaddq_u8(paired, paired);
        return static_cast<int>(vgetq_lane_u16(vreinterpretq_u16_u8(paired), 0));
    }
    static Vec vecSet1(uint8_t v) { return vdupq_n_u8(v); }
#endif

    static constexpr size_t N = sizeof(Vec);

    /// vectors filled with `l` and `u`, for determining leftmost position of the first symbol
    Vec patl, patu;
    /// lower and uppercase vectors of first 16 characters of `needle`
    Vec cachel{};
    Vec cacheu{};
    int cachemask = 0;
#endif

public:
    ASCIICaseInsensitiveStringSearcher(const UInt8 * needle_, size_t needle_size)
        : needle(reinterpret_cast<const uint8_t *>(needle_))
        , needle_end(needle + needle_size)
    {
        if (needle_size == 0)
            return;

        l = static_cast<uint8_t>(std::tolower(*needle));
        u = static_cast<uint8_t>(std::toupper(*needle));

#if defined(__SSE4_1__) || (defined(__aarch64__) && defined(__ARM_NEON))
        patl = vecSet1(l);
        patu = vecSet1(u);

        uint8_t cache_l_bytes[N] = {};
        uint8_t cache_u_bytes[N] = {};
        const auto * needle_pos = needle;

        for (size_t i = 0; i < N; ++i)
        {
            if (needle_pos != needle_end)
            {
                cache_l_bytes[i] = static_cast<uint8_t>(std::tolower(*needle_pos));
                cache_u_bytes[i] = static_cast<uint8_t>(std::toupper(*needle_pos));
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }

        cachel = vecLoad(cache_l_bytes);
        cacheu = vecLoad(cache_u_bytes);
#endif
    }

    ALWAYS_INLINE bool compare(const UInt8 * /*haystack*/, const UInt8 * haystack_end, const UInt8 * pos) const
    {
#if defined(__SSE4_1__) || (defined(__aarch64__) && defined(__ARM_NEON))
        if (pos + N <= haystack_end)
        {
            const auto v_haystack = vecLoad(pos);
            const auto v_against_l = vecCmpeq(v_haystack, cachel);
            const auto v_against_u = vecCmpeq(v_haystack, cacheu);
            const auto v_against_l_or_u = vecOr(v_against_l, v_against_u);
            const auto mask = vecMovemask(v_against_l_or_u);

            if (0xffff == cachemask)
            {
                if (mask == cachemask)
                {
                    pos += N;
                    const auto * needle_pos = needle + N;

                    while (needle_pos < needle_end && std::tolower(*pos) == std::tolower(*needle_pos))
                    {
                        ++pos;
                        ++needle_pos;
                    }

                    if (needle_pos == needle_end)
                        return true;
                }
            }
            else if ((mask & cachemask) == cachemask)
                return true;

            return false;
        }
#endif

        if (*pos == l || *pos == u)
        {
            ++pos;
            const auto * needle_pos = needle + 1;

            while (needle_pos < needle_end && std::tolower(*pos) == std::tolower(*needle_pos))
            {
                ++pos;
                ++needle_pos;
            }

            if (needle_pos == needle_end)
                return true;
        }

        return false;
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (needle == needle_end)
            return haystack;

        while (haystack < haystack_end)
        {
#if defined(__SSE4_1__) || (defined(__aarch64__) && defined(__ARM_NEON))
            if (haystack + N <= haystack_end)
            {
                const auto v_haystack = vecLoad(haystack);
                const auto v_against_l = vecCmpeq(v_haystack, patl);
                const auto v_against_u = vecCmpeq(v_haystack, patu);
                const auto v_against_l_or_u = vecOr(v_against_l, v_against_u);

                const auto mask = vecMovemask(v_against_l_or_u);

                if (mask == 0)
                {
                    haystack += N;
                    continue;
                }

                const auto offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack + N <= haystack_end)
                {
                    const auto v_haystack_offset = vecLoad(haystack);
                    const auto v_against_l_offset = vecCmpeq(v_haystack_offset, cachel);
                    const auto v_against_u_offset = vecCmpeq(v_haystack_offset, cacheu);
                    const auto v_against_l_or_u_offset = vecOr(v_against_l_offset, v_against_u_offset);
                    const auto mask_offset = vecMovemask(v_against_l_or_u_offset);

                    if (0xffff == cachemask)
                    {
                        if (mask_offset == cachemask)
                        {
                            const auto * haystack_pos = haystack + N;
                            const auto * needle_pos = needle + N;

                            while (haystack_pos < haystack_end && needle_pos < needle_end
                                   && std::tolower(*haystack_pos) == std::tolower(*needle_pos))
                            {
                                ++haystack_pos;
                                ++needle_pos;
                            }

                            if (needle_pos == needle_end)
                                return haystack;
                        }
                    }
                    else if ((mask_offset & cachemask) == cachemask)
                        return haystack;

                    ++haystack;
                    continue;
                }
            }
#endif

            if (haystack == haystack_end)
                return haystack_end;

            if (*haystack == l || *haystack == u)
            {
                const auto * haystack_pos = haystack + 1;
                const auto * needle_pos = needle + 1;

                while (haystack_pos < haystack_end && needle_pos < needle_end && std::tolower(*haystack_pos) == std::tolower(*needle_pos))
                {
                    ++haystack_pos;
                    ++needle_pos;
                }

                if (needle_pos == needle_end)
                    return haystack;
            }

            ++haystack;
        }

        return haystack_end;
    }

    const UInt8 * search(const UInt8 * haystack, size_t haystack_size) const { return search(haystack, haystack + haystack_size); }
};


/// Case-insensitive UTF-8 searcher
/// Uses StringZilla on x86_64_v4 (AVX-512), Poco + SIMD on ARM NEON, x86_64_v3 (AVX2), and Default (SSE4.1).

/// Default (Poco-based) implementation for older x86_64 CPUs and ARM NEON.
/// Declared directly (not via DECLARE_DEFAULT_CODE) because the class uses #ifdef for SIMD members.
namespace TargetSpecific::Default
{

class UTF8CaseInsensitiveSearcherImpl
{
private:
    using UTF8SequenceBuffer = uint8_t[6];

    const uint8_t * const needle;
    const size_t needle_size;
    const uint8_t * const needle_end = needle + needle_size;
    bool first_needle_symbol_is_ascii = false;
    uint8_t l = 0;
    uint8_t u = 0;

#if defined(__SSE4_1__) || (defined(__aarch64__) && defined(__ARM_NEON))
#ifdef __SSE4_1__
    using Vec = __m128i;
    static Vec vecLoad(const void * p) { return _mm_loadu_si128(reinterpret_cast<const __m128i *>(p)); }
    static Vec vecCmpeq(Vec a, Vec b) { return _mm_cmpeq_epi8(a, b); }
    static Vec vecOr(Vec a, Vec b) { return _mm_or_si128(a, b); }
    static Vec vecXor(Vec a, Vec b) { return _mm_xor_si128(a, b); }
    static Vec vecAnd(Vec a, Vec b) { return _mm_and_si128(a, b); }
    static int vecMovemask(Vec v) { return _mm_movemask_epi8(v); }
    static Vec vecSet1(uint8_t v) { return _mm_set1_epi8(static_cast<int8_t>(v)); }
    static Vec vecZero() { return _mm_setzero_si128(); }
#elif defined(__aarch64__) && defined(__ARM_NEON)
    using Vec = uint8x16_t;
    static Vec vecLoad(const void * p) { return vld1q_u8(reinterpret_cast<const uint8_t *>(p)); }
    static Vec vecCmpeq(Vec a, Vec b) { return vceqq_u8(a, b); }
    static Vec vecOr(Vec a, Vec b) { return vorrq_u8(a, b); }
    static Vec vecXor(Vec a, Vec b) { return veorq_u8(a, b); }
    static Vec vecAnd(Vec a, Vec b) { return vandq_u8(a, b); }
    static int vecMovemask(Vec v)
    {
        const uint8x16_t bitmask = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
        uint8x16_t masked = vandq_u8(v, bitmask);
        uint8x16_t paired = vpaddq_u8(masked, masked);
        paired = vpaddq_u8(paired, paired);
        paired = vpaddq_u8(paired, paired);
        return static_cast<int>(vgetq_lane_u16(vreinterpretq_u16_u8(paired), 0));
    }
    static Vec vecSet1(uint8_t v) { return vdupq_n_u8(v); }
    static Vec vecZero() { return vdupq_n_u8(0); }
#endif

    Vec patl;
    Vec patu;
    Vec cachel{};
    Vec cacheu{};
    uint32_t cachemask = 0;
    size_t cache_valid_len = 0;
    size_t cache_actual_len = 0;
#endif

    bool force_fallback = false;

public:
    UTF8CaseInsensitiveSearcherImpl(const UInt8 * needle_, size_t needle_size_);

    bool compareTrivial(const UInt8 * haystack_pos, const UInt8 * haystack_end, const uint8_t * needle_pos) const;
    bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const;
    const UInt8 * search(const UInt8 * haystack, const UInt8 * haystack_end) const;
};

} // namespace TargetSpecific::Default

/// AVX2-based implementation for x86_64_v3 CPUs (32-byte vectors instead of 16).
/// Uses DECLARE macro since the class has no #ifdef - AVX2 intrinsics are guaranteed by the target attribute.
DECLARE_X86_64_V3_SPECIFIC_CODE(

class UTF8CaseInsensitiveSearcherImpl
{
private:
    using UTF8SequenceBuffer = uint8_t[6];

    const uint8_t * const needle;
    const size_t needle_size;
    const uint8_t * const needle_end = needle + needle_size;
    bool first_needle_symbol_is_ascii = false;
    uint8_t l = 0;
    uint8_t u = 0;

    __m256i patl;
    __m256i patu;
    __m256i cachel = _mm256_setzero_si256();
    __m256i cacheu = _mm256_setzero_si256();
    uint32_t cachemask = 0;
    size_t cache_valid_len = 0;
    size_t cache_actual_len = 0;

    bool force_fallback = false;

public:
    UTF8CaseInsensitiveSearcherImpl(const UInt8 * needle_, size_t needle_size_);

    bool compareTrivial(const UInt8 * haystack_pos, const UInt8 * haystack_end, const uint8_t * needle_pos) const;
    bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const;
    const UInt8 * search(const UInt8 * haystack, const UInt8 * haystack_end) const;
};

) // DECLARE_X86_64_V3_SPECIFIC_CODE

/// StringZilla-based implementation (for x86_64_v4 with AVX-512)
DECLARE_X86_64_V4_SPECIFIC_CODE(

class UTF8CaseInsensitiveSearcherImpl
{
private:
    sz_cptr_t const needle;
    sz_cptr_t const needle_end;
    mutable sz_utf8_case_insensitive_needle_metadata_t needle_metadata;

public:
    UTF8CaseInsensitiveSearcherImpl(const UInt8 * needle_, size_t needle_size)
        : needle(reinterpret_cast<sz_cptr_t>(needle_))
        , needle_end(needle + needle_size)
        , needle_metadata{}
    {
    }

    bool compare(const UInt8 * /*haystack*/, const UInt8 * /*haystack_end*/, const UInt8 * pos) const
    {
        sz_cptr_t pos_cptr = reinterpret_cast<sz_cptr_t>(pos);
        size_t needle_size = needle_end - needle;
        sz_ordering_t result = sz_utf8_case_insensitive_order(pos_cptr, needle_size, needle, needle_size);
        return result == sz_equal_k;
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        if (needle == needle_end)
            return haystack;

        sz_cptr_t haystack_cptr = reinterpret_cast<sz_cptr_t>(haystack);
        size_t haystack_size = haystack_end - haystack;
        size_t needle_size = needle_end - needle;

        size_t matched_length = 0;
        const char * res
            = sz_utf8_case_insensitive_find(haystack_cptr, haystack_size, needle, needle_size, &needle_metadata, &matched_length);

        if (!res)
            return haystack_end;
        return reinterpret_cast<const UInt8 *>(res);
    }
};

) // DECLARE_X86_64_V4_SPECIFIC_CODE

#if defined(__aarch64__)
/// On ARM, use Default (Poco + NEON) implementation
class UTF8CaseInsensitiveStringSearcher final
{
private:
    TargetSpecific::Default::UTF8CaseInsensitiveSearcherImpl impl;

public:
    UTF8CaseInsensitiveStringSearcher(const UInt8 * needle_, size_t needle_size)
        : impl(needle_, needle_size)
    {
    }

    ALWAYS_INLINE bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const
    {
        return impl.compare(haystack, haystack_end, pos);
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
        return impl.search(haystack, haystack_end);
    }

    const UInt8 * search(const UInt8 * haystack, size_t haystack_size) const { return search(haystack, haystack + haystack_size); }
};

#else
/// On x86_64: runtime dispatch between x86_64_v4 (StringZilla), x86_64_v3 (AVX2), and Default (SSE4.1)
class UTF8CaseInsensitiveStringSearcher final
{
private:
    std::unique_ptr<TargetSpecific::Default::UTF8CaseInsensitiveSearcherImpl> impl_default;
#if USE_MULTITARGET_CODE
    std::unique_ptr<TargetSpecific::x86_64_v4::UTF8CaseInsensitiveSearcherImpl> impl_v4;
    std::unique_ptr<TargetSpecific::x86_64_v3::UTF8CaseInsensitiveSearcherImpl> impl_v3;
#endif
    enum class Impl : uint8_t { Default, V3, V4 };

    static Impl selectImpl()
    {
#if USE_MULTITARGET_CODE
        if (isArchSupported(TargetArch::x86_64_v4))
            return Impl::V4;
        if (isArchSupported(TargetArch::x86_64_v3))
            return Impl::V3;
#endif
        return Impl::Default;
    }

    const Impl active = selectImpl();

public:
    UTF8CaseInsensitiveStringSearcher(const UInt8 * needle_, size_t needle_size)
    {
        switch (active)
        {
#if USE_MULTITARGET_CODE
            case Impl::V4:
                impl_v4 = std::make_unique<TargetSpecific::x86_64_v4::UTF8CaseInsensitiveSearcherImpl>(needle_, needle_size);
                break;
            case Impl::V3:
                impl_v3 = std::make_unique<TargetSpecific::x86_64_v3::UTF8CaseInsensitiveSearcherImpl>(needle_, needle_size);
                break;
#endif
            default:
                impl_default = std::make_unique<TargetSpecific::Default::UTF8CaseInsensitiveSearcherImpl>(needle_, needle_size);
                break;
        }
    }

    ALWAYS_INLINE bool compare(const UInt8 * haystack, const UInt8 * haystack_end, const UInt8 * pos) const
    {
#if USE_MULTITARGET_CODE
        if (active == Impl::V4)
            return impl_v4->compare(haystack, haystack_end, pos);
        if (active == Impl::V3)
            return impl_v3->compare(haystack, haystack_end, pos);
#endif
        return impl_default->compare(haystack, haystack_end, pos);
    }

    const UInt8 * search(const UInt8 * haystack, const UInt8 * const haystack_end) const
    {
#if USE_MULTITARGET_CODE
        if (active == Impl::V4)
            return impl_v4->search(haystack, haystack_end);
        if (active == Impl::V3)
            return impl_v3->search(haystack, haystack_end);
#endif
        return impl_default->search(haystack, haystack_end);
    }

    const UInt8 * search(const UInt8 * haystack, size_t haystack_size) const { return search(haystack, haystack + haystack_size); }
};
#endif

/// Use only with short haystacks where cheap initialization is required.
template <bool CaseInsensitive>
struct StdLibASCIIStringSearcher
{
    const char * const needle_start;
    const char * const needle_end;

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StdLibASCIIStringSearcher(const CharT * const needle_start_, size_t needle_size_)
        : needle_start(reinterpret_cast<const char *>(needle_start_))
        , needle_end(reinterpret_cast<const char *>(needle_start) + needle_size_)
    {}

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, const CharT * const haystack_end) const
    {
        if constexpr (CaseInsensitive)
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return std::toupper(c1) == std::toupper(c2); });
        else
            return std::search(
                haystack_start, haystack_end, needle_start, needle_end,
                [](char c1, char c2) { return c1 == c2; });
    }

    template <typename CharT>
    requires (sizeof(CharT) == 1)
    const CharT * search(const CharT * haystack_start, size_t haystack_length) const
    {
        return search(haystack_start, haystack_start + haystack_length);
    }
};

}
