#include <Common/StringUtils.h>

#include <Common/TargetSpecific.h>

#if defined(__AVX2__) || defined(__AVX512F__) || USE_MULTITARGET_CODE
#include <immintrin.h>
#endif

namespace
{
constexpr size_t AVX512_ASCII_THRESHOLD = 16 * 1024;
}

#if USE_MULTITARGET_CODE || (defined(__AVX512F__) && defined(__AVX512BW__))
namespace DB::TargetSpecific::x86_64_v4
{
#if USE_MULTITARGET_CODE
X86_64_V4_FUNCTION_SPECIFIC_ATTRIBUTE
#endif
static bool NO_INLINE isAllASCIIAVX512(const UInt8 * data, size_t size)
{
    if (unlikely(_mm512_movepi8_mask(_mm512_loadu_si512(reinterpret_cast<const void *>(data))) != 0))
        return false;

    __m512i mask0 = _mm512_setzero_si512();
    __m512i mask1 = _mm512_setzero_si512();
    __m512i mask2 = _mm512_setzero_si512();
    __m512i mask3 = _mm512_setzero_si512();

    const auto address_alignment = reinterpret_cast<uintptr_t>(data) & 63;
    size_t i = (64 - address_alignment) & 63;
    if (i == 0)
        i = 64;

    for (; i + 256 <= size; i += 256)
    {
        mask0 = _mm512_or_si512(mask0, _mm512_load_si512(reinterpret_cast<const void *>(data + i)));
        mask1 = _mm512_or_si512(mask1, _mm512_load_si512(reinterpret_cast<const void *>(data + i + 64)));
        mask2 = _mm512_or_si512(mask2, _mm512_load_si512(reinterpret_cast<const void *>(data + i + 128)));
        mask3 = _mm512_or_si512(mask3, _mm512_load_si512(reinterpret_cast<const void *>(data + i + 192)));
    }

    __m512i mask = _mm512_or_si512(
        _mm512_or_si512(mask0, mask1),
        _mm512_or_si512(mask2, mask3));

    for (; i + 64 <= size; i += 64)
        mask = _mm512_or_si512(mask, _mm512_load_si512(reinterpret_cast<const void *>(data + i)));

    if (i < size)
    {
        const auto tail_mask = static_cast<__mmask64>(~UInt64{0} >> (64 - (size - i)));
        mask = _mm512_or_si512(mask, _mm512_maskz_loadu_epi8(tail_mask, data + i));
    }

    return _mm512_movepi8_mask(mask) == 0;
}
}
#endif


namespace impl
{

bool startsWith(const std::string & s, const char * prefix, size_t prefix_size)
{
    return s.size() >= prefix_size && 0 == memcmp(s.data(), prefix, prefix_size);
}

bool endsWith(const std::string & s, const char * suffix, size_t suffix_size)
{
    return s.size() >= suffix_size && 0 == memcmp(s.data() + s.size() - suffix_size, suffix, suffix_size);
}

}

bool isAllASCII(const UInt8 * data, size_t size)
{
#if defined(__AVX512F__) && defined(__AVX512BW__)
    if (size >= AVX512_ASCII_THRESHOLD)
        return DB::TargetSpecific::x86_64_v4::isAllASCIIAVX512(data, size);
#elif USE_MULTITARGET_CODE
    if (size >= AVX512_ASCII_THRESHOLD && DB::isArchSupported(DB::TargetArch::x86_64_v4))
        return DB::TargetSpecific::x86_64_v4::isAllASCIIAVX512(data, size);
#endif

#if defined(__AVX2__)
    __m256i masks = _mm256_setzero_si256();

    size_t i = 0;
    for (; i + 32 <= size; i += 32)
    {
        __m256i bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
        masks = _mm256_or_si256(masks, bytes);
    }
    int mask = _mm256_movemask_epi8(masks);

    UInt8 tail_mask = 0;
    for (; i < size; i++)
        tail_mask |= data[i];

    mask |= (tail_mask & 0x80);
    return !mask;
#else
    UInt8 mask = 0;
    for (size_t i = 0; i < size; ++i)
        mask |= data[i];

    return !(mask & 0x80);
#endif
}

LikePatternFixedPrefix extractFixedPrefixFromLikePattern(std::string_view like_pattern, bool requires_perfect_prefix)
{
    String fixed_prefix;
    fixed_prefix.reserve(like_pattern.size());

    const char * pos = like_pattern.data();
    const char * end = pos + like_pattern.size();
    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
            {
                bool is_perfect_prefix = std::all_of(pos, end, [](auto c) { return c == '%'; });
                if (requires_perfect_prefix && !is_perfect_prefix)
                    return {};
                return {.prefix = fixed_prefix, .is_perfect = is_perfect_prefix};
            }
            case '\\':
            {
                ++pos;
                /// A trailing escape is an invalid pattern the matcher rejects; never report it as exact,
                /// or a point range would prune the granule and skip that exception.
                if (pos == end)
                {
                    if (requires_perfect_prefix)
                        return {};
                    return {.prefix = fixed_prefix};
                }
                /// Only '\%', '\_' and '\\' drop the backslash, an unknown escape keeps it.
                if (*pos != '%' && *pos != '_' && *pos != '\\')
                    fixed_prefix += '\\';
                fixed_prefix += *pos;
                break;
            }
            default:
            {
                fixed_prefix += *pos;
            }
        }

        ++pos;
    }
    /// No wildcard was found, so the pattern is an exact match of `fixed_prefix`.
    return {.prefix = fixed_prefix, .is_exact = true};
}

/** For a given string, get a minimum string that is strictly greater than all strings with this prefix,
  *  or return an empty string if there are no such strings.
  */
String firstStringThatIsGreaterThanAllStringsWithPrefix(const String & prefix)
{
    /** Increment the last byte of the prefix by one. But if it is max (255), then remove it and increase the previous one.
      * Example (for convenience, suppose that the maximum value of byte is `z`)
      * abcx -> abcy
      * abcz -> abd
      * zzz -> empty string
      * z -> empty string
      */

    String res = prefix;

    while (!res.empty() && static_cast<UInt8>(res.back()) == std::numeric_limits<UInt8>::max())
        res.pop_back();

    if (res.empty())
        return res;

    res.back() = static_cast<char>(1 + static_cast<UInt8>(res.back()));
    return res;
}
