#include <Common/StringUtils.h>

#include <Common/TargetSpecific.h>

#if USE_MULTITARGET_CODE
#include <immintrin.h>
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

DECLARE_DEFAULT_CODE(
static bool isAllASCII(const UInt8 * data, size_t size)
{
    UInt8 mask = 0;
    for (size_t i = 0; i < size; ++i)
        mask |= data[i];

    return !(mask & 0x80);
})

DECLARE_SSE42_SPECIFIC_CODE(
/// Copy from https://github.com/lemire/fastvalidate-utf-8/blob/master/include/simdasciicheck.h
static bool isAllASCII(const UInt8 * data, size_t size)
{
    __m128i masks = _mm_setzero_si128();

    size_t i = 0;
    for (; i + 16 <= size; i += 16)
    {
        __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
        masks = _mm_or_si128(masks, bytes);
    }
    int mask = _mm_movemask_epi8(masks);

    UInt8 tail_mask = 0;
    for (; i < size; i++)
        tail_mask |= data[i];

    mask |= (tail_mask & 0x80);
    return !mask;
})

DECLARE_AVX2_SPECIFIC_CODE(
static bool isAllASCII(const UInt8 * data, size_t size)
{
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
})

bool isAllASCII(const UInt8 * data, size_t size)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(DB::TargetArch::AVX2))
        return TargetSpecific::AVX2::isAllASCII(data, size);
    if (isArchSupported(DB::TargetArch::SSE42))
        return TargetSpecific::SSE42::isAllASCII(data, size);
#endif
    return TargetSpecific::Default::isAllASCII(data, size);
}

/// Returns the prefix of like_pattern before the first wildcard, e.g. 'Hello\_World% ...' --> 'Hello\_World'
/// We call a pattern "perfect prefix" if:
/// - (1) the pattern has a wildcard
/// - (2) the first wildcard is '%' and is only followed by nothing or other '%'
/// e.g. 'test%' or 'test%% has perfect prefix 'test', 'test%x', 'test%_' or 'test_' has no perfect prefix.
std::tuple<String, bool> extractFixedPrefixFromLikePattern(std::string_view like_pattern, bool requires_perfect_prefix)
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
                if (requires_perfect_prefix)
                {
                    if (is_perfect_prefix)
                        return {fixed_prefix, true};
                    else
                        return {"", false};
                }
                else
                {
                    return {fixed_prefix, is_perfect_prefix};
                }
            }
            case '\\':
            {
                ++pos;
                if (pos == end)
                    break;
                [[fallthrough]];
            }
            default:
            {
                fixed_prefix += *pos;
            }
        }

        ++pos;
    }
    /// If we can reach this code, it means there was no wildcard found in the pattern, so it is not a perfect prefix
    if (requires_perfect_prefix)
        return {"", false};
    return {fixed_prefix, false};
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
