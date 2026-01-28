#include "ITokenExtractor.h"

#include <boost/algorithm/string.hpp>

#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <bit>

#if defined(__SSE2__)
#include <emmintrin.h>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

#endif


namespace DB
{

bool NgramTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_start = *pos;
    *token_length = 0;
    size_t code_points = 0;
    for (; code_points < n && *token_start + *token_length < length; ++code_points)
    {
        size_t sz = UTF8::seqLength(static_cast<UInt8>(data[*token_start + *token_length]));
        *token_length += sz;
    }
    *pos += UTF8::seqLength(static_cast<UInt8>(data[*pos]));
    return code_points == n;
}

bool NgramTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
{
    token.clear();

    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = *pos; i < length;)
    {
        if (escaped && (data[i] == '%' || data[i] == '_' || data[i] == '\\'))
        {
            token += data[i];
            ++code_points;
            escaped = false;
            ++i;
        }
        else if (!escaped && (data[i] == '%' || data[i] == '_'))
        {
            /// This token is too small, go to the next.
            token.clear();
            code_points = 0;
            escaped = false;
            *pos = ++i;
        }
        else if (!escaped && data[i] == '\\')
        {
            escaped = true;
            ++i;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[i]));
            for (size_t j = 0; j < sz; ++j)
                token += data[i + j];
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n)
        {
            *pos += UTF8::seqLength(static_cast<UInt8>(data[*pos]));
            return true;
        }
    }

    return false;
}

bool SplitTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_start = *pos;
    *token_length = 0;

    while (*pos < length)
    {
        if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos]))
        {
            /// Finish current token if any
            if (*token_length > 0)
                return true;
            *token_start = ++*pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++*pos;
            ++*token_length;
        }
    }

    return *token_length > 0;
}

bool SplitTokenExtractor::nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_start = *pos;
    *token_length = 0;

    while (*pos < length)
    {
#if defined(__SSE2__) && !defined(MEMORY_SANITIZER) /// We read uninitialized bytes and decide on the calculated mask
        // NOTE: we assume that `data` string is padded from the right with 15 bytes.
        const __m128i haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + *pos));
        const size_t haystack_length = 16;

#if defined(__SSE4_2__)
        // With the help of https://www.strchr.com/strcmp_and_strlen_using_sse_4.2
        const auto alnum_chars_ranges = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0,
                '\xFF', '\x80', 'z', 'a', 'Z', 'A', '9', '0');
        // Every bit represents if `haystack` character is in the ranges (1) or not (0)
        const unsigned result_bitmask = _mm_cvtsi128_si32(_mm_cmpestrm(alnum_chars_ranges, 8, haystack, haystack_length, _SIDD_CMP_RANGES));
#else
        // NOTE: -1 and +1 required since SSE2 has no `>=` and `<=` instructions on packed 8-bit integers (epi8).
        const auto number_begin =      _mm_set1_epi8('0' - 1);
        const auto number_end =        _mm_set1_epi8('9' + 1);
        const auto alpha_lower_begin = _mm_set1_epi8('a' - 1);
        const auto alpha_lower_end =   _mm_set1_epi8('z' + 1);
        const auto alpha_upper_begin = _mm_set1_epi8('A' - 1);
        const auto alpha_upper_end =   _mm_set1_epi8('Z' + 1);
        const auto zero =              _mm_set1_epi8(0);

        // every bit represents if `haystack` character `c` satisfies condition:
        // (c < 0) || (c > '0' - 1 && c < '9' + 1) || (c > 'a' - 1 && c < 'z' + 1) || (c > 'A' - 1 && c < 'Z' + 1)
        // < 0 since _mm_cmplt_epi8 threats chars as SIGNED, and so all chars > 0x80 are negative.
        const unsigned result_bitmask = _mm_movemask_epi8(_mm_or_si128(_mm_or_si128(_mm_or_si128(
                _mm_cmplt_epi8(haystack, zero),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, number_begin),      _mm_cmplt_epi8(haystack, number_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_lower_begin), _mm_cmplt_epi8(haystack, alpha_lower_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_upper_begin), _mm_cmplt_epi8(haystack, alpha_upper_end))));
#endif
        if (result_bitmask == 0)
        {
            if (*token_length != 0)
                // end of token started on previous haystack
                return true;

            *pos += haystack_length;
            continue;
        }

        const auto token_start_pos_in_current_haystack = std::countr_zero(result_bitmask);
        if (*token_length == 0)
            // new token
            *token_start = *pos + token_start_pos_in_current_haystack;
        else if (token_start_pos_in_current_haystack != 0)
            // end of token starting in one of previous haystacks
            return true;

        const auto token_bytes_in_current_haystack = std::countr_zero(~(result_bitmask >> token_start_pos_in_current_haystack));
        *token_length += token_bytes_in_current_haystack;

        *pos += token_start_pos_in_current_haystack + token_bytes_in_current_haystack;
        if (token_start_pos_in_current_haystack + token_bytes_in_current_haystack == haystack_length)
            // check if there are leftovers in next `haystack`
            continue;

        break;
#else
        if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos]))
        {
            /// Finish current token if any
            if (*token_length > 0)
                return true;
            *token_start = ++*pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++*pos;
            ++*token_length;
        }
#endif
    }

#if defined(__SSE2__) && !defined(MEMORY_SANITIZER)
    // Could happen only if string is not padded with zeros, and we accidentally hopped over the end of data.
    if (*token_start > length)
        return false;
    *token_length = std::min(length - *token_start, *token_length);
#endif

    return *token_length > 0;
}

bool SplitTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
{
    token.clear();
    bool bad_token = false; // % or _ before token
    bool escaped = false;
    while (*pos < length)
    {
        if (!escaped && (data[*pos] == '%' || data[*pos] == '_'))
        {
            token.clear();
            bad_token = true;
            ++*pos;
        }
        else if (!escaped && data[*pos] == '\\')
        {
            escaped = true;
            ++*pos;
        }
        else if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos]))
        {
            if (!bad_token && !token.empty())
                return true;

            token.clear();
            bad_token = false;
            escaped = false;
            ++*pos;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[*pos]));
            for (size_t j = 0; j < sz; ++j)
            {
                token += data[*pos];
                ++*pos;
            }
            escaped = false;
        }
    }

    return !bad_token && !token.empty();
}

void SplitTokenExtractor::substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
        // In order to avoid filter updates with incomplete tokens,
        // first token is ignored, unless substring is prefix and
        // last token is ignored, unless substring is suffix
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            bloom_filter.add(data + token_start, token_len);
}

void SplitTokenExtractor::substringToGinFilter(const char * data, size_t length, GinFilter & gin_filter, bool is_prefix, bool is_suffix) const
{
    gin_filter.setQueryString(data, length);

    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
        // In order to avoid filter updates with incomplete tokens,
        // first token is ignored, unless substring is prefix and
        // last token is ignored, unless substring is suffix
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            gin_filter.addTerm(data + token_start, token_len);
}

}
