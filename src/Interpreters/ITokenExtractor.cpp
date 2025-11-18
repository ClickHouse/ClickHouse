#include <Interpreters/ITokenExtractor.h>

#include <boost/algorithm/string.hpp>

#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>

#if defined(__SSE2__)
#  include <emmintrin.h>
#  if defined(__SSE4_2__)
#    include <nmmintrin.h>
#  endif
#endif

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

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

bool NgramTokenExtractor::nextInStringLike(
    const char * data, size_t length, size_t * pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_length = 0;
    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = *pos; i < length;)
    {
        if (code_points == 0)
            *token_start = i;

        if (escaped && (data[i] == '%' || data[i] == '_' || data[i] == '\\'))
        {
            ++code_points;
            escaped = false;
            ++i;
        }
        else if (!escaped && (data[i] == '%' || data[i] == '_'))
        {
            /// This token is too small, go to the next.
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
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n)
        {
            *token_length = i - *token_start;
            *pos += UTF8::seqLength(static_cast<UInt8>(data[*pos]));
            return true;
        }
    }

    return false;
}

bool DefaultTokenExtractor::nextInString(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool DefaultTokenExtractor::nextInStringLike(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_length = 0;
    bool bad_token = false; // % or _ before token
    bool escaped = false;
    while (*pos < length)
    {
        if (*token_length == 0)
            *token_start = *pos;

        if (!escaped && (data[*pos] == '%' || data[*pos] == '_'))
        {
            *token_length = 0;
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
            if (!bad_token && *token_length != 0)
                return true;

            *token_length = 0;
            bad_token = false;
            escaped = false;
            ++*pos;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[*pos]));
            *token_length += sz;
            *pos += sz;
            escaped = false;
        }
    }

    return !bad_token && *token_length != 0;
}

void DefaultTokenExtractor::substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
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

void DefaultTokenExtractor::substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
    {
        // In order to avoid filter updates with incomplete tokens,
        // first token is ignored, unless substring is prefix and
        // last token is ignored, unless substring is suffix
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            tokens.push_back({data + token_start, token_len});
    }
}

namespace
{

bool startsWithSeparator(const char * data, size_t length, size_t pos, const std::vector<String> & separators, std::string & matched_sep)
{
    for (const auto & separator : separators)
    {
        size_t separator_length = separator.size();
        if (pos + separator_length <= length && std::memcmp(data + pos, separator.data(), separator_length) == 0)
        {
            matched_sep = separator;
            return true;
        }
    }
    return false;
}

}

bool SplitTokenExtractor::nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const
{
    size_t i = *pos;
    std::string matched_separators;

    /// Skip prefix of separators
    while (i < length && startsWithSeparator(data, length, i, separators, matched_separators))
        i += matched_separators.size();

    if (i >= length)
    {
        *pos = length;
        return false;
    }

    /// Read token until next separator
    size_t start = i;
    while (i < length && !startsWithSeparator(data, length, i, separators, matched_separators))
        ++i;

    *token_start = start;
    *token_length = i - start;
    *pos = i;

    return true;
}

bool SplitTokenExtractor::nextInStringLike(
    const char * /*data*/, size_t /*length*/, size_t * /*pos*/, size_t * /* token_start */, size_t * /* token_length */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StringTokenExtractor::nextInStringLike is not implemented");
}

bool NoOpTokenExtractor::nextInString(const char * /*data*/, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const
{
    if (*pos == 0)
    {
        *pos = length;
        *token_start = 0;
        *token_length = length;
        return true;
    }
    return false;
}

bool NoOpTokenExtractor::nextInStringLike(
    const char * /*data*/, size_t /*length*/, size_t * /*pos*/, size_t * /* token_start */, size_t * /* token_length */) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "NoOpTokenExtractor::nextInStringLike is not implemented");
}

SparseGramTokenExtractor::SparseGramTokenExtractor(size_t min_length, size_t max_length, std::optional<size_t> min_cutoff_length_)
    : ITokenExtractorHelper(Type::SparseGram)
    , sparse_grams_iterator(min_length, max_length, min_cutoff_length_)
{
}

bool SparseGramTokenExtractor::nextInString(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    if (std::tie(data, length) != std::tie(previous_data, previous_len))
    {
        previous_data = data;
        previous_len = length;
        sparse_grams_iterator.set(data, data + length);
    }

    Pos next_begin;
    Pos next_end;
    if (!sparse_grams_iterator.get(next_begin, next_end))
    {
        previous_data = nullptr;
        previous_len = 0;
        return false;
    }
    *token_start = next_begin - data;
    *token_length = next_end - next_begin;
    *pos = *token_start;

    return true;
}

bool SparseGramTokenExtractor::nextInStringLike(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    if (std::tie(data, length) != std::tie(previous_data, previous_len))
    {
        previous_data = data;
        previous_len = length;
        sparse_grams_iterator.set(data, data + length);
    }

    *token_length = 0;
    while (true)
    {
        Pos next_begin;
        Pos next_end;
        if (!sparse_grams_iterator.get(next_begin, next_end))
        {
            previous_data = nullptr;
            previous_len = 0;
            return false;
        }
        bool match_substring = true;
        size_t local_len = 0;
        for (size_t i = next_begin - data; i < static_cast<size_t>(next_end - data);)
        {
            /// Escaped sequences are not supported by sparse grams tokenizers.
            /// It requires pushing down this logic into sparse grams implementation,
            /// because it changes the split into sparse grams. We don't want to do that now.
            if (data[i] == '%' || data[i] == '_' || data[i] == '\\')
            {
                local_len = 0;
                match_substring = false;
                break;
            }
            else
            {
                const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[i]));
                local_len += sz;
                i += sz;
            }
        }
        if (match_substring)
        {
            *token_start = next_begin - data;
            *token_length = local_len;
            *pos = *token_start;
            return true;
        }
    }
}

bool StandardTokenExtractor::nextInString(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    return nextInStringImpl<false>(data, length, pos, token_start, token_length);
}

bool StandardTokenExtractor::nextInStringLike(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    return nextInStringImpl<true>(data, length, pos, token_start, token_length);
}

template <bool is_like_pattern>
bool StandardTokenExtractor::nextInStringImpl(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_length = 0;
    [[maybe_unused]] bool bad_token = false; // For LIKE: % or _ before token makes it invalid
    [[maybe_unused]] bool escaped = false;   // For LIKE: track escape state

    while (*pos < length)
    {
        *token_start = *pos;
        char c = data[*pos];

        /// =============================================
        /// LIKE pattern: handle wildcards and escapes
        /// =============================================
        if constexpr (is_like_pattern)
        {
            /// Wildcard % or _ before token invalidates it
            if (!escaped && (c == '%' || c == '_'))
            {
                *token_length = 0;
                bad_token = true;
                ++(*pos);
                continue;
            }

            /// Escape character
            if (!escaped && c == '\\')
            {
                escaped = true;
                ++(*pos);
                continue;
            }
        }

        /// =======================
        /// 1. ASCII fast path
        /// =======================
        if (isAlphaNumericASCII(c) || c == '_')
        {
            size_t token_alnum_count = 0;
            *token_length = 0;

            while (*pos < length)
            {
                char cur = data[*pos];

                /// 1a. ASCII letter or digit
                if (isAlphaNumericASCII(cur))
                {
                    ++(*pos);
                    ++(*token_length);
                    ++token_alnum_count;
                    if constexpr (is_like_pattern)
                        escaped = false;
                    continue;
                }

                /// 1b. Underscore
                if (cur == '_')
                {
                    /// In LIKE pattern, unescaped _ is wildcard
                    if constexpr (is_like_pattern)
                    {
                        if (!escaped)
                            break;
                    }

                    ++(*pos);
                    ++(*token_length);
                    if constexpr (is_like_pattern)
                        escaped = false;
                    continue;
                }

                /// Check if next character exists
                if (*pos + 1 >= length)
                    break;

                char next_c = data[*pos + 1];
                char prev_c = (*pos > 0) ? data[*pos - 1] : '\0';

                /// 1c. Colon: connects letters only
                if (cur == ':' && isAlphaASCII(prev_c) && isAlphaASCII(next_c))
                {
                    ++(*pos);
                    ++(*token_length);
                    if constexpr (is_like_pattern)
                        escaped = false;
                    continue;
                }

                /// 1d. Dot or single quote: connects letter-letter or digit-digit
                if ((cur == '.' || cur == '\'') &&
                    ((isAlphaASCII(prev_c) && isAlphaASCII(next_c)) ||
                     (isNumericASCII(prev_c) && isNumericASCII(next_c))))
                {
                    ++(*pos);
                    ++(*token_length);
                    if constexpr (is_like_pattern)
                        escaped = false;
                    continue;
                }

                /// Token ends
                break;
            }

            /// Token must contain at least one alphanumeric character
            if (token_alnum_count > 0)
            {
                /// In LIKE mode, bad_token invalidates the result
                if constexpr (is_like_pattern)
                {
                    if (bad_token)
                    {
                        *token_length = 0;
                        bad_token = false;
                        continue;
                    }
                }

                /// Check if token is a stop word
                std::string_view token_view(data + *token_start, *token_length);
                if (stop_words.contains(token_view))
                {
                    *token_length = 0;
                    if constexpr (is_like_pattern)
                        bad_token = false;
                    continue;
                }
                return true;
            }

            /// Invalid token (underscore only), continue searching
            *token_length = 0;
            if constexpr (is_like_pattern)
                bad_token = false;
            continue;
        }

        /// =======================
        /// 2. ASCII non-alphanumeric: skip
        /// =======================
        if (isASCII(c))
        {
            ++(*pos);
            if constexpr (is_like_pattern)
            {
                bad_token = false;
                escaped = false;
            }
            continue;
        }

        /// =======================
        /// 3. Unicode character handling
        /// =======================
        size_t char_len = UTF8::seqLength(static_cast<UInt8>(c));

        /// Prevent out-of-bounds
        if (*pos + char_len > length)
        {
            ++(*pos);
            continue;
        }

        std::string_view utf8_char(data + *pos, char_len);

        /// 3a. Stop words (Chinese punctuation, etc.): skip
        if (stop_words.contains(utf8_char))
        {
            *pos += char_len;
            if constexpr (is_like_pattern)
            {
                bad_token = false;
                escaped = false;
            }
            continue;
        }

        /// 3b. Chinese character or other Unicode: single-character token
        /// In LIKE mode, check if preceded by wildcard
        if constexpr (is_like_pattern)
        {
            if (bad_token)
            {
                *pos += char_len;
                bad_token = false;
                escaped = false;
                continue;
            }
        }

        *token_start = *pos;
        *token_length = char_len;
        *pos += char_len;

        if constexpr (is_like_pattern)
            escaped = false;
        return true;
    }

    return false;
}

void StandardTokenExtractor::substringToBloomFilter(
    const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
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

void StandardTokenExtractor::substringToTokens(
    const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
    {
        // In order to avoid filter updates with incomplete tokens,
        // first token is ignored, unless substring is prefix and
        // last token is ignored, unless substring is suffix
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            tokens.push_back({data + token_start, token_len});
    }
}

void forEachTokenToBloomFilter(const ITokenExtractor & extractor, const char * data, size_t length, BloomFilter & bloom_filter)
{
    forEachToken(
        extractor,
        data,
        length,
        [&](const char * token_start, size_t token_length)
        {
            bloom_filter.add(token_start, token_length);
            return false;
        });
}

}
