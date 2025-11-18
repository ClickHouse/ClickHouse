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

bool DefaultTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool DefaultTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
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

bool SplitTokenExtractor::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t * /*token_start*/, String & /*token_length*/) const
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

bool NoOpTokenExtractor::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t * /*token_start*/, String & /*token_length*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "NoOpTokenExtractor::nextInStringLike is not implemented");
}

SparseGramTokenExtractor::SparseGramTokenExtractor(size_t min_length, size_t max_length, std::optional<size_t> min_cutoff_length_)
    : ITokenExtractorHelper(Type::SparseGram)
    , sparse_grams_iterator(min_length, max_length, min_cutoff_length_)
{
}

bool SparseGramTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool SparseGramTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
{
    if (std::tie(data, length) != std::tie(previous_data, previous_len))
    {
        previous_data = data;
        previous_len = length;
        sparse_grams_iterator.set(data, data + length);
    }

    token.clear();

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
        for (size_t i = next_begin - data; i < static_cast<size_t>(next_end - data);)
        {
            /// Escaped sequences are not supported by sparse grams tokenizers.
            /// It requires pushing down this logic into sparse grams implementation,
            /// because it changes the split into sparse grams. We don't want to do that now.
            if (data[i] == '%' || data[i] == '_' || data[i] == '\\')
            {
                token.clear();
                match_substring = false;
                break;
            }
            else
            {
                const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[i]));
                for (size_t j = 0; j < sz; ++j)
                    token.push_back(data[i + j]);
                i += sz;
            }
        }
        if (match_substring)
        {
            *pos = next_begin - data;
            return true;
        }
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
