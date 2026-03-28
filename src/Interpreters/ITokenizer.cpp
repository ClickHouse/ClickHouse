#include <Interpreters/ITokenizer.h>

#include <Common/quoteString.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>

#if defined(__SSE2__)
#  include <emmintrin.h>
#  if defined(__SSE4_2__)
#    include <nmmintrin.h>
#  endif
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

bool NgramsTokenizer::nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
{
    token_start = pos;
    token_length = 0;
    size_t code_points = 0;
    for (; code_points < n && token_start + token_length < length; ++code_points)
    {
        size_t sz = UTF8::seqLength(static_cast<UInt8>(data[token_start + token_length]));
        token_length += sz;
    }
    /// The sequence length is determined by the first character, but we should always check it does not go beyond the buffer length.
    token_length = std::min(token_length, length - token_start);
    pos += UTF8::seqLength(static_cast<UInt8>(data[pos]));
    return code_points == n;
}

bool NgramsTokenizer::nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const
{
    token.clear();

    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = pos; i < length;)
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
            ++i;
            pos = i;
        }
        else if (!escaped && data[i] == '\\')
        {
            escaped = true;
            ++i;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[i]));
            /// The sequence length is determined by the first character, but we should always check it does not go beyond the buffer length.
            for (size_t j = 0; j < sz && i + j < length; ++j)
                token += data[i + j];
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n)
        {
            pos += UTF8::seqLength(static_cast<UInt8>(data[pos]));
            return true;
        }
    }

    return false;
}

void NgramsTokenizer::substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool, bool) const
{
    stringToBloomFilter(data, length, bloom_filter);
}

void NgramsTokenizer::substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool, bool) const
{
    stringToTokens(data, length, tokens);
}

bool SplitByNonAlphaTokenizer::nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
{
    token_start = pos;
    token_length = 0;

    while (pos < length)
    {
        if (isASCII(data[pos]) && !isAlphaNumericASCII(data[pos]))
        {
            /// Finish current token if any
            if (token_length > 0)
                return true;
            token_start = ++pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++pos;
            ++token_length;
        }
    }

    return token_length > 0;
}

bool SplitByNonAlphaTokenizer::nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const
{
    token.clear();
    bool bad_token = false; // % or _ before token
    bool escaped = false;
    while (pos < length)
    {
        if (!escaped && (data[pos] == '%' || data[pos] == '_'))
        {
            /// Unescaped wildcard: invalidates current token
            token.clear();
            bad_token = true;
            ++pos;
        }
        else if (!escaped && data[pos] == '\\')
        {
            escaped = true;
            ++pos;
        }
        else if (isASCII(data[pos]) && !isAlphaNumericASCII(data[pos]))
        {
            /// Non-alphanumeric ASCII is a separator (including escaped \_ and \%)
            if (!bad_token && !token.empty())
            {
                ++pos; /// consume separator before returning
                return true;
            }

            token.clear();
            bad_token = false;
            escaped = false;
            ++pos;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(data[pos]));
            /// The sequence length is determined by the first character, but we should always check it does not go beyond the buffer length.
            for (size_t j = 0; j < sz && pos < length; ++j)
            {
                token += data[pos];
                ++pos;
            }
            escaped = false;
        }
    }

    return !bad_token && !token.empty();
}

namespace
{

/// Shared implementation of `substringToBloomFilter` for word-boundary tokenizers
/// (`SplitByNonAlphaTokenizer`, `UnicodeWordTokenizer`).
///
/// In order to avoid filter updates with incomplete tokens, the first token is
/// ignored unless the substring is a prefix, and the last token is ignored unless
/// the substring is a suffix.
/// Ex: If we want to match row "Service is not ready", and substring is "Serv"
/// or "eady", we don't want to add either of these substrings as tokens since
/// they will not match any of the real tokens. However if our token string is
/// "Service " or " not ", we want to add these full tokens to our bloom filter.
template <typename Tokenizer>
void wordBoundarySubstringToBloomFilter(
    const Tokenizer & tokenizer, const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && tokenizer.nextInString(data, length, cur, token_start, token_len))
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            bloom_filter.add(data + token_start, token_len);
}

/// Shared implementation of `substringToTokens` for word-boundary tokenizers.
/// Same boundary-filtering logic as `wordBoundarySubstringToBloomFilter`.
template <typename Tokenizer>
void wordBoundarySubstringToTokens(
    const Tokenizer & tokenizer, const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && tokenizer.nextInString(data, length, cur, token_start, token_len))
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            tokens.push_back({data + token_start, token_len});
}

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

void SplitByNonAlphaTokenizer::substringToBloomFilter(
    const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
{
    wordBoundarySubstringToBloomFilter(*this, data, length, bloom_filter, is_prefix, is_suffix);
}

void SplitByNonAlphaTokenizer::substringToTokens(
    const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const
{
    wordBoundarySubstringToTokens(*this, data, length, tokens, is_prefix, is_suffix);
}

bool SplitByStringTokenizer::nextInString(const char * data, size_t length, size_t & pos, size_t & token_start, size_t & token_length) const
{
    size_t i = pos;
    std::string matched_separators;

    /// Skip prefix of separators
    while (i < length && startsWithSeparator(data, length, i, separators, matched_separators))
        i += matched_separators.size();

    if (i >= length)
    {
        pos = length;
        return false;
    }

    /// Read token until next separator
    size_t start = i;
    while (i < length && !startsWithSeparator(data, length, i, separators, matched_separators))
        ++i;

    token_start = start;
    token_length = i - start;
    pos = i;

    return true;
}

bool SplitByStringTokenizer::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t & /*pos*/, String & /*token*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StringTokenExtractor::nextInStringLike is not implemented");
}

void SplitByStringTokenizer::substringToBloomFilter(const char *, size_t, BloomFilter &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SplitByStringTokenizer::substringToBloomFilter is not implemented");
}

void SplitByStringTokenizer::substringToTokens(const char *, size_t, std::vector<String> &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SplitByStringTokenizer::substringToTokens is not implemented");
}

String SplitByStringTokenizer::getDescription() const
{
    String result = fmt::format("{}([", getName());
    for (size_t i = 0; i < separators.size(); ++i)
    {
        if (i != 0)
            result += ", ";

        result += quoteString(separators[i]);
    }

    return result + "])";
}

bool ArrayTokenizer::nextInString(const char * /*data*/, size_t length, size_t & pos, size_t & token_start, size_t & token_length) const
{
    if (pos == 0)
    {
        pos = length;
        token_start = 0;
        token_length = length;
        return true;
    }
    return false;
}

bool ArrayTokenizer::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t & /*pos*/, String & /*token*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayTokenizer::nextInStringLike is not implemented");
}

void ArrayTokenizer::substringToBloomFilter(const char *, size_t, BloomFilter &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayTokenizer::substringToBloomFilter is not implemented");
}

void ArrayTokenizer::substringToTokens(const char *, size_t, std::vector<String> &, bool, bool) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayTokenizer::substringToTokens is not implemented");
}

SparseGramsTokenizer::SparseGramsTokenizer(size_t min_length, size_t max_length, std::optional<size_t> min_cutoff_length_)
    : ITokenizerHelper(Type::SparseGrams)
    , min_gram_length(min_length)
    , max_gram_length(max_length)
    , min_cutoff_length(min_cutoff_length_)
    , sparse_grams_iterator(min_length, max_length, min_cutoff_length_)
{
}

bool SparseGramsTokenizer::nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
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
    token_start = next_begin - data;
    token_length = next_end - next_begin;
    pos = token_start;

    return true;
}

bool SparseGramsTokenizer::nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const
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
                /// The sequence length is determined by the first character, but we should always check it does not go beyond the buffer length.
                for (size_t j = 0; j < sz && i + j < length; ++j)
                    token.push_back(data[i + j]);
                i += sz;
            }
        }
        if (match_substring)
        {
            pos = next_begin - data;
            return true;
        }
    }
}

void SparseGramsTokenizer::substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool /*is_prefix*/, bool /*is_suffix*/) const
{
    stringToBloomFilter(data, length, bloom_filter);
}

void SparseGramsTokenizer::substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool /*is_prefix*/, bool /*is_suffix*/) const
{
    stringToTokens(data, length, tokens);
}

std::vector<String> SparseGramsTokenizer::compactTokens(const std::vector<String> & tokens) const
{
    std::unordered_set<String> result;
    auto sorted_tokens = tokens;

    /// Bug in clang-tidy: https://github.com/llvm/llvm-project/issues/78132
    std::ranges::sort(sorted_tokens, [](const auto & lhs, const auto & rhs) { return lhs.size() > rhs.size(); }); /// NOLINT(clang-analyzer-cplusplus.Move)

    /// Filter out sparse grams that are covered by longer ones,
    /// because if index has longer sparse gram, it has all shorter covered ones.
    /// Using dumb O(n^2) algorithm to avoid unnecessary complexity, because this method
    /// is used only for transforming constant searched tokens, which amount is usually small.
    for (const auto & token : sorted_tokens)
    {
        bool is_covered = false;

        for (const auto & existing_token : result)
        {
            if (existing_token.find(token) != std::string::npos)
            {
                is_covered = true;
                break;
            }
        }

        if (!is_covered)
            result.insert(token);
    }

    return std::vector<String>(result.begin(), result.end());
}

String SparseGramsTokenizer::getDescription() const
{
    String result = fmt::format("{}({}, {}", getName(), min_gram_length, max_gram_length);
    if (min_cutoff_length.has_value())
        result += fmt::format(", {}", *min_cutoff_length);
    return result + ")";
}

void forEachTokenToBloomFilter(const ITokenizer & tokenizer, const char * data, size_t length, BloomFilter & bloom_filter)
{
    forEachToken(
        tokenizer,
        data,
        length,
        [&](const char * token_start, size_t token_length)
        {
            bloom_filter.add(token_start, token_length);
            return false;
        });
}

bool UnicodeWordTokenizer::nextInString(
    const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
{
    token_length = 0;
    while (pos < length)
    {
        token_start = pos;
        char c = data[pos];

        /// 1. ASCII fast path

        if (isAlphaNumericASCII(c) || c == '_')
        {
            size_t token_alnum_count = 0;
            token_length = 0;

            while (pos < length)
            {
                char cur = data[pos];

                /// 1a. ASCII letter or digit
                if (isAlphaNumericASCII(cur))
                {
                    ++pos;
                    ++token_length;
                    ++token_alnum_count;
                    continue;
                }

                /// 1b. Underscore
                if (cur == '_')
                {
                    ++pos;
                    ++token_length;
                    continue;
                }

                /// Check if next character exists
                if (pos + 1 >= length)
                    break;

                char next_c = data[pos + 1];
                char prev_c = pos > 0 ? data[pos - 1] : '\0';

                /// 1c. Colon: connects letters only
                if (cur == ':' && isAlphaASCII(prev_c) && isAlphaASCII(next_c))
                {
                    ++pos;
                    ++token_length;
                    continue;
                }

                /// 1d. Dot or single quote: connects letter-letter or digit-digit
                if ((cur == '.' || cur == '\'') &&
                    ((isAlphaASCII(prev_c) && isAlphaASCII(next_c)) ||
                     (isNumericASCII(prev_c) && isNumericASCII(next_c))))
                {
                    ++pos;
                    ++token_length;
                    continue;
                }

                /// Token ends
                break;
            }

            /// Token must contain at least one alphanumeric character
            if (token_alnum_count > 0)
            {
                return true;
            }

            /// Invalid token (underscore only), continue searching
            token_length = 0;
            continue;
        }

        /// 2. ASCII non-alphanumeric: skip

        if (isASCII(c))
        {
            ++pos;
            continue;
        }

        /// 3. Unicode character handling

        size_t char_len = UTF8::seqLength(static_cast<UInt8>(c));

        /// Truncated UTF-8 sequence at end of buffer
        if (pos + char_len > length)
        {
            pos = length;
            return false;
        }

        token_start = pos;
        token_length = char_len;
        pos += char_len;

        return true;
    }

    return false;
}

bool UnicodeWordTokenizer::nextInStringLike(const char * data, size_t length, size_t & __restrict pos, String & token) const
{
    token.clear();
    size_t token_start;
    std::optional<size_t> last_glob_pos;
    bool escaped = false; /// Whether current char is an escaped char
    while (pos < length)
    {
        token_start = pos;
        char c = data[pos];

        if (c == '\\' && !escaped)
        {
            if (pos + 1 >= length)
                break;
            ++pos;
            c = data[pos];
            escaped = true;
        }

        /// ASCII alphanumeric or escaped underscore
        if (isAlphaNumericASCII(c) || (c == '_' && escaped))
        {
            size_t token_alnum_count = 0;
            token.clear();

            while (pos < length)
            {
                char cur = data[pos];
                if (cur == '\\' && !escaped)
                {
                    if (pos + 1 >= length)
                        break;
                    ++pos;
                    cur = data[pos];
                    escaped = true;
                }

                /// 1a. ASCII letter or digit
                if (isAlphaNumericASCII(cur))
                {
                    ++pos;
                    escaped = false;
                    token.push_back(cur);
                    ++token_alnum_count;
                    continue;
                }

                /// 1b. Underscore
                if (cur == '_' && escaped)
                {
                    ++pos;
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// Wildcard
                if (cur == '_' || (cur == '%' && !escaped))
                {
                    last_glob_pos = pos;
                    ++pos;
                    break;
                }

                /// Check if next character exists
                if (pos + 1 >= length)
                    break;

                char next_c = data[pos + 1];
                if (next_c == '\\')
                {
                    if (pos + 2 >= length)
                        break;
                    next_c = data[pos + 2];
                }
                char prev_c = token.empty() ? '\0' : token.back();

                /// 1c. Colon: connects letters only
                if (cur == ':' && isAlphaASCII(prev_c) && isAlphaASCII(next_c))
                {
                    ++pos;
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// 1d. Dot or single quote: connects letter-letter or digit-digit
                if ((cur == '.' || cur == '\'') &&
                    ((isAlphaASCII(prev_c) && isAlphaASCII(next_c)) ||
                     (isNumericASCII(prev_c) && isNumericASCII(next_c))))
                {
                    ++pos;
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// Token ends
                break;
            }

            /// Token must contain at least one alphanumeric character. Tokens adjacent to unescaped wildcards are
            /// discarded, since their boundaries are ambiguous.
            if (token_alnum_count > 0 && (!last_glob_pos || (*last_glob_pos + 1 != pos && *last_glob_pos + 1 != token_start)))
            {
                /// If we consumed a backslash but the escaped char didn't continue the token, back up so the next call
                /// re-parses `\X` with proper escape context.
                if (escaped)
                    --pos;

                return true;
            }

            /// Invalid token, continue searching
            continue;
        }

        /// Skip ASCII non-alphanumeric
        if (isASCII(c))
        {
            if (!escaped && (c == '_' || c == '%'))
                last_glob_pos = pos;

            ++pos;
            escaped = false;
            continue;
        }

        /// Unicode character
        size_t char_len = UTF8::seqLength(static_cast<UInt8>(c));

        /// Truncated UTF-8 sequence at end of buffer
        if (pos + char_len > length)
        {
            pos = length;
            escaped = false;
            continue;
        }

        token = {data + pos, char_len};

        pos += char_len;
        return true;
    }

    return false;
}

void UnicodeWordTokenizer::substringToBloomFilter(
    const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
{
    wordBoundarySubstringToBloomFilter(*this, data, length, bloom_filter, is_prefix, is_suffix);
}

void UnicodeWordTokenizer::substringToTokens(
    const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const
{
    wordBoundarySubstringToTokens(*this, data, length, tokens, is_prefix, is_suffix);
}

}
