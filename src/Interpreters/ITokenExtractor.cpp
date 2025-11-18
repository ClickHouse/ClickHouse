#include <IO/Operators.h>
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

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

static constexpr UInt64 MIN_NGRAM_SIZE = 1;
static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MIN_LENGTH = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MAX_LENGTH = 100;

/// Unlike for sparse ngrams, there is no upper length for ngrams.
/// Such a limit historically existed for the text index and SQL function hasToken() but not for the bloom filter skip index.
/// Since the lower/upper limit checks in this file are central to all of these, we need to go with the lowest common denominator for backward compatibility.

namespace
{

void assertParamsCount(size_t params_count, size_t max_count, std::string_view tokenizer)
{
    if (params_count > max_count)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "'{}' tokenizer accepts at most {} parameters, but got {}",
            tokenizer, max_count, params_count);
    }
}

template <typename Type>
std::optional<Type> tryCastAs(const Field & field)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    return expected_type == field.getType() ? std::make_optional(field.safeGet<Type>()) : std::nullopt;
}

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto result = tryCastAs<Type>(field);

    if (!result.has_value())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Tokenizer argument '{}' expected to be of type {}, but got type: {}",
            argument_name, fieldTypeToString(Field::TypeToEnum<Type>::value), field.getTypeName());
    }

    return result.value();
}

}

UInt64 TokenizerFactory::extractNgramParam(std::span<const Field> params)
{
    assertParamsCount(params.size(), 1, NgramsTokenExtractor::getExternalName());

    auto ngram_size = params.empty() ? DEFAULT_NGRAM_SIZE : castAs<UInt64>(params[0], "ngram_size");
    if (ngram_size < MIN_NGRAM_SIZE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect param of tokenizer '{}': ngram length must be at least {}, but got {}",
            NgramsTokenExtractor::getExternalName(),
            MIN_NGRAM_SIZE,
            ngram_size);

    return ngram_size;
}

std::vector<String> TokenizerFactory::extractSplitByStringParam(std::span<const Field> params)
{
    assertParamsCount(params.size(), 1, SplitByStringTokenExtractor::getExternalName());
    if (params.empty())
        return std::vector<String>{" "};

    std::vector<String> values;
    auto array = castAs<Array>(params[0], "separators");

    for (const auto & value : array)
        values.emplace_back(castAs<String>(value, "separator"));

    if (values.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect params of tokenizer '{}': separators cannot be empty",
            SplitByStringTokenExtractor::getExternalName());
    }

    return values;
}

std::tuple<UInt64, UInt64, std::optional<UInt64>> TokenizerFactory::extractSparseGramsParams(std::span<const Field> params)
{
    const auto * tokenizer_name = SparseGramsTokenExtractor::getExternalName();
    assertParamsCount(params.size(), 3, tokenizer_name);

    UInt64 min_length = DEFAULT_SPARSE_GRAMS_MIN_LENGTH;
    UInt64 max_length = DEFAULT_SPARSE_GRAMS_MAX_LENGTH;
    std::optional<UInt64> min_cutoff_length;

    if (!params.empty())
        min_length = castAs<UInt64>(params[0], "min_length");

    if (params.size() > 1)
        max_length = castAs<UInt64>(params[1], "max_length");

    if (params.size() > 2)
        min_cutoff_length = castAs<UInt64>(params[2], "min_cutoff_length");

    if (min_length < DEFAULT_SPARSE_GRAMS_MIN_LENGTH)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected parameter of tokenizer '{}': minimal length must be at least {}, but got {}",
            tokenizer_name,
            DEFAULT_SPARSE_GRAMS_MIN_LENGTH,
            min_length);
    }
    if (max_length > DEFAULT_SPARSE_GRAMS_MAX_LENGTH)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected parameter of tokenizer '{}': maximal length must be at most {}, but got {}",
            tokenizer_name,
            DEFAULT_SPARSE_GRAMS_MAX_LENGTH,
            max_length);
    }
    if (min_length > max_length)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected parameter of tokenizer '{}': minimal length {} cannot be larger than maximal length {}",
            tokenizer_name,
            min_length,
            max_length);
    }
    if (min_cutoff_length.has_value() && min_cutoff_length.value() < min_length)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be smaller than minimal length {}",
            tokenizer_name,
            min_cutoff_length.value(),
            min_length);
    }
    if (min_cutoff_length.has_value() && min_cutoff_length.value() > max_length)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be larger than maximal length {}",
            tokenizer_name,
            min_cutoff_length.value(),
            max_length);
    }

    return {min_length, max_length, min_cutoff_length};
}

std::vector<String> TokenizerFactory::extractStandardParam(std::span<const Field> params)
{
    assertParamsCount(params.size(), 1, StandardTokenExtractor::getExternalName());
    if (params.empty())
        return std::vector<String>{"，", "。", "！", "？", "；", "：", "、", "“", "”", "‘", "’"};

    std::vector<String> values;
    auto array = castAs<Array>(params[0], "stop_words");

    for (const auto & value : array)
        values.emplace_back(castAs<String>(value, "stop_word"));

    return values;
}

void TokenizerFactory::isAllowedTokenizer(std::string_view tokenizer, const std::vector<String> & allowed_tokenizers, std::string_view caller_name)
{
    if (std::ranges::find(allowed_tokenizers, tokenizer) == allowed_tokenizers.end())
    {
        WriteBufferFromOwnString buf;
        for (size_t i = 0; i < allowed_tokenizers.size(); ++i)
        {
            if (i < allowed_tokenizers.size() - 1)
                buf << "'" << allowed_tokenizers[0] << "', "; /// asserted not empty in constructor
            else
                buf << "and '" << allowed_tokenizers[i] << "'";
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function or index '{}' supports only tokenizers {}, received '{}'", caller_name, buf.str(), tokenizer);
    }
}

std::unique_ptr<ITokenExtractor> TokenizerFactory::createTokenizer(
        std::string_view tokenizer,
        std::span<const Field> params,
        const std::vector<String> & allowed_tokenizers,
        std::string_view caller_name,
        bool only_validate)
{
    isAllowedTokenizer(tokenizer, allowed_tokenizers, caller_name);

    if (tokenizer == NgramsTokenExtractor::getName() || tokenizer == NgramsTokenExtractor::getExternalName())
    {
        auto ngram_size = extractNgramParam(params);
        return only_validate ? nullptr : std::make_unique<NgramsTokenExtractor>(ngram_size);
    }
    if (tokenizer == SplitByNonAlphaTokenExtractor::getName() || tokenizer == SplitByNonAlphaTokenExtractor::getExternalName())
    {
        assertParamsCount(params.size(), 0, tokenizer);
        return only_validate ? nullptr : std::make_unique<SplitByNonAlphaTokenExtractor>();
    }
    if (tokenizer == SplitByStringTokenExtractor::getName() || tokenizer == SplitByStringTokenExtractor::getExternalName())
    {
        auto separators = extractSplitByStringParam(params);
        return only_validate ? nullptr : std::make_unique<SplitByStringTokenExtractor>(separators);
    }
    if (tokenizer == SparseGramsTokenExtractor::getName() || tokenizer == SparseGramsTokenExtractor::getExternalName()
        || tokenizer == SparseGramsTokenExtractor::getBloomFilterIndexName())
    {
        auto [min_ngram_length, max_ngram_length, min_cutoff_length] = extractSparseGramsParams(params);
        return only_validate ? nullptr : std::make_unique<SparseGramsTokenExtractor>(min_ngram_length, max_ngram_length, min_cutoff_length);
    }
    if (tokenizer == ArrayTokenExtractor::getName() || tokenizer == ArrayTokenExtractor::getExternalName())
    {
        assertParamsCount(params.size(), 0, tokenizer);
        return only_validate ? nullptr : std::make_unique<ArrayTokenExtractor>();
    }
    if (tokenizer == StandardTokenExtractor::getName() || tokenizer == StandardTokenExtractor::getExternalName())
    {
        auto stop_words = extractStandardParam(params);
        return only_validate ? nullptr : std::make_unique<StandardTokenExtractor>(stop_words);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown tokenizer: '{}' for function or index '{}'", tokenizer, caller_name);
}

bool NgramsTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool NgramsTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
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

bool SplitByNonAlphaTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool SplitByNonAlphaTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
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

void SplitByNonAlphaTokenExtractor::substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
    {
        /// In order to avoid filter updates with incomplete tokens, first token is ignored unless substring is prefix,
        /// and last token is ignored, unless substring is suffix. See comment below for example
        if ((token_start > 0 || is_prefix) && (token_start + token_len < length || is_suffix))
            bloom_filter.add(data + token_start, token_len);
    }
}

void SplitByNonAlphaTokenExtractor::substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && nextInString(data, length, &cur, &token_start, &token_len))
    {
        /// In order to avoid adding incomplete tokens, first token is ignored unless substring is prefix and last token is ignored, unless substring is suffix.
        /// Ex: If we want to match row "Service is not ready", and substring is "Serv" or "eady", we don't want to add either
        /// of these substrings as tokens since they will not match any of the real tokens. However if our token string is
        /// "Service " or " not ", we want to add these full tokens to our tokens vector.
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

bool SplitByStringTokenExtractor::nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const
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

bool SplitByStringTokenExtractor::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t * /*token_start*/, String & /*token_length*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StringTokenExtractor::nextInStringLike is not implemented");
}

bool ArrayTokenExtractor::nextInString(const char * /*data*/, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const
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

bool ArrayTokenExtractor::nextInStringLike(const char * /*data*/, size_t /*length*/, size_t * /*token_start*/, String & /*token_length*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ArrayTokenExtractor::nextInStringLike is not implemented");
}

SparseGramsTokenExtractor::SparseGramsTokenExtractor(size_t min_length, size_t max_length, std::optional<size_t> min_cutoff_length_)
    : ITokenExtractorHelper(Type::SparseGrams)
    , sparse_grams_iterator(min_length, max_length, min_cutoff_length_)
{
}

bool SparseGramsTokenExtractor::nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

bool SparseGramsTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const
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

bool StandardTokenExtractor::nextInString(
    const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
{
    *token_length = 0;
    while (*pos < length)
    {
        *token_start = *pos;
        char c = data[*pos];

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
                    continue;
                }

                /// 1b. Underscore
                if (cur == '_')
                {
                    ++(*pos);
                    ++(*token_length);
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
                    continue;
                }

                /// 1d. Dot or single quote: connects letter-letter or digit-digit
                if ((cur == '.' || cur == '\'') &&
                    ((isAlphaASCII(prev_c) && isAlphaASCII(next_c)) ||
                     (isNumericASCII(prev_c) && isNumericASCII(next_c))))
                {
                    ++(*pos);
                    ++(*token_length);
                    continue;
                }

                /// Token ends
                break;
            }

            /// Token must contain at least one alphanumeric character
            if (token_alnum_count > 0)
            {
                /// Check if token is a stop word
                std::string_view token_view(data + *token_start, *token_length);
                if (stop_words.contains(token_view))
                {
                    *token_length = 0;
                    continue;
                }
                return true;
            }

            /// Invalid token (underscore only), continue searching
            *token_length = 0;
            continue;
        }

        /// =======================
        /// 2. ASCII non-alphanumeric: skip
        /// =======================
        if (isASCII(c))
        {
            ++(*pos);
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
            continue;
        }

        *token_start = *pos;
        *token_length = char_len;
        *pos += char_len;

        return true;
    }

    return false;
}

bool StandardTokenExtractor::nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const
{
    token.clear();
    size_t token_start;
    std::optional<size_t> last_glob_pos;
    bool escaped = false; /// Whether current char is an escaped char
    while (*pos < length)
    {
        token_start = *pos;
        char c = data[*pos];

        if (c == '\\' && !escaped)
        {
            if (*pos + 1 >= length)
                break;
            ++(*pos);
            c = data[*pos];
            escaped = true;
        }

        /// =======================
        /// 1. ASCII fast path
        /// =======================
        if (isAlphaNumericASCII(c) || (c == '_' && escaped))
        {
            size_t token_alnum_count = 0;
            token.clear();

            while (*pos < length)
            {
                char cur = data[*pos];
                if (cur == '\\' && !escaped)
                {
                    if (*pos + 1 >= length)
                        break;
                    ++(*pos);
                    cur = data[*pos];
                    escaped = true;
                }

                /// 1a. ASCII letter or digit
                if (isAlphaNumericASCII(cur))
                {
                    ++(*pos);
                    escaped = false;
                    token.push_back(cur);
                    ++token_alnum_count;
                    continue;
                }

                /// 1b. Underscore
                if (cur == '_' && escaped)
                {
                    ++(*pos);
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// Wildcard
                if (cur == '_' || (cur == '%' && !escaped))
                {
                    last_glob_pos = *pos;
                    ++(*pos);
                    break;
                }

                /// Check if next character exists
                if (*pos + 1 >= length)
                    break;

                char next_c = data[*pos + 1];
                if (next_c == '\\')
                {
                    if (*pos + 2 >= length)
                        break;
                    next_c = data[*pos + 2];
                }
                char prev_c = token.empty() ? '\0' : token.back();

                /// 1c. Colon: connects letters only
                if (cur == ':' && isAlphaASCII(prev_c) && isAlphaASCII(next_c))
                {
                    ++(*pos);
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// 1d. Dot or single quote: connects letter-letter or digit-digit
                if ((cur == '.' || cur == '\'') &&
                    ((isAlphaASCII(prev_c) && isAlphaASCII(next_c)) ||
                     (isNumericASCII(prev_c) && isNumericASCII(next_c))))
                {
                    ++(*pos);
                    escaped = false;
                    token.push_back(cur);
                    continue;
                }

                /// Token ends
                break;
            }

            /// Token must contain at least one alphanumeric character
            if (token_alnum_count > 0 && (!last_glob_pos || (*last_glob_pos + 1 != *pos && *last_glob_pos + 1 != token_start)))
            {
                /// Check if token is a stop word
                if (stop_words.contains(token))
                    continue;

                if (escaped)
                    --(*pos);
                return true;
            }

            /// Invalid token, continue searching
            continue;
        }

        /// =======================
        /// 2. ASCII non-alphanumeric: skip
        /// =======================
        if (isASCII(c))
        {
            if (!escaped && (c == '_' || c == '%'))
                last_glob_pos = *pos;

            ++(*pos);
            escaped = false;
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
            escaped = false;
            continue;
        }

        token = {data + *pos, char_len};

        /// 3a. Stop words (Chinese punctuation, etc.): skip
        if (stop_words.contains(token))
        {
            *pos += char_len;
            escaped = false;
            continue;
        }

        *pos += char_len;
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
