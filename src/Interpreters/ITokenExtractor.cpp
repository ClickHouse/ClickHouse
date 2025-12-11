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
    extern const int INCORRECT_QUERY;
    extern const int NOT_IMPLEMENTED;
}

static constexpr UInt64 MIN_NGRAM_SIZE = 1;
static constexpr UInt64 MAX_NGRAM_SIZE = 8;
static constexpr UInt64 DEFAULT_NGRAM_SIZE = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MIN_LENGTH = 3;
static constexpr UInt64 DEFAULT_SPARSE_GRAMS_MAX_LENGTH = 100;

namespace
{

void assertParamsCount(size_t params_count, size_t max_count, std::string_view tokenizer)
{
    if (params_count > max_count)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
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
            ErrorCodes::INCORRECT_QUERY,
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
    if (ngram_size < 1 || ngram_size > 8)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect param of tokenizer '{}': ngram length must be between {} and {}, but got {}",
            NgramsTokenExtractor::getExternalName(),
            MIN_NGRAM_SIZE,
            MAX_NGRAM_SIZE,
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
            ErrorCodes::INCORRECT_QUERY,
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

    if (min_length < 3)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unexpected parameter of tokenizer '{}': minimal length must be at least {}, but got {}",
            tokenizer_name,
            DEFAULT_SPARSE_GRAMS_MIN_LENGTH,
            min_length);
    }
    if (max_length > 100)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unexpected parameter of tokenizer '{}': maximal length must be at most {}, but got {}",
            tokenizer_name,
            DEFAULT_SPARSE_GRAMS_MAX_LENGTH,
            max_length);
    }
    if (min_length > max_length)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unexpected parameter of tokenizer '{}': minimal length {} cannot be larger than maximal length {}",
            tokenizer_name,
            min_length,
            max_length);
    }
    if (min_cutoff_length.has_value() && min_cutoff_length.value() < min_length)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be smaller than minimal length {}",
            tokenizer_name,
            min_cutoff_length.value(),
            min_length);
    }
    if (min_cutoff_length.has_value() && min_cutoff_length.value() > max_length)
    {
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "Unexpected parameter of tokenizer '{}': minimal cutoff length {} cannot be larger than maximal length {}",
            tokenizer_name,
            min_cutoff_length.value(),
            max_length);
    }

    return {min_length, max_length, min_cutoff_length};
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

bool SplitByNonAlphaTokenExtractor::nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
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

}
