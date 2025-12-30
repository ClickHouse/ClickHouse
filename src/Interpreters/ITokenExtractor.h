#pragma once

#include <base/FnTraits.h>
#include <base/types.h>
#include <Interpreters/BloomFilter.h>
#include <Common/assert_cast.h>

#include <Functions/sparseGrams.h>

namespace DB
{

/// Interface for string parsers.
struct ITokenExtractor
{
public:
    enum class Type
    {
        SplitByNonAlpha,
        Ngrams,
        SplitByString,
        Array,
        SparseGrams,
    };

    ITokenExtractor() = default;
    explicit ITokenExtractor(Type type_) : type(type_) {}
    ITokenExtractor(const ITokenExtractor &) = default;
    ITokenExtractor & operator=(const ITokenExtractor &) = default;

    Type getType() const { return type; }

    virtual ~ITokenExtractor() = default;
    virtual std::unique_ptr<ITokenExtractor> clone() const = 0;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of extractor).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const = 0;

    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextInStringLike(const char * data, size_t length, size_t * pos, String & out) const = 0;

    /// Updates Bloom filter from exact-match string filter value
    virtual void stringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    /// Updates Bloom filter from substring-match string filter value.
    /// An `ITokenExtractor` implementation may decide to skip certain
    /// tokens depending on whether the substring is a prefix or a suffix.
    virtual void substringToBloomFilter(
        const char * data,
        size_t length,
        BloomFilter & bloom_filter,
        bool /*is_prefix*/,
        bool /*is_suffix*/) const
    {
        stringToBloomFilter(data, length, bloom_filter);
    }

    virtual void stringLikeToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    /// Collects copy of tokens into vector. This method is inefficient and should be used only for constants.
    virtual void stringToTokens(const char * data, size_t length, std::vector<String> & tokens) const = 0;

    /// Collects copy of tokens into vector from substring-match string filter value.
    /// An `ITokenExtractor` implementation may decide to skip certain
    /// tokens depending on whether the substring is a prefix or a suffix.
    /// This method is inefficient and should be used only for constants.
    virtual void substringToTokens(
        const char * data,
        size_t length,
        std::vector<String> & tokens,
        bool /*is_prefix*/,
        bool /*is_suffix*/) const
    {
        stringToTokens(data, length, tokens);
    }

    virtual void stringLikeToTokens(const char * data, size_t length, std::vector<String> & tokens) const = 0;
    virtual bool supportsStringLike() const = 0;

private:
    Type type;
};

using TokenExtractorPtr = const ITokenExtractor *;

template <typename Derived>
class ITokenExtractorHelper : public ITokenExtractor
{
protected:
    explicit ITokenExtractorHelper(Type type_) : ITokenExtractor(type_) {}

private:
    std::unique_ptr<ITokenExtractor> clone() const override
    {
        return std::make_unique<Derived>(*static_cast<const Derived *>(this));
    }

    void stringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
            bloom_filter.add(data + token_start, token_len);
    }

    void stringLikeToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        String token;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringLike(data, length, &cur, token))
            bloom_filter.add(token.c_str(), token.size());
    }

    void stringToTokens(const char * data, size_t length, std::vector<String> & tokens) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
            tokens.push_back({data + token_start, token_len});
    }

    void stringLikeToTokens(const char * data, size_t length, std::vector<String> & tokens) const override
    {
        size_t cur = 0;
        String token;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringLike(data, length, &cur, token))
            tokens.push_back(token);
    }
};

class TokenizerFactory : public boost::noncopyable
{
public:
    static void isAllowedTokenizer(std::string_view tokenizer, const std::vector<String> & allowed_tokenizers, std::string_view caller_name);

    static std::unique_ptr<ITokenExtractor> createTokenizer(
            std::string_view tokenizer, /// internal or external tokenizer name
            std::span<const Field> params,
            const std::vector<String> & allowed_tokenizers,
            std::string_view caller_name,
            bool only_validate = false);

private:
    static UInt64 extractNgramParam(std::span<const Field> params);
    static std::vector<String> extractSplitByStringParam(std::span<const Field> params);
    static std::tuple<UInt64, UInt64, std::optional<UInt64>> extractSparseGramsParams(std::span<const Field> params);
};

/// Parser extracting all ngrams from string.
struct NgramsTokenExtractor final : public ITokenExtractorHelper<NgramsTokenExtractor>
{
    explicit NgramsTokenExtractor(size_t n_) : ITokenExtractorHelper(Type::Ngrams), n(n_) {}

    static const char * getName() { return "ngrambf_v1"; }
    static const char * getExternalName() { return "ngrams"; }

    bool nextInString(const char * data, size_t length, size_t *  __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    size_t getN() const { return n; }

    bool supportsStringLike() const override { return true; }
private:
    size_t n;
};

/// Parser extracting tokens which consist of alphanumeric ASCII characters or Unicode characters (not necessarily alphanumeric)
struct SplitByNonAlphaTokenExtractor final : public ITokenExtractorHelper<SplitByNonAlphaTokenExtractor>
{
    SplitByNonAlphaTokenExtractor() : ITokenExtractorHelper(Type::SplitByNonAlpha) {}

    static const char * getName() { return "tokenbf_v1"; }
    static const char * getExternalName() { return "splitByNonAlpha"; }

    template <Fn<bool(const char *, size_t)> Callback>
    void forEachTokenImpl(const char * __restrict data, size_t length, Callback && callback) const;
    bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const override;
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

    bool supportsStringLike() const override { return true; }
};

/// Parser extracting tokens which are separated by certain strings.
/// Allows to emulate e.g. BigQuery's LOG_ANALYZER.
struct SplitByStringTokenExtractor final : public ITokenExtractorHelper<SplitByStringTokenExtractor>
{
    explicit SplitByStringTokenExtractor(const std::vector<String> & separators_) : ITokenExtractorHelper(Type::SplitByString), separators(separators_) {}

    static const char * getName() { return "splitByString"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
private:
    std::vector<String> separators;
};

/// Parser doing "no operation". Returns the entire input as a single token.
struct ArrayTokenExtractor final : public ITokenExtractorHelper<ArrayTokenExtractor>
{
    ArrayTokenExtractor() : ITokenExtractorHelper(Type::Array) {}

    static const char * getName() { return "array"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
};

/// Parser extracting sparse grams (the same as function sparseGrams).
/// See sparseGrams.h for more details.
struct SparseGramsTokenExtractor final : public ITokenExtractorHelper<SparseGramsTokenExtractor>
{
    explicit SparseGramsTokenExtractor(size_t min_length = 3, size_t max_length = 100, std::optional<size_t> min_cutoff_length_ = std::nullopt);

    static const char * getBloomFilterIndexName() { return "sparse_grams"; }
    static const char * getName() { return "sparseGrams"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t *  __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;
    bool supportsStringLike() const override { return true; }

private:
    mutable SparseGramsImpl<true> sparse_grams_iterator;
    mutable const char * previous_data = nullptr;
    mutable size_t previous_len = 0;
};

template <Fn<bool(const char *, size_t)> Callback>
void SplitByNonAlphaTokenExtractor::forEachTokenImpl(const char * __restrict data, size_t length, Callback && callback) const
{
    const char * begin = data;
    const char * end = data + length;
    const char * pos = begin;

    while (pos < end)
    {
#if defined(__SSE2__) && !defined(MEMORY_SANITIZER) /// We read uninitialized bytes and decide on the calculated mask
        // NOTE: we assume that `data` string is padded from the right with 15 bytes.
        const __m128i haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pos));
        const size_t haystack_length = 16;

#if defined(__SSE4_2__)
        // With the help of https://www.strchr.com/strcmp_and_strlen_using_sse_4.2
        const auto alnum_chars_ranges = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0,
                '\xFF', '\x80', 'z', 'a', 'Z', 'A', '9', '0');
        // Every bit represents if `haystack` character is in the ranges (1) or not (0)
        unsigned result_bitmask = _mm_cvtsi128_si32(_mm_cmpestrm(alnum_chars_ranges, 8, haystack, haystack_length, _SIDD_CMP_RANGES));
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
        unsigned result_bitmask = _mm_movemask_epi8(_mm_or_si128(_mm_or_si128(_mm_or_si128(
                _mm_cmplt_epi8(haystack, zero),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, number_begin),      _mm_cmplt_epi8(haystack, number_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_lower_begin), _mm_cmplt_epi8(haystack, alpha_lower_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_upper_begin), _mm_cmplt_epi8(haystack, alpha_upper_end))));
#endif
        const char * next_pos = std::min(end, pos + haystack_length);
        while (pos < next_pos)
        {
            if (result_bitmask == 0)
            {
                if (pos > begin)
                {
                    if (callback(begin, pos - begin))
                        return;
                }

                pos = next_pos;
                begin = pos;
                break;
            }

            const auto token_start_pos_in_current_haystack = std::countr_zero(result_bitmask);
            if (token_start_pos_in_current_haystack != 0)
            {
                if (begin < pos)
                {
                    if (callback(begin, pos - begin))
                        return;
                }
                pos += token_start_pos_in_current_haystack;
                begin = pos;
            }

            const auto token_bytes_in_current_haystack = std::countr_zero(~(result_bitmask >> token_start_pos_in_current_haystack));
            pos += token_bytes_in_current_haystack;

            result_bitmask >>= token_start_pos_in_current_haystack + token_bytes_in_current_haystack;
        }
#else
        if (isASCII(*pos) && !isAlphaNumericASCII(*pos))
        {
            /// Finish current token if any
            if (pos > begin)
            {
                if (callback(begin, pos - begin))
                    return;
            }

            begin = ++pos;
        }
        else
        {
            ++pos;
        }
#endif
    }

    if (begin >= end)
        return;

    callback(begin, end - begin);
}

namespace detail
{

template <typename TokenExtractorType, typename Callback>
void forEachTokenImpl(const TokenExtractorType & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && extractor.nextInString(data, length, &cur, &token_start, &token_len))
    {
        if (callback(data + token_start, token_len))
            return;
    }
}

}

/// Calls the callback for each token in the data.
/// Stops searching tokens if the callback returns true.
template <Fn<bool(const char *, size_t)> Callback>
void forEachToken(const ITokenExtractor & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    if (length == 0)
        return;

    switch (extractor.getType())
    {
        case ITokenExtractor::Type::SplitByNonAlpha:
        {
            const auto & split_by_non_alpha_extractor = assert_cast<const SplitByNonAlphaTokenExtractor &>(extractor);
            split_by_non_alpha_extractor.forEachTokenImpl(data, length, callback);
            return;
        }
        case ITokenExtractor::Type::Ngrams:
        {
            const auto & ngrams_extractor = assert_cast<const NgramsTokenExtractor &>(extractor);

            if (length < ngrams_extractor.getN())
                return;

            detail::forEachTokenImpl(ngrams_extractor, data, length, callback);
            return;
        }
        case ITokenExtractor::Type::SplitByString:
        {
            const auto & split_by_string_extractor = assert_cast<const SplitByStringTokenExtractor &>(extractor);
            detail::forEachTokenImpl(split_by_string_extractor, data, length, callback);
            return;
        }
        case ITokenExtractor::Type::Array:
        {
            callback(data, length);
            return;
        }
        case ITokenExtractor::Type::SparseGrams:
        {
            const auto & sparse_grams_extractor = assert_cast<const SparseGramsTokenExtractor &>(extractor);
            detail::forEachTokenImpl(sparse_grams_extractor, data, length, callback);
            return;
        }
    }
}

void forEachTokenToBloomFilter(const ITokenExtractor & extractor, const char * data, size_t length, BloomFilter & bloom_filter);

}
