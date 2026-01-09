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
        Default,
        Ngram,
        Split,
        NoOp,
        SparseGram,
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

    /// Optimized version that can assume at least 15 padding bytes after data + len (as our Columns provide).
    virtual bool nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
    {
        return nextInString(data, length, pos, token_start, token_length);
    }

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

    virtual void stringPaddedToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const
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

    void stringPaddedToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringPadded(data, length, &cur, &token_start, &token_len))
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


/// Parser extracting all ngrams from string.
struct NgramTokenExtractor final : public ITokenExtractorHelper<NgramTokenExtractor>
{
    explicit NgramTokenExtractor(size_t n_) : ITokenExtractorHelper(Type::Ngram), n(n_) {}

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
struct DefaultTokenExtractor final : public ITokenExtractorHelper<DefaultTokenExtractor>
{
    DefaultTokenExtractor() : ITokenExtractorHelper(Type::Default) {}

    static const char * getName() { return "tokenbf_v1"; }
    static const char * getExternalName() { return "splitByNonAlpha"; }

    bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const override;
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

    bool supportsStringLike() const override { return true; }
};

/// Parser extracting tokens which are separated by certain strings.
/// Allows to emulate e.g. BigQuery's LOG_ANALYZER.
struct SplitTokenExtractor final : public ITokenExtractorHelper<SplitTokenExtractor>
{
    explicit SplitTokenExtractor(const std::vector<String> & separators_) : ITokenExtractorHelper(Type::Split), separators(separators_) {}

    static const char * getName() { return "splitByString"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
private:
    std::vector<String> separators;
};

/// Parser doing "no operation". Returns the entire input as a single token.
struct NoOpTokenExtractor final : public ITokenExtractorHelper<NoOpTokenExtractor>
{
    NoOpTokenExtractor() : ITokenExtractorHelper(Type::NoOp) {}

    static const char * getName() { return "array"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
};

/// Parser extracting sparse grams (the same as function sparseGrams).
/// See sparseGrams.h for more details.
struct SparseGramTokenExtractor final : public ITokenExtractorHelper<SparseGramTokenExtractor>
{
    explicit SparseGramTokenExtractor(size_t min_length = 3, size_t max_length = 100, std::optional<size_t> min_cutoff_length_ = std::nullopt);

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

namespace detail
{

template <bool is_padded, typename TokenExtractorType, typename Callback>
void forEachTokenImpl(const TokenExtractorType & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    if constexpr (is_padded)
    {
        while (cur < length && extractor.nextInStringPadded(data, length, &cur, &token_start, &token_len))
        {
            if (callback(data + token_start, token_len))
                return;
        }
    }
    else
    {
        while (cur < length && extractor.nextInString(data, length, &cur, &token_start, &token_len))
        {
            if (callback(data + token_start, token_len))
                return;
        }
    }
}

template <bool is_padded, typename Callback>
void forEachTokenCase(const ITokenExtractor & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    if (length == 0)
        return;

    switch (extractor.getType())
    {
        case ITokenExtractor::Type::Default:
        {
            const auto & default_extractor = assert_cast<const DefaultTokenExtractor &>(extractor);
            forEachTokenImpl<is_padded>(default_extractor, data, length, callback);
            return;
        }
        case ITokenExtractor::Type::Ngram:
        {
            const auto & ngram_extractor = assert_cast<const NgramTokenExtractor &>(extractor);

            if (length < ngram_extractor.getN())
            {
                callback(data, length);
                return;
            }

            forEachTokenImpl<is_padded>(ngram_extractor, data, length, callback);
            return;
        }
        case ITokenExtractor::Type::Split:
        {
            const auto & split_extractor = assert_cast<const SplitTokenExtractor &>(extractor);
            forEachTokenImpl<is_padded>(split_extractor, data, length, callback);
            return;
        }
        case ITokenExtractor::Type::NoOp:
        {
            callback(data, length);
            return;
        }
        case ITokenExtractor::Type::SparseGram:
        {
            const auto & sparse_gram_extractor = assert_cast<const SparseGramTokenExtractor &>(extractor);
            forEachTokenImpl<is_padded>(sparse_gram_extractor, data, length, callback);
            return;
        }
    }
}

}

/// Calls the callback for each token in the data.
/// Stops searching tokens if the callback returns true.
template <Fn<bool(const char *, size_t)> Callback>
void forEachTokenPadded(const ITokenExtractor & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    detail::forEachTokenCase<true>(extractor, data, length, callback);
}

template <Fn<bool(const char *, size_t)> Callback>
void forEachToken(const ITokenExtractor & extractor, const char * __restrict data, size_t length, Callback && callback)
{
    detail::forEachTokenCase<false>(extractor, data, length, callback);
}

}
