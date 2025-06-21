#pragma once

#include <base/types.h>

#include <Interpreters/BloomFilter.h>
#include <Interpreters/GinFilter.h>


namespace DB
{

/// Interface for string parsers.
struct ITokenExtractor
{
    virtual ~ITokenExtractor() = default;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of extractor).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const = 0;

    /// Optimized version that can assume at least 15 padding bytes after data + len (as our Columns provide).
    virtual bool nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
    {
        return nextInString(data, length, pos, token_start, token_length);
    }

    /// Slow implementation for tokenizers which don't support inplace tokenization.
    virtual std::vector<String> getTokens(const char * data, size_t length) const;

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

    /// Updates GIN filter from exact-match string filter value
    virtual void stringToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;

    /// Updates GIN filter from substring-match string filter value.
    /// An `ITokenExtractor` implementation may decide to skip certain
    /// tokens depending on whether the substring is a prefix or a suffix.
    virtual void substringToGinFilter(
        const char * data,
        size_t length,
        GinFilter & gin_filter,
        bool /*is_prefix*/,
        bool /*is_suffix*/) const
    {
        stringToGinFilter(data, length, gin_filter);
    }

    virtual void stringPaddedToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const
    {
        stringToGinFilter(data, length, gin_filter);
    }

    virtual void stringLikeToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;

};

using TokenExtractorPtr = const ITokenExtractor *;

template <typename Derived>
class ITokenExtractorHelper : public ITokenExtractor
{
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

    void stringToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);
        const auto& tokens = getTokens(data, length);
        for (const auto& token : tokens)
            gin_filter.addTerm(token.data(), token.size());
    }

    void stringPaddedToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringPadded(data, length, &cur, &token_start, &token_len))
            gin_filter.addTerm(data + token_start, token_len);
    }

    void stringLikeToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        String token;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringLike(data, length, &cur, token))
            gin_filter.addTerm(token.c_str(), token.size());
    }
};


/// Parser extracting all ngrams from string.
struct NgramTokenExtractor final : public ITokenExtractorHelper<NgramTokenExtractor>
{
    explicit NgramTokenExtractor(size_t n_) : n(n_) {}

    static const char * getName() { return "ngrambf_v1"; }
    static const char * getExternalName() { return "ngram"; }

    std::vector<String> getTokens(const char * data, size_t length) const override;
    bool nextInString(const char * data, size_t length, size_t *  __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    size_t getN() const { return n; }

private:
    size_t n;
};

/// Parser extracting tokens which consist of alphanumeric ASCII characters or Unicode characters (not necessarily alphanumeric)
struct DefaultTokenExtractor final : public ITokenExtractorHelper<DefaultTokenExtractor>
{
    static const char * getName() { return "tokenbf_v1"; }
    static const char * getExternalName() { return "default"; }

    bool nextInString(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringPadded(const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const override;
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToGinFilter(const char * data, size_t length, GinFilter & gin_filter, bool is_prefix, bool is_suffix) const override;
};

/// Parser extracting tokens which are separated by certain strings.
/// Allows to emulate e.g. BigQuery's LOG_ANALYZER.
struct SplitTokenExtractor final : public ITokenExtractorHelper<SplitTokenExtractor>
{
    explicit SplitTokenExtractor(const std::vector<String> & separators_);

    static const char * getName() { return "split"; }
    static const char * getExternalName() { return getName(); }

    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

private:
    std::vector<String> separators;
};

/// Parser doing "no operation". Returns the entire input as a single token.
struct NoOpTokenExtractor final : public ITokenExtractorHelper<NoOpTokenExtractor>
{
    static const char * getName() { return "no_op"; }
    static const char * getExternalName() { return getName(); }

    std::vector<String> getTokens(const char * data, size_t length) const override;
    bool nextInString(const char * data, size_t length, size_t * pos, size_t * token_start, size_t * token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;
};

}
