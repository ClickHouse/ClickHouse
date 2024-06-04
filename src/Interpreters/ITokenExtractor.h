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
    virtual bool nextInString(
        const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
        = 0;

    /// Optimized version that can assume at least 15 padding bytes after data + len (as our Columns provide).
    virtual bool nextInStringPadded(
        const char * data, size_t length, size_t * __restrict pos, size_t * __restrict token_start, size_t * __restrict token_length) const
    {
        return nextInString(data, length, pos, token_start, token_length);
    }

    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextInStringLike(const char * data, size_t length, size_t * pos, String & out) const = 0;

    virtual void stringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    virtual void stringPaddedToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const
    {
        stringToBloomFilter(data, length, bloom_filter);
    }

    virtual void stringLikeToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    /// Special implementation for creating bloom filter for startsWith function.
    /// It ignores the last token.
    virtual void stringStartsWithToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    /// Special implementation for creating bloom filter for startsWith function.
    /// It ignores the first token.
    virtual void stringEndsWithToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    virtual void stringToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;

    virtual void stringPaddedToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const
    {
        stringToGinFilter(data, length, gin_filter);
    }

    virtual void stringLikeToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;

    virtual void stringStartsWithToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;

    virtual void stringEndsWithToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const = 0;
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

    void stringStartsWithToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0, prev_token_start = 0;
        size_t token_len = 0, prev_token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
        {
            if (prev_token_len > 0)
            {
                bloom_filter.add(data + prev_token_start, prev_token_len);
            }
            prev_token_start = token_start;
            prev_token_len = token_len;
        }
    }

    void stringEndsWithToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;
        bool first_token = true;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
        {
            if (!first_token)
                bloom_filter.add(data + token_start, token_len);
            else
                first_token = false;
        }
    }

    void stringToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
            gin_filter.addTerm(data + token_start, token_len);
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

    void stringStartsWithToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0, prev_token_start = 0;
        size_t token_len = 0, prev_token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
        {
            if (prev_token_len > 0)
            {
                gin_filter.addTerm(data + prev_token_start, prev_token_len);
            }
            prev_token_start = token_start;
            prev_token_len = token_len;
        }
    }

    void stringEndsWithToGinFilter(const char * data, size_t length, GinFilter & gin_filter) const override
    {
        gin_filter.setQueryString(data, length);

        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;
        bool first_token = true;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, &cur, &token_start, &token_len))
        {
            if (!first_token)
                gin_filter.addTerm(data + token_start, token_len);
            else
                first_token = false;
        }
    }
};


/// Parser extracting all ngrams from string.
struct NgramTokenExtractor final : public ITokenExtractorHelper<NgramTokenExtractor>
{
    explicit NgramTokenExtractor(size_t n_) : n(n_) { }

    static const char * getName() { return "ngrambf_v1"; }

    bool nextInString(
        const char * data,
        size_t length,
        size_t * __restrict pos,
        size_t * __restrict token_start,
        size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * pos, String & token) const override;

    size_t getN() const { return n; }

private:
    size_t n;
};

/// Parser extracting tokens (sequences of numbers and ascii letters).
struct SplitTokenExtractor final : public ITokenExtractorHelper<SplitTokenExtractor>
{
    static const char * getName() { return "tokenbf_v1"; }

    bool nextInString(
        const char * data,
        size_t length,
        size_t * __restrict pos,
        size_t * __restrict token_start,
        size_t * __restrict token_length) const override;

    bool nextInStringPadded(
        const char * data,
        size_t length,
        size_t * __restrict pos,
        size_t * __restrict token_start,
        size_t * __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t * __restrict pos, String & token) const override;
};
}
