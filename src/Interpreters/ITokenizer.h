#pragma once

#include <Common/assert_cast.h>
#include <Functions/sparseGramsImpl.h>
#include <Interpreters/BloomFilter.h>
#include <base/FnTraits.h>
#include <base/types.h>
#include <fmt/format.h>

namespace DB
{

/// Interface for string parsers.
struct ITokenizer
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

    ITokenizer() = default;
    explicit ITokenizer(Type type_) : type(type_) {}
    ITokenizer(const ITokenizer &) = default;
    ITokenizer & operator=(const ITokenizer &) = default;

    Type getType() const { return type; }

    virtual ~ITokenizer() = default;
    virtual std::unique_ptr<ITokenizer> clone() const = 0;

    /// Returns a formatted description of the tokenizer with arguments.
    virtual String getDescription() const = 0;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of tokenizer).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const = 0;

    /// Optimized version that can assume at least 15 padding bytes after data + len (as our Columns provide).
    virtual bool nextInStringPadded(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const
    {
        return nextInString(data, length, pos, token_start, token_length);
    }

    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool nextInStringLike(const char * data, size_t length, size_t & pos, String & out) const = 0;

    /// Updates Bloom filter from exact-match string filter value
    virtual void stringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const = 0;

    /// Filters out tokens excessive for search.
    /// This method is inefficient and should be used only for constants.
    virtual std::vector<String> compactTokens(const std::vector<String> & tokens) const = 0;

    /// Updates Bloom filter from substring-match string filter value.
    /// An `ITokenizer` implementation may decide to skip certain
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
    /// An `ITokenizer` implementation may decide to skip certain
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

using TokenizerPtr = const ITokenizer *;

template <typename Derived>
class ITokenizerHelper : public ITokenizer
{
protected:
    explicit ITokenizerHelper(Type type_) : ITokenizer(type_) {}

private:
    std::unique_ptr<ITokenizer> clone() const override
    {
        return std::make_unique<Derived>(*static_cast<const Derived *>(this));
    }

    void stringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, cur, token_start, token_len))
            bloom_filter.add(data + token_start, token_len);
    }

    void stringPaddedToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringPadded(data, length, cur, token_start, token_len))
            bloom_filter.add(data + token_start, token_len);
    }

    void stringLikeToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter) const override
    {
        size_t cur = 0;
        String token;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringLike(data, length, cur, token))
            bloom_filter.add(token.c_str(), token.size());
    }

    void stringToTokens(const char * data, size_t length, std::vector<String> & tokens) const override
    {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived *>(this)->nextInString(data, length, cur, token_start, token_len))
            tokens.push_back({data + token_start, token_len});
    }

    void stringLikeToTokens(const char * data, size_t length, std::vector<String> & tokens) const override
    {
        size_t cur = 0;
        String token;

        while (cur < length && static_cast<const Derived *>(this)->nextInStringLike(data, length, cur, token))
            tokens.push_back(token);
    }

    std::vector<String> compactTokens(const std::vector<String> & tokens) const override
    {
        std::unordered_set<String> unique_tokens(tokens.begin(), tokens.end());
        return std::vector<String>(unique_tokens.begin(), unique_tokens.end());
    }
};

/// Parser extracting all ngrams from string.
struct NgramsTokenizer final : public ITokenizerHelper<NgramsTokenizer>
{
    explicit NgramsTokenizer(size_t n_) : ITokenizerHelper(Type::Ngrams), n(n_) {}

    static const char * getName() { return "ngrambf_v1"; }
    static const char * getExternalName() { return "ngrams"; }
    String getDescription() const override { return fmt::format("ngrams({})", n); }

    bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;

    size_t getN() const { return n; }

    bool supportsStringLike() const override { return true; }
private:
    size_t n;
};

/// Parser extracting tokens which consist of alphanumeric ASCII characters or Unicode characters (not necessarily alphanumeric)
struct SplitByNonAlphaTokenizer final : public ITokenizerHelper<SplitByNonAlphaTokenizer>
{
    SplitByNonAlphaTokenizer() : ITokenizerHelper(Type::SplitByNonAlpha) {}

    static const char * getName() { return "tokenbf_v1"; }
    static const char * getExternalName() { return "splitByNonAlpha"; }
    String getDescription() const override { return "splitByNonAlpha"; }

    bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const override;
    bool nextInStringPadded(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t & __restrict pos, String & token) const override;
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

    bool supportsStringLike() const override { return true; }
};

/// Parser extracting tokens which are separated by certain strings.
/// Allows to emulate e.g. BigQuery's LOG_ANALYZER.
struct SplitByStringTokenizer final : public ITokenizerHelper<SplitByStringTokenizer>
{
    explicit SplitByStringTokenizer(const std::vector<String> & separators_) : ITokenizerHelper(Type::SplitByString), separators(separators_) {}

    static const char * getName() { return "splitByString"; }
    static const char * getExternalName() { return getName(); }
    String getDescription() const override;

    bool nextInString(const char * data, size_t length, size_t & pos, size_t & token_start, size_t & token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
private:
    std::vector<String> separators;
};

/// Parser doing "no operation". Returns the entire input as a single token.
struct ArrayTokenizer final : public ITokenizerHelper<ArrayTokenizer>
{
    ArrayTokenizer() : ITokenizerHelper(Type::Array) {}

    static const char * getName() { return "array"; }
    static const char * getExternalName() { return getName(); }
    String getDescription() const override { return getName(); }

    bool nextInString(const char * data, size_t length, size_t & pos, size_t & token_start, size_t & token_length) const override;
    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;

    bool supportsStringLike() const override { return false; }
};

/// Parser extracting sparse grams (the same as function sparseGrams).
/// See sparseGramsImpl.h for more details.
struct SparseGramsTokenizer final : public ITokenizerHelper<SparseGramsTokenizer>
{
    explicit SparseGramsTokenizer(size_t min_length = 3, size_t max_length = 100, std::optional<size_t> min_cutoff_length_ = std::nullopt);

    static const char * getBloomFilterIndexName() { return "sparse_grams"; }
    static const char * getName() { return "sparseGrams"; }
    static const char * getExternalName() { return getName(); }

    String getDescription() const override;
    bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const override;
    std::vector<String> compactTokens(const std::vector<String> & tokens) const override;

    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;
    bool supportsStringLike() const override { return true; }
private:
    size_t min_gram_length;
    size_t max_gram_length;
    std::optional<size_t> min_cutoff_length;
    mutable SparseGramsImpl<true> sparse_grams_iterator;
    mutable const char * previous_data = nullptr;
    mutable size_t previous_len = 0;
};

namespace detail
{

template <bool is_padded, typename Tokenizer, typename Callback>
void forEachTokenImpl(const Tokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    if constexpr (is_padded)
    {
        while (cur < length && tokenizer.nextInStringPadded(data, length, cur, token_start, token_len))
        {
            if (callback(data + token_start, token_len))
                return;
        }
    }
    else
    {
        while (cur < length && tokenizer.nextInString(data, length, cur, token_start, token_len))
        {
            if (callback(data + token_start, token_len))
                return;
        }
    }
}

template <bool is_padded, typename Callback>
void forEachTokenCase(const ITokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    if (length == 0)
        return;

    switch (tokenizer.getType())
    {
        case ITokenizer::Type::SplitByNonAlpha:
        {
            const auto & split_by_non_alpha_tokenizer = assert_cast<const SplitByNonAlphaTokenizer &>(tokenizer);
            forEachTokenImpl<is_padded>(split_by_non_alpha_tokenizer, data, length, callback);
            return;
        }
        case ITokenizer::Type::Ngrams:
        {
            const auto & ngrams_tokenizer = assert_cast<const NgramsTokenizer &>(tokenizer);

            if (length < ngrams_tokenizer.getN())
                return;

            forEachTokenImpl<is_padded>(ngrams_tokenizer, data, length, callback);
            return;
        }
        case ITokenizer::Type::SplitByString:
        {
            const auto & split_by_string_tokenizer = assert_cast<const SplitByStringTokenizer &>(tokenizer);
            forEachTokenImpl<is_padded>(split_by_string_tokenizer, data, length, callback);
            return;
        }
        case ITokenizer::Type::Array:
        {
            callback(data, length);
            return;
        }
        case ITokenizer::Type::SparseGrams:
        {
            const auto & sparse_grams_tokenizer = assert_cast<const SparseGramsTokenizer &>(tokenizer);
            forEachTokenImpl<is_padded>(sparse_grams_tokenizer, data, length, callback);
            return;
        }
    }
}

}

/// Calls the callback for each token in the data.
/// Stops searching tokens if the callback returns true.
template <Fn<bool(const char *, size_t)> Callback>
void forEachTokenPadded(const ITokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    detail::forEachTokenCase<true>(tokenizer, data, length, callback);
}

template <Fn<bool(const char *, size_t)> Callback>
void forEachToken(const ITokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    detail::forEachTokenCase<false>(tokenizer, data, length, callback);
}

}
