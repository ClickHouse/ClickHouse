#pragma once

#include <Common/assert_cast.h>
#include <Common/StringUtils.h>
#include <Functions/sparseGramsImpl.h>
#include <Interpreters/BloomFilter.h>
#include <base/FnTraits.h>
#include <base/types.h>
#include <fmt/format.h>

#if defined(__SSE2__)
#  include <emmintrin.h>
#  if defined(__SSE4_2__)
#    include <nmmintrin.h>
#  endif
#endif

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
        AsciiCJK,
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

    virtual const char * getTokenizerName() const = 0;
    virtual const char * getTokenizerExternalName() const = 0;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of tokenizer).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool nextInString(const char * data, size_t length, size_t & __restrict pos, size_t & __restrict token_start, size_t & __restrict token_length) const = 0;

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
        bool is_prefix,
        bool is_suffix) const = 0;

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
        bool is_prefix,
        bool is_suffix) const = 0;

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

    const char * getTokenizerName() const override { return Derived::getName(); }
    const char * getTokenizerExternalName() const override { return Derived::getExternalName(); }

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
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

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
    bool nextInStringLike(const char * data, size_t length, size_t & __restrict pos, String & token) const override;
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

    bool supportsStringLike() const override { return true; }

    /// High-performance callback-based tokenizer with SSE optimization.
    /// Assumes data is padded from the right with at least 15 bytes (as our Columns provide).
    template <Fn<bool(const char *, size_t)> Callback>
    void forEachTokenImpl(const char * __restrict data, size_t length, Callback && callback) const
    {
        const char * begin = data;
        const char * end = data + length;
        const char * pos = data;

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
                    /// end of token started on previous haystack
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
                /// end of token starting in one of previous haystacks
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
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;
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
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;
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
    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;
    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;
private:
    size_t min_gram_length;
    size_t max_gram_length;
    std::optional<size_t> min_cutoff_length;
    mutable SparseGramsImpl<true> sparse_grams_iterator;
    mutable const char * previous_data = nullptr;
    mutable size_t previous_len = 0;
};

/// Split text into tokens using Unicode word boundary rules, similar to UAX #29.
/// 1. ASCII Alphanumeric Tokens
///
/// * ASCII letters (`A-Z`, `a-z`) and digits (`0-9`) are accumulated into tokens.
/// * `_` can appear at the **start, middle, or end** of a token, but a token cannot be **only `_` characters**.
///
///   * E.g., `a_b` -> token
///   * `_a` -> token
///   * `__` -> ignored (no alphanumeric)
///
/// 2. Connectors (ASCII only)
///
/// * ASCII `:` (U+003A) connects **letters only**, not digits.
/// * ASCII `.` and `'` connect **letters-letters** or **digits-digits**.
/// * If the connector cannot connect both sides, it is treated as a **token boundary**.
///
/// 3. Unicode / CJK
///
/// * Non-ASCII Unicode characters are **always single-character tokens** (including CJK).
///
/// 4. Token Validity
///
/// * ASCII tokens must contain at least **one ASCII letter or digit** to be valid.
/// * Non-ASCII Unicode characters are valid single-character tokens on their own.
/// * Connectors `_`, `:`, `.`, `'` cannot form a token by themselves.
/// * `_` can start or end the token but must **not be the only character**.
///
/// 5. Stream Processing
///
/// * Tokenization is **stream-based** -- return tokens as soon as they are complete.
/// * ASCII fast path should be taken first for performance.
/// * Only parse Unicode for **non-ASCII characters or stop characters**.
///
/// ---
///
/// Examples
///
/// | Input                    | Output Tokens                         |
/// | ------------------------ | ------------------------------------- |
/// | `a_b a_3 a_ _a __a_b_3_` | `['a_b','a_3','a_','_a','__a_b_3_']`  |
/// | `3_b 3_3 3_ _3 __3_4_3_` | `['3_b','3_3','3_','_3','__3_4_3_']`  |
/// | `a:b a:3 a: :a ::a:b:3:` | `['a:b','a','3','a','a','a:b','3']`   |
/// | `a'b a'3 a' 'a ''a'b'3'` | `['a\'b','a','3','a','a','a\'b','3']` |
/// | `a.b a.3 a. .a ..a.b.3.` | `['a.b','a','3','a','a','a.b','3']`   |
struct AsciiCJKTokenizer final : public ITokenizerHelper<AsciiCJKTokenizer>
{
    explicit AsciiCJKTokenizer()
        : ITokenizerHelper(Type::AsciiCJK)
    {
    }

    static const char * getName() { return "asciiCJK"; }
    static const char * getExternalName() { return getName(); }
    String getDescription() const override { return getName(); }

    bool nextInString(
        const char * data,
        size_t length,
        size_t & __restrict pos,
        size_t & __restrict token_start,
        size_t & __restrict token_length) const override;

    bool nextInStringLike(const char * data, size_t length, size_t & pos, String & token) const override;

    void substringToBloomFilter(const char * data, size_t length, BloomFilter & bloom_filter, bool is_prefix, bool is_suffix) const override;

    void substringToTokens(const char * data, size_t length, std::vector<String> & tokens, bool is_prefix, bool is_suffix) const override;

    bool supportsStringLike() const override { return true; }
};

namespace detail
{

template <typename Tokenizer, typename Callback>
void forEachTokenImpl(const Tokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;

    while (cur < length && tokenizer.nextInString(data, length, cur, token_start, token_len))
    {
        if (callback(data + token_start, token_len))
            return;
    }
}

}

/// Calls the callback for each token in the data.
/// Stops searching tokens if the callback returns true.
template <Fn<bool(const char *, size_t)> Callback>
void forEachToken(const ITokenizer & tokenizer, const char * __restrict data, size_t length, Callback && callback)
{
    if (length == 0)
        return;

    switch (tokenizer.getType())
    {
        case ITokenizer::Type::SplitByNonAlpha:
        {
            const auto & split_by_non_alpha_tokenizer = assert_cast<const SplitByNonAlphaTokenizer &>(tokenizer);
            split_by_non_alpha_tokenizer.forEachTokenImpl(data, length, callback);
            return;
        }
        case ITokenizer::Type::Ngrams:
        {
            const auto & ngrams_tokenizer = assert_cast<const NgramsTokenizer &>(tokenizer);

            if (length < ngrams_tokenizer.getN())
                return;

            detail::forEachTokenImpl(ngrams_tokenizer, data, length, callback);
            return;
        }
        case ITokenizer::Type::SplitByString:
        {
            const auto & split_by_string_tokenizer = assert_cast<const SplitByStringTokenizer &>(tokenizer);
            detail::forEachTokenImpl(split_by_string_tokenizer, data, length, callback);
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
            detail::forEachTokenImpl(sparse_grams_tokenizer, data, length, callback);
            return;
        }
        case ITokenizer::Type::AsciiCJK:
        {
            const auto & ascii_cjk_tokenizer = assert_cast<const AsciiCJKTokenizer &>(tokenizer);
            detail::forEachTokenImpl(ascii_cjk_tokenizer, data, length, callback);
            return;
        }
    }
}

void forEachTokenToBloomFilter(const ITokenizer & tokenizer, const char * data, size_t length, BloomFilter & bloom_filter);

}
