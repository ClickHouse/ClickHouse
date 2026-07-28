#pragma once

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <span>
#include <vector>
#include <base/defines.h>
#include <base/sort.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/isValidUTF8.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

/// The reusable buffers for Token mode to avoid repeated heap allocations in various places.
struct TokenScratch
{
    VectorWithMemoryTracking<std::string_view> tokens;
    PaddedPODArray<char> ngram_buffer;
};

struct ClassProbability
{
    UInt32 class_id;
    double probability;
};

/// The reusable buffers to avoid repeated heap allocations in various places. An instance is owned by the caller.
struct NaiveBayesScratch
{
    /// For byte and code-point modes, padded_input is the query input with `(n - 1)` start/end tokens on each side if needed.
    PaddedPODArray<char> padded_input;

    /// In code-point mode, this holds the byte offset of each code point in `padded_input`, plus a trailing sentinel equal to its size.
    /// For example, the string "a𐍈b" has 6 bytes and 3 code points, so code_point_offsets = {0, 1, 5, 6}.  Storing the size at the end
    /// allows us to easily know the size of the final codepoint during streaming over the input.
    PODArray<UInt32> code_point_offsets;

    /// In token mode, this holds the word tokens and the generated n-gram buffer.
    TokenScratch token_scratch;

    /// During classification, this accumulates the per-class scores, indexed by dense class index [0, num_classes).
    PODArray<Float64> scores;

    /// This is the reused output for `classifyWithAllProbs`; each entry pairs a class id with its probability.
    VectorWithMemoryTracking<ClassProbability> probabilities;
};

/// Returned by `prepareNgram`. Only code-point mode reports `valid == false`, when the bytes are not valid UTF-8.
struct PreparedNgram
{
    std::string_view key;
    size_t token_count;
    bool valid = true;
};


/// Each token is a single byte.
struct BytePolicy
{
    /// This is not applied to training data, which is already tokenized into n-grams. It is applied to query input, which is tokenized into n-grams
    /// to match against the training data.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view input_text,
        UInt32 ngram_size,
        std::string_view start_token,
        std::string_view end_token,
        NaiveBayesScratch & scratch,
        Visit && visit) const
    {
        const size_t start_pad_count = start_token.empty() ? 0 : ngram_size - 1;
        const size_t end_pad_count = end_token.empty() ? 0 : ngram_size - 1;

        /// TODO: If both pad counts are zero, skip the copy and emit n-grams as views straight into input_text.
        auto & padded = scratch.padded_input;
        padded.resize(start_pad_count + input_text.size() + end_pad_count);

        if (start_pad_count)
            memset(padded.data(), start_token[0], start_pad_count);

        memcpy(padded.data() + start_pad_count, input_text.data(), input_text.size());

        if (end_pad_count)
            memset(padded.data() + start_pad_count + input_text.size(), end_token[0], end_pad_count);

        const size_t padded_size = padded.size();

        /// With no padding configured the raw input can be shorter than the n-gram size, which has no n-grams.
        if (padded_size < ngram_size)
            return 0;

        const size_t ngram_count = padded_size - ngram_size + 1;
        for (size_t i = 0; i < ngram_count; ++i)
            visit(std::string_view(padded.data() + i, ngram_size));
        return ngram_count;
    }

    /// This method is applied to training data but not to query input.
    /// Byte n-grams need no normalization: every byte is significant.
    PreparedNgram prepareNgram(std::string_view ngram, TokenScratch &) const { return {ngram, ngram.size()}; }
};

/// Each token is a single Unicode (UTF-8) code point.
struct CodePointPolicy
{
    /// This is not applied to training data, which is already tokenized into n-grams. It is applied to query input, which is tokenized into n-grams
    /// to match against the training data.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view input_text,
        UInt32 ngram_size,
        std::string_view start_token,
        std::string_view end_token,
        NaiveBayesScratch & scratch,
        Visit && visit) const
    {
        const size_t start_pad_count = start_token.empty() ? 0 : ngram_size - 1;
        const size_t end_pad_count = end_token.empty() ? 0 : ngram_size - 1;

        /// TODO: If both pad counts are zero, skip the copy and compute the offsets over input_text directly.
        auto & padded = scratch.padded_input;
        padded.resize(start_pad_count * start_token.size() + input_text.size() + end_pad_count * end_token.size());
        size_t write_pos = 0;

        for (size_t i = 0; i < start_pad_count; ++i)
        {
            memcpy(padded.data() + write_pos, start_token.data(), start_token.size());
            write_pos += start_token.size();
        }

        memcpy(padded.data() + write_pos, input_text.data(), input_text.size());
        write_pos += input_text.size();

        for (size_t i = 0; i < end_pad_count; ++i)
        {
            memcpy(padded.data() + write_pos, end_token.data(), end_token.size());
            write_pos += end_token.size();
        }

        auto & offsets = scratch.code_point_offsets;
        offsets.clear();

        const size_t padded_size = padded.size();
        size_t byte_pos = 0;
        while (byte_pos < padded_size)
        {
            offsets.push_back(static_cast<UInt32>(byte_pos));
            byte_pos += DB::UTF8::seqLength(static_cast<UInt8>(padded[byte_pos]));
        }
        offsets.push_back(static_cast<UInt32>(padded_size));

        const size_t code_point_count = offsets.size() - 1;
        if (code_point_count < ngram_size)
            return 0;

        const size_t ngram_count = code_point_count - ngram_size + 1;
        for (size_t i = 0; i < ngram_count; ++i)
            visit(std::string_view(padded.data() + offsets[i], offsets[i + ngram_size] - offsets[i]));

        return ngram_count;
    }

    /// This method is applied to training data but not to query input.
    /// Codepoint n-grams need no normalization: every codepoint is significant.
    PreparedNgram prepareNgram(std::string_view ngram, TokenScratch &) const
    {
        const auto * bytes = reinterpret_cast<const UInt8 *>(ngram.data());
        if (!UTF8::isValidUTF8(bytes, ngram.size()))
            return {ngram, 0, false};

        return {ngram, UTF8::countCodePoints(bytes, ngram.size())};
    }
};

/// Each token is a whitespace-delimited word.
struct TokenPolicy
{
    /// Removes redundant whitespace and appends tokens to `tokens`.
    void tokenize(std::string_view text, VectorWithMemoryTracking<std::string_view> & tokens) const
    {
        /// The reservation assumes about three bytes per token on average.
        tokens.reserve(tokens.size() + text.size() / 3);

        size_t pos = 0;
        const char * data = text.data();
        const size_t length = text.size();

        while (pos < length)
        {
            while (pos < length && isWhitespaceASCII(data[pos]))
                ++pos;
            if (pos == length)
                break;

            const size_t word_start = pos;
            while (pos < length && !isWhitespaceASCII(data[pos]))
                ++pos;

            tokens.emplace_back(data + word_start, pos - word_start);
        }
    }

    /// Combines the tokens in `window` into a single n-gram, with each token separated by a single space.
    /// The result is written to `ngram`.
    void join(std::span<const std::string_view> window, PaddedPODArray<char> & ngram) const
    {
        chassert(!window.empty());

        /// Each pair of consecutive tokens is separated by a single space.
        size_t total_size = window.size() - 1;
        for (const auto & token : window)
            total_size += token.size();
        ngram.resize(total_size);

        size_t pos = 0;
        for (size_t i = 0; i < window.size(); ++i)
        {
            if (i)
                ngram[pos++] = ' ';
            memcpy(ngram.data() + pos, window[i].data(), window[i].size());
            pos += window[i].size();
        }
    }

    /// This is not applied to training data, which is already tokenized into n-grams. It is applied to query input, which is tokenized into n-grams
    /// to match against the training data.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view input_text,
        UInt32 ngram_size,
        std::string_view start_token,
        std::string_view end_token,
        NaiveBayesScratch & scratch,
        Visit && visit) const
    {
        auto & tokens = scratch.token_scratch.tokens;
        tokens.clear();

        const UInt32 start_pad_count = start_token.empty() ? 0 : ngram_size - 1;
        const UInt32 end_pad_count = end_token.empty() ? 0 : ngram_size - 1;

        tokens.insert(tokens.end(), start_pad_count, start_token);
        tokenize(input_text, tokens);
        tokens.insert(tokens.end(), end_pad_count, end_token);

        if (tokens.size() < ngram_size)
            return 0;

        const size_t ngram_count = tokens.size() - ngram_size + 1;
        auto & ngram = scratch.token_scratch.ngram_buffer;
        for (size_t i = 0; i < ngram_count; ++i)
        {
            join(std::span<const std::string_view>(&tokens[i], ngram_size), ngram);
            visit(std::string_view(ngram.data(), ngram.size()));
        }
        return ngram_count;
    }

    /// This method is applied to training data but not to query input.
    /// Token n-grams are normalized by removing extra whitespace, so "a b" and "a   b" are the same n-gram.
    /// We do not apply such normalization to `Byte` and `CodePoint` modes because it is not as clear cut as whitespace in token
    /// mode. It is never a bad idea to normalize both the training and query data by removing whitespace.
    PreparedNgram prepareNgram(std::string_view ngram, TokenScratch & scratch) const
    {
        size_t token_count = 0;

        /// Whether the ngram is already in proper form. If so, we can avoid the extra copy by skipping the normalization.
        bool canonical = true;
        const size_t length = ngram.size();
        size_t pos = 0;
        while (pos < length)
        {
            if (isWhitespaceASCII(ngram[pos]))
            {
                if (token_count == 0 || ngram[pos] != ' ')
                    canonical = false;
                const size_t whitespace_start = pos;
                while (pos < length && isWhitespaceASCII(ngram[pos]))
                    ++pos;
                if (pos - whitespace_start > 1 || pos == length)
                    canonical = false;
            }
            else
            {
                ++token_count;
                while (pos < length && !isWhitespaceASCII(ngram[pos]))
                    ++pos;
            }
        }

        if (canonical || token_count == 0)
            return {ngram, token_count};

        scratch.tokens.clear();
        tokenize(ngram, scratch.tokens);
        join(scratch.tokens, scratch.ngram_buffer);
        return {std::string_view(scratch.ngram_buffer.data(), scratch.ngram_buffer.size()), token_count};
    }
};

/// Maps an n-gram to its dense index.
using NGramIndexMap = StringHashMap<UInt32>;

using ClassCountMap = HashMap<UInt32, UInt64, HashCRC32<UInt32>>; // class id -> total n-gram count
using ClassIndexMap = HashMap<UInt32, UInt32, HashCRC32<UInt32>>; // class id -> dense class index
using ClassLogPriorMap = HashMap<UInt32, double, HashCRC32<UInt32>>; // class id -> log prior probability

/// One observed `(n-gram, class, count)` row handed from the trainer to `NaiveBayesModelData::compile`. The
/// n-gram index and class id are packed into one 64-bit `key` (index in the high 32 bits, class id in the low
/// 32) so the whole set sorts and groups cheaply.
struct NaiveBayesEntry
{
    NaiveBayesEntry(UInt32 ngram_index, UInt32 class_id, UInt64 count_)
        : key((static_cast<UInt64>(ngram_index) << 32) | class_id)
        , count(count_)
    {
    }

    UInt32 ngramIndex() const { return static_cast<UInt32>(key >> 32); }
    UInt32 classId() const { return static_cast<UInt32>(key); }
    UInt64 getCount() const { return count; }

    /// Orders entries by the packed key — n-gram index first, then class id; the count is not part of the
    /// order. `buildNgramTables` sorts on this so each n-gram's rows are contiguous and its per-class rows adjacent.
    bool operator<(const NaiveBayesEntry & other) const { return key < other.key; }

private:
    UInt64 key;
    UInt64 count;
};

/// How class prior probabilities are determined when finalizing a model.
enum class PriorsMode : uint8_t
{
    Uniform, /// Equal probability for every class.
    Proportional, /// Probability proportional to each class's total n-gram count.
    Explicit, /// Probabilities given explicitly per class.
};

enum class TokenizerMode : uint8_t
{
    Byte, /// Each token is a single byte.
    CodePoint, /// Each token is one Unicode code point.
    Token, /// Each token is a whitespace-delimited word.
};

/// The largest n-gram size accepted. Query-time tokenization scans n units per n-gram (plus (n-1) padding tokens
/// per side), so cost grows with n; real n-gram sizes are tiny, and this cap keeps a misconfigured model from
/// making every query allocate enormous buffers.
static constexpr UInt32 MAX_NGRAM_SIZE = 1024;

/// The tokenization mode's name, for error and log messages.
inline const char * toString(TokenizerMode mode)
{
    switch (mode)
    {
        case TokenizerMode::Byte: return "byte";
        case TokenizerMode::CodePoint: return "codepoint";
        case TokenizerMode::Token: return "token";
    }
    UNREACHABLE();
}

/// Parses a `mode` string into the enum used everywhere after; rejects any unknown mode.
inline TokenizerMode parseTokenizerMode(const String & mode)
{
    if (mode == "byte")
        return TokenizerMode::Byte;
    if (mode == "codepoint")
        return TokenizerMode::CodePoint;
    if (mode == "token")
        return TokenizerMode::Token;
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "NaiveBayes: mode must be 'byte', 'codepoint', or 'token', got '{}'", mode);
}

/// Resolves a padding token for the given mode into the bytes used to pad the input. For `byte` and `codepoint`
/// the token is given as a number (decimal or 0x hex) and resolved to its byte / UTF-8 here, so the same value is
/// portable (raw bytes cannot travel through XML config) and means the same thing whether it is set on a dictionary
/// layout or passed to `naiveBayesNgrams`; `token` mode takes the literal token string.
inline String parsePaddingToken(const String & raw_value, TokenizerMode mode, std::string_view parameter_name)
{
    /// Parses a bounded non-negative integer for byte/codepoint modes. from_chars is used (not parse<>) so an
    /// out-of-range value is rejected rather than silently wrapped onto another valid token.
    auto parse_number = [&](UInt32 max_value) -> UInt32
    {
        const char * begin = raw_value.data();
        const char * const end = begin + raw_value.size();

        /// Accept a 0x / 0X hex prefix in addition to decimal; strip it before parsing.
        int base = 10;
        if (raw_value.size() > 2 && begin[0] == '0' && (begin[1] == 'x' || begin[1] == 'X'))
        {
            base = 16;
            begin += 2;
        }
        UInt32 number = 0;
        const auto [parsed_end, error] = std::from_chars(begin, end, number, base);

        /// One check rejects empty input, non-digits, trailing characters, overflow, and values past the maximum.
        if (begin == end || error != std::errc{} || parsed_end != end || number > max_value)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes: {} for '{}' mode must be an integer between 0 and {} (decimal or 0x hex), got '{}'",
                parameter_name,
                toString(mode),
                max_value,
                raw_value);
        return number;
    };

    /// A byte-mode token is the single resolved byte.
    if (mode == TokenizerMode::Byte)
        return String(1, static_cast<char>(parse_number(0xFF)));

    if (mode == TokenizerMode::CodePoint)
    {
        const UInt32 code_point = parse_number(0x10FFFF);

        /// Surrogates are not Unicode scalar values and have no UTF-8 encoding.
        if (code_point >= 0xD800 && code_point <= 0xDFFF)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes: {} for 'codepoint' mode must not be a UTF-16 surrogate (0xD800-0xDFFF), got '{}'",
                parameter_name,
                raw_value);
        /// A codepoint-mode token is the code point's UTF-8 bytes, matching how the input is tokenized.
        char utf8[4];
        const size_t length = UTF8::convertCodePointToUTF8(static_cast<int>(code_point), utf8, sizeof(utf8));
        return String(utf8, length);
    }

    /// token mode: the literal token, which must be a single whitespace-delimited token. An empty value means
    /// "no padding" and is handled by the caller, so it never reaches here.
    if (std::ranges::any_of(raw_value, isWhitespaceASCII))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "NaiveBayes: {} for 'token' mode must not contain whitespace, got '{}'", parameter_name, raw_value);
    return raw_value;
}

template <typename Tokenizer>
class NaiveBayesModelData
{
public:
    NaiveBayesModelData(UInt32 ngram_size_, String start_token_, String end_token_, Tokenizer tokenizer_)
        : ngram_size(ngram_size_)
        , start_token(std::move(start_token_))
        , end_token(std::move(end_token_))
        , tokenizer(std::move(tokenizer_))
    {
    }

    NaiveBayesModelData(const NaiveBayesModelData &) = delete;
    NaiveBayesModelData & operator=(const NaiveBayesModelData &) = delete;
    NaiveBayesModelData(NaiveBayesModelData &&) = delete;
    NaiveBayesModelData & operator=(NaiveBayesModelData &&) = delete;

    /// Returns the dense index of `ngram` and assigns a new one (and copies its bytes into the
    /// arena) when it is not yet in the vocabulary.
    UInt32 getOrAssignNgramIndex(std::string_view ngram)
    {
        ArenaKeyHolder key_holder{ngram, key_arena};
        NGramIndexMap::LookupResult it = nullptr;
        bool inserted = false;
        ngram_to_index.emplace(key_holder, it, inserted);

        if (inserted)
        {
            const size_t new_index = ngram_to_index.size() - 1;
            if (new_index > std::numeric_limits<UInt32>::max())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes: the number of distinct n-grams exceeds the supported maximum of {}",
                    std::numeric_limits<UInt32>::max());
            it->getMapped() = static_cast<UInt32>(new_index);
        }

        return it->getMapped();
    }

    /// This method is applied to training data but not to query input.
    PreparedNgram prepareNgram(std::string_view ngram, TokenScratch & scratch) const { return tokenizer.prepareNgram(ngram, scratch); }

    /// Transforms the accumulated training data into the compact, query-ready model, filling the member arrays.
    /// The inputs are `observations` (one `(n-gram, class, count)` row each), the per-class `class_totals`, and
    /// the per-class `log_priors`; `alpha` is the additive (Lidstone) smoothing strength. `observations` is sorted in place,
    /// while `class_totals` and `log_priors` are only read.
    ///
    /// The model is laid out as two groups of arrays:
    ///   - The first group is indexed by the dense class index `c`. `class_ids[c]` returns the original class id.
    ///     `log_prior[c]` is the log of class `c`'s prior probability. `absent_ngram_log_prob[c] = log(alpha /
    ///     (class_totals[c] + alpha * V))` is the baseline log-probability that class `c` gives an n-gram it never
    ///     saw, where `V` is the vocabulary size.
    ///
    ///   - The second group stores the present `(class, delta)` pairs per dense n-gram index, in
    ///     compressed-sparse-row (CSR) form. The full n-gram-by-class table is mostly empty, because each n-gram is
    ///     usually seen by only a few classes, so we keep only the pairs that actually occurred. They all live
    ///     back-to-back in one flat `class_deltas` array, and `ngram_offsets` records where each n-gram's run
    ///     begins, so n-gram `i` owns the slice `class_deltas[ngram_offsets[i] .. ngram_offsets[i + 1])`. Each
    ///     entry there is a class that did see the n-gram, paired with its `delta = log((count + alpha) / alpha)`.
    ///
    ///     The absent pairs (a class that never saw an n-gram) are not stored at all, and they need not be, because
    ///     the smoothed log-probability of an unseen n-gram is the same for every n-gram that class never saw. It
    ///     depends only on the class, not the n-gram. That single per-class number is the `absent_ngram_log_prob[c]`
    ///     baseline above.
    ///
    /// For example (token mode, n = 1, alpha = 1):
    ///   Six source rows over two classes, in arrival order:
    ///   "good" -> class 0 (count 3),  "great" -> class 0 (count 1),  "the" -> class 0 (count 2),
    ///   "bad"  -> class 1 (count 4),  "awful" -> class 1 (count 1),  "the" -> class 1 (count 2).
    /// In first-seen order the vocabulary is {"good": 0, "great": 1, "the": 2, "bad": 3, "awful": 4} (V = 5), the
    /// classes are {0, 1}, and class_totals = {0: 6, 1: 7}. Note that "the" is shared by both classes.
    ///
    /// Each observation is a `(n-gram index, class, count)` row. `compile` first sorts them by the packed key
    /// (n-gram index, then class), which groups each n-gram's rows together. Here the last arrival row, "the" in
    /// class 1, moves up next to "the" in class 0:
    ///
    ///   observations (sorted) = [(0, 0, 3),   // "good"  in class 0
    ///                            (1, 0, 1),   // "great" in class 0
    ///                            (2, 0, 2),   // "the"   in class 0
    ///                            (2, 1, 2),   // "the"   in class 1  (moved up from the end)
    ///                            (3, 1, 4),   // "bad"   in class 1
    ///                            (4, 1, 1)]   // "awful" in class 1
    ///
    /// It then fills:
    ///
    ///   class_ids             = [0, 1]
    ///   log_prior             = [log P(0), log P(1)]
    ///   absent_ngram_log_prob = [log(1/11), log(1/12)]    // -log(class_totals[c] + V): -log(6 + 5), -log(7 + 5)
    ///   ngram_offsets         = [0, 1, 2, 4, 5, 6]        // one slice per n-gram; the 2 -> 4 jump is "the"'s two entries
    ///   class_deltas          = [{class 0, log 4},                    // "good"  (count 3 in class 0)
    ///                            {class 0, log 2},                    // "great" (count 1 in class 0)
    ///                            {class 0, log 3}, {class 1, log 3},  // "the"   (count 2 in class 0 and class 1)
    ///                            {class 1, log 5},                    // "bad"   (count 4 in class 1)
    ///                            {class 1, log 2}]                    // "awful" (count 1 in class 1)
    void
    compile(PODArray<NaiveBayesEntry> & observations, const ClassCountMap & class_totals, const ClassLogPriorMap & log_priors, double alpha)
    {
        const size_t vocabulary_size = ngram_to_index.size();

        chassert(vocabulary_size > 0);

        const double log_alpha = std::log(alpha);
        const ClassIndexMap class_id_to_class_index = buildClassTables(class_totals, log_priors, vocabulary_size, alpha, log_alpha);
        buildNgramTables(observations, class_id_to_class_index, vocabulary_size, alpha, log_alpha);
    }

    void computeScores(std::string_view input, NaiveBayesScratch & scratch) const
    {
        const size_t num_classes = class_ids.size();

        auto & scores = scratch.scores;
        scores.resize(num_classes);
        for (size_t c = 0; c < num_classes; ++c)
            scores[c] = log_prior[c];

        /// Only n-grams in the model vocabulary are features. An n-gram seen in no class during training carries no
        /// class signal, so it is skipped entirely: it contributes neither a delta nor the per-class absent baseline.
        size_t matched_ngrams = 0;
        auto accumulate = [&](std::string_view ngram)
        {
            auto it = ngram_to_index.find(ngram);
            if (!it)
                return;
            ++matched_ngrams;
            const UInt32 index = it->getMapped();

            chassert(index + 1 < ngram_offsets.size());

            const UInt32 begin = ngram_offsets[index];
            const UInt32 end = ngram_offsets[index + 1];
            for (UInt32 j = begin; j < end; ++j)
                scores[class_deltas[j].class_index] += static_cast<double>(class_deltas[j].delta);
        };
        tokenizer.enumerateNgrams(input, ngram_size, start_token, end_token, scratch, accumulate);

        for (size_t c = 0; c < num_classes; ++c)
            scores[c] += static_cast<double>(matched_ngrams) * static_cast<double>(absent_ngram_log_prob[c]);
    }

    /// Maps a dense class index back to the original class id.
    UInt32 classIdAt(size_t class_index) const
    {
        chassert(class_index < class_ids.size());
        return class_ids[class_index];
    }

    UInt64 getAllocatedBytes() const
    {
        UInt64 total = sizeof(*this);
        total += key_arena.allocatedBytes();
        total += ngram_to_index.getBufferSizeInBytes();
        total += class_ids.allocated_bytes() + log_prior.allocated_bytes() + absent_ngram_log_prob.allocated_bytes();
        total += ngram_offsets.allocated_bytes() + class_deltas.allocated_bytes();
        return total;
    }

    size_t getElementCount() const { return ngram_to_index.size(); }

private:
    ClassIndexMap buildClassTables(
        const ClassCountMap & class_totals, const ClassLogPriorMap & log_priors, size_t vocabulary_size, double alpha, double log_alpha)
    {
        VectorWithMemoryTracking<UInt32> classes;
        classes.reserve(class_totals.size());
        for (const auto & entry : class_totals)
            classes.push_back(entry.getKey());
        ::sort(classes.begin(), classes.end());

        const size_t num_classes = classes.size();

        chassert(num_classes > 0);

        class_ids.resize(num_classes);
        log_prior.resize(num_classes);
        absent_ngram_log_prob.resize(num_classes);

        const double smoothing = alpha * static_cast<double>(vocabulary_size);
        if (!std::isfinite(smoothing))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: alpha is too large; the smoothing term (alpha * vocabulary size = {} * {}) is not finite",
                alpha,
                vocabulary_size);

        ClassIndexMap class_id_to_class_index;
        for (size_t c = 0; c < num_classes; ++c)
        {
            const UInt32 class_id = classes[c];
            class_id_to_class_index[class_id] = static_cast<UInt32>(c);
            class_ids[c] = class_id;
            log_prior[c] = log_priors.at(class_id);
            const double denom = static_cast<double>(class_totals.at(class_id)) + smoothing;
            absent_ngram_log_prob[c] = static_cast<Float32>(log_alpha - std::log(denom));
        }
        return class_id_to_class_index;
    }

    void buildNgramTables(
        PODArray<NaiveBayesEntry> & observations,
        const ClassIndexMap & class_id_to_class_index,
        size_t vocabulary_size,
        double alpha,
        double log_alpha)
    {
        /// `ngram_offsets` holds UInt32 offsets into `class_deltas`, whose size never exceeds the number of
        /// observations. Reject a source that would overflow those offsets rather than wrap them.
        if (observations.size() > std::numeric_limits<UInt32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes: the number of n-gram/class observations exceeds the supported maximum of {}",
                std::numeric_limits<UInt32>::max());

        ::sort(observations.begin(), observations.end());

        ngram_offsets.resize(vocabulary_size + 1);
        class_deltas.reserve(observations.size());

        const size_t num_observations = observations.size();
        size_t i = 0;
        for (size_t index = 0; index < vocabulary_size; ++index)
        {
            ngram_offsets[index] = static_cast<UInt32>(class_deltas.size());
            while (i < num_observations && observations[i].ngramIndex() == index)
            {
                const UInt32 class_id = observations[i].classId();
                UInt64 summed_count = observations[i].getCount();
                for (++i; i < num_observations && observations[i].ngramIndex() == index && observations[i].classId() == class_id; ++i)
                    summed_count += observations[i].getCount();
                const UInt32 class_index = class_id_to_class_index.at(class_id);
                const Float32 delta = static_cast<Float32>(std::log(static_cast<double>(summed_count) + alpha) - log_alpha);
                class_deltas.push_back(ClassDelta{class_index, delta});
            }
        }

        chassert(i == num_observations);

        ngram_offsets[vocabulary_size] = static_cast<UInt32>(class_deltas.size());
    }

    /// N-gram size.
    UInt32 ngram_size;

    /// Start / end padding tokens added at each end of the query input; empty when padding is disabled (the default).
    String start_token;
    String end_token;
    Tokenizer tokenizer;

    /// Owns the bytes of every distinct n-gram key; the keys in `ngram_to_index` are views into this arena.
    Arena key_arena;

    /// Maps each distinct n-gram to its dense index (0, 1, 2, ... in insertion order); that index is its row in the CSR arrays below.
    NGramIndexMap ngram_to_index;

    /// Dense class index -> original class id, ascending.
    PODArray<UInt32> class_ids;
    /// Dense class index -> log prior probability.
    PODArray<Float64> log_prior;
    /// Dense class index -> contribution of an absent n-gram, `log(alpha) - log(denom[c])`.
    PODArray<Float32> absent_ngram_log_prob;

    /// One present (class, delta) entry of an n-gram: the dense class index that observed it, and the amount its
    /// presence adds to that class's score above the absent-n-gram baseline, i.e. `log(count + alpha) - log(alpha)`.
    struct ClassDelta
    {
        UInt32 class_index;
        Float32 delta;
    };

    /// The n-gram tables in compressed-sparse-row form. `ngram_offsets` are the row pointers, indexed by dense
    /// n-gram index: n-gram `i`'s entries are the slice `[ngram_offsets[i], ngram_offsets[i + 1])` of
    /// `class_deltas`. It has the vocabulary size + 1 entries; its last entry is the total entry count.
    PODArray<UInt32> ngram_offsets;
    /// Every n-gram's present (class, delta) entries, grouped contiguously and located via `ngram_offsets`.
    PODArray<ClassDelta> class_deltas;
};


template <typename Tokenizer>
class NaiveBayesModel
{
public:
    explicit NaiveBayesModel(std::unique_ptr<NaiveBayesModelData<Tokenizer>> data_)
        : data(std::move(data_))
    {
    }

    /// Returns the class with the highest log probability. Ties resolve to the smallest class id.
    UInt32 classify(std::string_view input, NaiveBayesScratch & scratch) const
    {
        data->computeScores(input, scratch);
        return data->classIdAt(argmax(scratch.scores));
    }

    /// Returns the best class and its normalized probability.
    ClassProbability classifyWithProb(std::string_view input, NaiveBayesScratch & scratch) const
    {
        data->computeScores(input, scratch);
        const auto & scores = scratch.scores;
        const size_t best_class = argmax(scores);

        double sum_exp = 0.0;
        for (size_t c = 0; c < scores.size(); ++c)
            sum_exp += std::exp(scores[c] - scores[best_class]);
        return {data->classIdAt(best_class), 1.0 / sum_exp};
    }

    /// Returns every class with its normalized probability, ordered from most to least probable, with ties
    /// broken by ascending class id so that the order is fully determined.
    const VectorWithMemoryTracking<ClassProbability> & classifyWithAllProbs(std::string_view input, NaiveBayesScratch & scratch) const
    {
        data->computeScores(input, scratch);
        softmaxInto(scratch);
        ::sort(
            scratch.probabilities.begin(),
            scratch.probabilities.end(),
            [](const ClassProbability & a, const ClassProbability & b)
            {
                if (a.probability != b.probability)
                    return a.probability > b.probability; // most probable first
                return a.class_id < b.class_id; // ties: smallest class id first
            });
        return scratch.probabilities;
    }

    UInt64 getAllocatedBytes() const { return sizeof(*this) + data->getAllocatedBytes(); }

    size_t getElementCount() const { return data->getElementCount(); }

private:
    /// Index of the maximum score; ties resolve to the smallest index, which is the smallest class id.
    static size_t argmax(const PODArray<Float64> & scores)
    {
        chassert(!scores.empty());
        size_t best = 0;
        for (size_t c = 1; c < scores.size(); ++c)
            if (scores[c] > scores[best])
                best = c;
        return best;
    }

    /// Converts `scratch.scores` into normalized probabilities in `scratch.probabilities`, pairing each class id
    /// with its probability, using a numerically stable softmax.
    void softmaxInto(NaiveBayesScratch & scratch) const
    {
        chassert(!scratch.scores.empty());
        const size_t num_classes = scratch.scores.size();
        const auto & scores = scratch.scores;

        double max_log = -std::numeric_limits<double>::infinity();
        for (size_t c = 0; c < num_classes; ++c)
            max_log = std::max(max_log, scores[c]);

        auto & probabilities = scratch.probabilities;
        probabilities.resize(num_classes);
        double sum_exp = 0.0;
        for (size_t c = 0; c < num_classes; ++c)
        {
            const double exp_val = std::exp(scores[c] - max_log);
            probabilities[c] = {data->classIdAt(c), exp_val};
            sum_exp += exp_val;
        }

        for (auto & entry : probabilities)
            entry.probability /= sum_exp;
    }

    std::unique_ptr<NaiveBayesModelData<Tokenizer>> data;
};

}
