#pragma once

#include <algorithm>
#include <cmath>
#include <concepts>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <base/defines.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/isValidUTF8.h>

namespace DB
{

/// Per-call working buffers for classification, created once per query and reused across every row so that
/// the hot path performs no per-row heap allocation. An instance is owned by the caller and is therefore
/// confined to a single thread; the model it is used with stays immutable and shared.
struct NaiveBayesScratch
{
    /// Byte and code point modes build the padded input here and slice n-grams out of it as views.
    PaddedPODArray<char> padded;

    /// Code point mode: byte offset of each code point in `padded`, with a trailing sentinel equal to its size.
    PODArray<UInt32> code_point_offsets;

    /// Token mode: the word tokens of one input (views into the input) and the buffer the current n-gram is joined into.
    VectorWithMemoryTracking<std::string_view> tokens;
    std::string ngram_buffer;

    /// The per-class score accumulator, indexed by dense class index (0 .. num_classes).
    PODArray<Float64> scores;

    /// Reused output for the probability-returning entry points, indexed by dense class index (0 .. num_classes).
    std::vector<std::pair<UInt32, double>> probabilities;
};

template <class T>
concept Tokenizer = requires {
    { T::start_token } -> std::convertible_to<std::string_view>;
    { T::end_token } -> std::convertible_to<std::string_view>;
};

/// What `prepareNgram` returns for one source n-gram: the key to store, the number of tokens it resolves to
/// (validated against n), and whether it is well-formed for the tokenizer. Only code-point mode can report
/// `valid == false`, when the bytes are not valid UTF-8.
struct PreparedNgram
{
    std::string_view key;
    size_t token_count;
    bool valid = true;
};

/// Byte-level tokenizer: each token is a single byte.
struct BytePolicy
{
    static constexpr std::string_view start_token{"\x01", 1};
    static constexpr std::string_view end_token{"\xFF", 1};

    /// Builds `(n - 1)` start bytes, the input, then `(n - 1)` end bytes, and yields every length-`n` window
    /// as a contiguous view into that buffer. Returns the number of n-grams.
    template <typename Visit>
    size_t enumerateNgrams(std::string_view text, UInt32 n, std::string_view start, std::string_view end, NaiveBayesScratch & scratch, Visit && visit) const
    {
        const size_t pad = n - 1;
        auto & buf = scratch.padded;
        buf.resize(pad + text.size() + pad);

        for (size_t i = 0; i < pad; ++i)
            buf[i] = start[0];

        memcpy(buf.data() + pad, text.data(), text.size());

        for (size_t i = 0; i < pad; ++i)
            buf[pad + text.size() + i] = end[0];

        const size_t total = buf.size();
        if (total < n)
            return 0;
        const size_t count = total - n + 1;
        for (size_t i = 0; i < count; ++i)
            visit(std::string_view(buf.data() + i, n));
        return count;
    }

    /// Returns the key to store for `s` and its token count (one per byte). Byte n-grams are stored verbatim:
    /// every byte is significant, so the source key already matches what a query looks up.
    PreparedNgram prepareNgram(std::string_view s, NaiveBayesScratch &) const { return {s, s.size()}; }
};

/// CodePoint-level tokenizer: each token is a single Unicode (UTF-8) code point.
struct CodePointPolicy
{
    // U+10FFFE -> F4 8F BF BE
    static constexpr std::string_view start_token{"\xF4\x8F\xBF\xBE"};
    // U+10FFFF -> F4 8F BF BF
    static constexpr std::string_view end_token{"\xF4\x8F\xBF\xBF"};

    /// Builds `(n - 1)` start tokens, the input, then `(n - 1)` end tokens, records each code point boundary,
    /// and yields every window of `n` consecutive code points as a contiguous view. Returns the number of n-grams.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view text, UInt32 n, std::string_view start, std::string_view end, NaiveBayesScratch & scratch, Visit && visit) const
    {
        const size_t pad = n - 1;
        auto & buf = scratch.padded;
        buf.resize(pad * start.size() + text.size() + pad * end.size());
        size_t pos = 0;
        for (size_t i = 0; i < pad; ++i)
        {
            memcpy(buf.data() + pos, start.data(), start.size());
            pos += start.size();
        }
        memcpy(buf.data() + pos, text.data(), text.size());
        pos += text.size();
        for (size_t i = 0; i < pad; ++i)
        {
            memcpy(buf.data() + pos, end.data(), end.size());
            pos += end.size();
        }

        auto & offsets = scratch.code_point_offsets;
        offsets.clear();
        const size_t total = buf.size();
        size_t at = 0;
        while (at < total)
        {
            offsets.push_back(static_cast<UInt32>(at));
            at += DB::UTF8::seqLength(static_cast<UInt8>(buf[at]));
        }
        offsets.push_back(static_cast<UInt32>(total));

        const size_t code_points = offsets.size() - 1;
        if (code_points < n)
            return 0;
        const size_t count = code_points - n + 1;
        for (size_t i = 0; i < count; ++i)
            visit(std::string_view(buf.data() + offsets[i], offsets[i + n] - offsets[i]));
        return count;
    }

    /// Returns the key to store for `s` and its code-point count. Code-point n-grams are stored verbatim: every
    /// code point is significant, so the source key already matches what a query looks up. The bytes must be
    /// valid UTF-8 first; otherwise the per-code-point lengths below would be meaningless, so it is reported as
    /// invalid for the caller to reject.
    PreparedNgram prepareNgram(std::string_view s, NaiveBayesScratch &) const
    {
        if (!UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size()))
            return {s, 0, false};

        size_t count = 0;
        for (size_t at = 0; at < s.size(); at += DB::UTF8::seqLength(static_cast<UInt8>(s[at])))
            ++count;

        return {s, count};
    }
};

/// Token-level tokenizer: each token is a whitespace-delimited word.
struct TokenPolicy
{
    static constexpr std::string_view start_token{"<s>"};
    static constexpr std::string_view end_token{"</s>"};

    void tokenize(std::string_view text, VectorWithMemoryTracking<std::string_view> & tokens) const
    {
        tokens.reserve(tokens.size() + text.size() / 3);

        size_t pos = 0;
        const char * data = text.data();
        const size_t size = text.size();

        while (pos < size)
        {
            // Skip any leading ASCII whitespace
            while (pos < size && isWhitespaceASCII(data[pos]))
                ++pos;
            if (pos == size)
                break;

            // Find the next whitespace
            size_t start = pos;
            while (pos < size && !isWhitespaceASCII(data[pos]))
                ++pos;

            tokens.emplace_back(data + start, pos - start);
        }
    }

    void join(const std::string_view * start, size_t n, std::string & ngram) const
    {
        chassert(n != 0);
        size_t total = n - 1; // spaces
        for (size_t i = 0; i < n; ++i)
            total += start[i].size();
        ngram.resize(total);

        size_t pos = 0;
        for (size_t i = 0; i < n; ++i)
        {
            if (i)
                ngram[pos++] = ' ';
            memcpy(ngram.data() + pos, start[i].data(), start[i].size());
            pos += start[i].size();
        }
    }

    /// Tokenizes into words, pads with `(n - 1)` start and end tokens, and yields each window of `n` words
    /// joined with single spaces. The token views and the join buffer live in the scratch and keep their
    /// capacity across rows. Returns the number of n-grams.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view text, UInt32 n, std::string_view start, std::string_view end, NaiveBayesScratch & scratch, Visit && visit) const
    {
        auto & tokens = scratch.tokens;
        tokens.clear();
        for (UInt32 i = 0; i + 1 < n; ++i)
            tokens.push_back(start);
        tokenize(text, tokens);
        for (UInt32 i = 0; i + 1 < n; ++i)
            tokens.push_back(end);

        if (tokens.size() < n)
            return 0;
        const size_t count = tokens.size() - n + 1;
        auto & ngram = scratch.ngram_buffer;
        for (size_t i = 0; i < count; ++i)
        {
            join(&tokens[i], n, ngram);
            visit(std::string_view(ngram));
        }
        return count;
    }

    /// Returns the key to store for `s` and its word-token count, in a single pass. The key is the form the
    /// tokenizer emits at query time: word tokens joined by single spaces. When `s` is already in that form
    /// (the common case) it is returned verbatim with no copy; otherwise it is rebuilt in the scratch buffer so
    /// that source n-grams differing only in whitespace (leading, trailing, or repeated spaces, or tabs) fold
    /// onto the key a query produces instead of being stored under an unreachable key. A rebuilt key stays
    /// valid until the next call.
    PreparedNgram prepareNgram(std::string_view s, NaiveBayesScratch & scratch) const
    {
        size_t count = 0;
        bool canonical = true;
        const size_t size = s.size();
        size_t pos = 0;
        while (pos < size)
        {
            if (isWhitespaceASCII(s[pos]))
            {
                /// A separator is canonical only as a single ' ' between two tokens, never leading the string.
                if (count == 0 || s[pos] != ' ')
                    canonical = false;
                const size_t ws_start = pos;
                while (pos < size && isWhitespaceASCII(s[pos]))
                    ++pos;
                if (pos - ws_start > 1 || pos == size)
                    canonical = false;
            }
            else
            {
                ++count;
                while (pos < size && !isWhitespaceASCII(s[pos]))
                    ++pos;
            }
        }

        /// An empty or all-whitespace n-gram (count 0) is left for the caller's arity check to reject, so its
        /// original text appears in the error.
        if (canonical || count == 0)
            return {s, count};

        scratch.tokens.clear();
        tokenize(s, scratch.tokens);
        join(scratch.tokens.data(), scratch.tokens.size(), scratch.ngram_buffer);
        return {std::string_view(scratch.ngram_buffer), count};
    }
};

using ClassCountMap = HashMap<UInt32, UInt64, HashCRC32<UInt32>>;

/// Maps an n-gram to its dense index. StringHashMap packs short keys (the common byte/codepoint n-grams) into
/// integer sub-tables, which keeps the keys dense and reduces probing to integer hashing and comparison.
using NGramIndexMap = StringHashMap<UInt32>;
using ClassIndexMap = HashMap<UInt32, UInt32, HashCRC32<UInt32>>;
using LogProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

/// One observed `(n-gram, class, count)` row, packed so the whole set can be sorted and grouped cheaply during
/// finalize: `key = (n-gram index << 32) | class id`. Keeping every observation in one flat array makes the
/// memory used while loading proportional to the number of source rows.
struct NaiveBayesEntry
{
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

/// Holds all of the state of a Naive Bayes model for one tokenizer policy. It is accumulated by a trainer,
/// then compiled in place into the query-ready compressed-sparse-row (CSR) form, and finally owned by the
/// immutable model. It owns an arena and is therefore neither copyable nor movable (the n-gram keys are views
/// into that arena); it is always held through a smart pointer so the owning trainer/model can move cheaply.
///
/// After compilation, the present `(class, delta)` pairs of the n-gram whose index is `i` occupy
/// `slice_class_index[slice_offsets[i] .. slice_offsets[i + 1])` and the matching `slice_delta` range, where
/// the index is the value stored for the n-gram in `ngram_to_index`. The accumulation buffers (`entries` and
/// `class_totals`) are freed once the CSR arrays are built.
template <Tokenizer Tok>
struct NaiveBayesData
{
    NaiveBayesData(UInt32 n_, double alpha_, Tok tokenizer_)
        : n(n_)
        , alpha(alpha_)
        , start_token(Tok::start_token)
        , end_token(Tok::end_token)
        , tokenizer(std::move(tokenizer_))
    {
    }

    /// N-gram size.
    UInt32 n;
    /// Laplace smoothing parameter.
    double alpha;

    /// Start / end padding tokens.
    String start_token;
    String end_token;
    Tok tokenizer;

    /// Arena owning all the n-gram key bytes that `ngram_to_index` views into.
    Arena pool;
    /// N-gram -> its dense index. The first inserted n-gram gets index 0, the next unique one gets 1, etc.
    NGramIndexMap ngram_to_index;
    /// One row per observation. Sorted and grouped into the CSR arrays at finalize, then freed.
    PODArray<NaiveBayesEntry> entries;
    /// Class -> total count over all n-grams. Used while finalizing.
    ClassCountMap class_totals;

    /// Dense class index -> original class id, ascending.
    PODArray<UInt32> class_id_of;
    /// Dense class index -> log prior probability.
    PODArray<Float64> log_prior;
    /// Dense class index -> contribution of an absent n-gram, `log(alpha) - log(denom[c])`. This and
    /// `slice_delta` are kept as Float32 deliberately: scores accumulate in Float64, so halving the size of
    /// these large arrays outweighs the lost mantissa bits. The reduced precision changes the argmax only when
    /// the top classes are separated by less than the Float32 rounding error, i.e. when they are effectively
    /// tied and the choice between them is arbitrary anyway.
    PODArray<Float32> base;

    /// Compressed-sparse-row representation of the present `(class, delta)` pairs, indexed by n-gram index.
    PODArray<UInt32> slice_offsets;
    PODArray<UInt32> slice_class_index;
    PODArray<Float32> slice_delta;
};

/// An immutable, ready-to-query Naive Bayes model. Everything that does not depend on the input is precomputed
/// at finalize time, so classification is reduced to additions:
///
///     score[c] = log_prior[c] + K * base[c] + sum over present n-grams of delta[n-gram, c]
///
/// where `K` is the number of n-grams in the input (present or absent), `base[c] = log(alpha) - log(denom[c])`
/// is the contribution of an absent n-gram, and `delta[n-gram, c] = log(count + alpha) - log(alpha)` is the
/// extra contribution when the n-gram is present. There is no logarithm or division on the query path: a
/// single hash probe maps an n-gram to its CSR slice, which is then streamed into the per-class scores.
///
/// An instance can only be produced by finalizing a trainer, which guarantees a model is fully built before
/// it can be used.
template <Tokenizer Tok>
class NaiveBayesModel
{
public:
    explicit NaiveBayesModel(std::unique_ptr<NaiveBayesData<Tok>> data_)
        : data(std::move(data_))
    {
    }

    /// Returns the class with the highest log probability. Ties resolve to the smallest class id.
    UInt32 classify(std::string_view input, NaiveBayesScratch & scratch) const
    {
        computeScores(input, scratch);
        return data->class_id_of[argmax(scratch.scores)];
    }

    /// Returns the best class and its normalized probability.
    std::pair<UInt32, double> classifyWithProb(std::string_view input, NaiveBayesScratch & scratch) const
    {
        computeScores(input, scratch);
        softmaxInto(scratch);
        const size_t best = argmax(scratch.scores);
        return {data->class_id_of[best], scratch.probabilities[best].second};
    }

    /// Returns every class with its normalized probability, ordered from most to least probable, with ties
    /// broken by ascending class id so that the order is fully determined. The result is held in `scratch`
    /// and reused across rows, so the returned reference stays valid only until the next classify call on the
    /// same scratch.
    const std::vector<std::pair<UInt32, double>> & classifyWithAllProbs(std::string_view input, NaiveBayesScratch & scratch) const
    {
        computeScores(input, scratch);
        softmaxInto(scratch);
        std::sort(
            scratch.probabilities.begin(),
            scratch.probabilities.end(),
            [](const auto & a, const auto & b) { return a.second != b.second ? a.second > b.second : a.first < b.first; });
        return scratch.probabilities;
    }

    /// Convenience overloads that allocate a private scratch, for callers that classify a single input.
    UInt32 classify(std::string_view input) const
    {
        NaiveBayesScratch scratch;
        return classify(input, scratch);
    }

    std::pair<UInt32, double> classifyWithProb(std::string_view input) const
    {
        NaiveBayesScratch scratch;
        return classifyWithProb(input, scratch);
    }

    std::vector<std::pair<UInt32, double>> classifyWithAllProbs(std::string_view input) const
    {
        NaiveBayesScratch scratch;
        return classifyWithAllProbs(input, scratch);
    }

    UInt64 getAllocatedBytes() const
    {
        UInt64 total = sizeof(*this) + sizeof(NaiveBayesData<Tok>);
        total += data->pool.allocatedBytes();
        total += data->ngram_to_index.getBufferSizeInBytes();
        total += data->class_id_of.allocated_bytes() + data->log_prior.allocated_bytes() + data->base.allocated_bytes();
        total += data->slice_offsets.allocated_bytes() + data->slice_class_index.allocated_bytes() + data->slice_delta.allocated_bytes();
        return total;
    }

    size_t getElementCount() const { return data->ngram_to_index.size(); }

private:
    /// Fills `scratch.scores` (sized to the number of classes) with the unnormalized log-probability of every
    /// class for the given input.
    void computeScores(std::string_view input, NaiveBayesScratch & scratch) const
    {
        const auto & model = *data;
        const size_t num_classes = model.class_id_of.size();

        auto & scores = scratch.scores;
        scores.resize(num_classes);
        for (size_t c = 0; c < num_classes; ++c)
            scores[c] = model.log_prior[c];

        size_t num_ngrams = 0;
        auto accumulate = [&](std::string_view ngram)
        {
            ++num_ngrams;
            auto it = model.ngram_to_index.find(ngram);
            if (!it)
                return;
            const UInt32 index = it->getMapped();
            const UInt32 begin = model.slice_offsets[index];
            const UInt32 end = model.slice_offsets[index + 1];
            for (UInt32 j = begin; j < end; ++j)
                scores[model.slice_class_index[j]] += static_cast<double>(model.slice_delta[j]);
        };
        model.tokenizer.enumerateNgrams(input, model.n, model.start_token, model.end_token, scratch, accumulate);

        for (size_t c = 0; c < num_classes; ++c)
            scores[c] += static_cast<double>(num_ngrams) * static_cast<double>(model.base[c]);
    }

    /// Index of the maximum score; ties resolve to the smallest index, which is the smallest class id.
    static size_t argmax(const PODArray<Float64> & scores)
    {
        size_t best = 0;
        for (size_t c = 1; c < scores.size(); ++c)
            if (scores[c] > scores[best])
                best = c;
        return best;
    }

    /// Converts `scratch.scores` into normalized probabilities in `scratch.probabilities` using the
    /// numerically stable log-sum-exp method, pairing each with its class id.
    void softmaxInto(NaiveBayesScratch & scratch) const
    {
        const size_t num_classes = data->class_id_of.size();
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
            probabilities[c] = {data->class_id_of[c], exp_val};
            sum_exp += exp_val;
        }

        for (auto & entry : probabilities)
            entry.second /= sum_exp;
    }

    std::unique_ptr<NaiveBayesData<Tok>> data;
};

}
