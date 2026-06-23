#pragma once

#include <algorithm>
#include <cmath>
#include <concepts>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <base/StringViewHash.h>
#include <base/defines.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Tests whether a character is ASCII whitespace. This deliberately avoids `std::isspace`, whose behaviour
/// is locale-dependent and undefined for argument values that are not representable as `unsigned char`.
inline bool isAsciiWhitespace(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f';
}

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
    /// The per-class score accumulator, indexed by dense class index.
    PODArray<Float64> scores;
    /// Reused output for the probability-returning entry points, indexed by dense class index.
    std::vector<std::pair<UInt32, double>> probabilities;
};

/// A tokenizer policy turns an input string into the n-grams to look up. Each policy enumerates the n-grams
/// of an input through `enumerateNgrams`, which invokes a callback once per n-gram and returns their total
/// count. The start and end tokens pad the input so that the leading and trailing n-grams carry positional
/// information, exactly as the model was trained.
template <class T>
concept Tokenizer = requires
{
    { T::start_token } -> std::convertible_to<std::string_view>;
    { T::end_token } -> std::convertible_to<std::string_view>;
};

/// Byte-level tokenizer: each token is a single byte.
struct BytePolicy
{
    static constexpr std::string_view start_token{"\x01", 1};
    static constexpr std::string_view end_token{"\xFF", 1};

    /// Builds `(n - 1)` start bytes, the input, then `(n - 1)` end bytes, and yields every length-`n` window
    /// as a contiguous view into that buffer. Returns the number of n-grams.
    template <typename Visit>
    size_t enumerateNgrams(
        std::string_view text, UInt32 n, std::string_view start, std::string_view end, NaiveBayesScratch & scratch, Visit && visit) const
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

    /// Number of byte tokens in `s` (one per byte). Used to check a model against the configured n.
    size_t tokenCount(std::string_view s) const { return s.size(); }
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

    /// Number of code-point tokens in `s`. Used to check a model against the configured n.
    size_t tokenCount(std::string_view s) const
    {
        size_t count = 0;
        for (size_t at = 0; at < s.size(); at += DB::UTF8::seqLength(static_cast<UInt8>(s[at])))
            ++count;
        return count;
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
            while (pos < size && isAsciiWhitespace(data[pos]))
                ++pos;
            if (pos == size)
                break;

            // Find the next whitespace
            size_t start = pos;
            while (pos < size && !isAsciiWhitespace(data[pos]))
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

    /// Number of whitespace-delimited word tokens in `s`. Used to check a model against the configured n.
    size_t tokenCount(std::string_view s) const
    {
        size_t count = 0;
        size_t pos = 0;
        while (pos < s.size())
        {
            while (pos < s.size() && isAsciiWhitespace(s[pos]))
                ++pos;
            if (pos == s.size())
                break;
            ++count;
            while (pos < s.size() && !isAsciiWhitespace(s[pos]))
                ++pos;
        }
        return count;
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
    Uniform,        /// Equal probability for every class.
    Proportional,   /// Probability proportional to each class's total n-gram count.
    Explicit,       /// Probabilities given explicitly per class.
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
    /// Start / end padding tokens, kept so the query path pads exactly as the model was built.
    String start_token;
    String end_token;
    Tok tokenizer;

    /// Arena owning all the n-gram key bytes that `ngram_to_index` views into.
    Arena pool;
    /// N-gram -> its dense index (also the row index of the CSR arrays).
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
    /// these large arrays outweighs the lost mantissa bits, which do not change the argmax.
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

    /// Computes every class's normalized probability, leaving them in `scratch.probabilities` sorted by
    /// probability descending and then by class id ascending so that the order is fully determined.
    void classifyAllProbs(std::string_view input, NaiveBayesScratch & scratch) const
    {
        computeScores(input, scratch);
        softmaxInto(scratch);
        std::sort(
            scratch.probabilities.begin(),
            scratch.probabilities.end(),
            [](const auto & a, const auto & b) { return a.second != b.second ? a.second > b.second : a.first < b.first; });
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

    std::vector<std::pair<UInt32, double>> classifyAllProbs(std::string_view input) const
    {
        NaiveBayesScratch scratch;
        classifyAllProbs(input, scratch);
        return {scratch.probabilities.begin(), scratch.probabilities.end()};
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
