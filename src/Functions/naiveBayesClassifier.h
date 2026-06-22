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
#include <base/sort.h>
#include <fmt/ranges.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/PODArray.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int RECEIVED_EMPTY_DATA;
}


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
};

using ClassCountMap = HashMap<UInt32, UInt64, HashCRC32<UInt32>>;

using NGramIndexMap = HashMap<std::string_view, UInt32, StringViewHash>;
using ClassIndexMap = HashMap<UInt32, UInt32, HashCRC32<UInt32>>;
using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;
using LogProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

/// One observed `(n-gram, class, count)` row, packed so the whole set can be sorted and grouped cheaply during
/// finalize: `key = (n-gram index << 32) | class id`. This flat list replaces a per-n-gram count map, which is
/// what made loading allocate gigabytes (one hash table per distinct n-gram).
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
/// the index is the value stored for the n-gram in `ngram_to_index`. The bulky per-n-gram count maps used
/// during accumulation are freed once the CSR arrays are built.
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
    /// Dense class index -> contribution of an absent n-gram, `log(alpha) - log(denom[c])`.
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
            const auto it = model.ngram_to_index.find(ngram);
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

/// Accumulates class, n-gram, and count observations and then compiles them into an immutable model.
template <Tokenizer Tok>
class NaiveBayesTrainer
{
public:
    NaiveBayesTrainer(UInt32 n, double alpha, Tok tokenizer = {})
        : data(std::make_unique<NaiveBayesData<Tok>>(n, alpha, std::move(tokenizer)))
    {
    }

    /// Adds a single observation of a class, an n-gram, and its count.
    void addNgram(UInt32 class_id, std::string_view ngram, UInt64 count)
    {
        ArenaKeyHolder key_holder{ngram, data->pool};
        NGramIndexMap::LookupResult it = nullptr;
        bool inserted = false;
        data->ngram_to_index.emplace(key_holder, it, inserted);

        if (inserted)
            it->getMapped() = static_cast<UInt32>(data->ngram_to_index.size() - 1);

        const UInt32 index = it->getMapped();
        data->entries.push_back(NaiveBayesEntry{(static_cast<UInt64>(index) << 32) | class_id, count});
        data->class_totals[class_id] += count;
    }

    /// Computes the class priors according to the given mode, compiles the accumulated counts into the flat CSR
    /// arrays (reusing the existing n-gram index and arena), frees the per-n-gram count maps, and returns the
    /// finished model. The explicit priors are consulted, and required, only when the mode is explicit.
    NaiveBayesModel<Tok> finalize(PriorsMode mode, const ProbabilityMap & explicit_priors = {})
    {
        if (data->ngram_to_index.empty())
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No n-grams found in the model");

        LogProbabilityMap log_class_priors;
        switch (mode)
        {
            case PriorsMode::Uniform:
                computeUniformPriors(log_class_priors);
                break;
            case PriorsMode::Proportional:
                computeProportionalPriors(log_class_priors);
                break;
            case PriorsMode::Explicit:
                setExplicitPriors(explicit_priors, log_class_priors);
                break;
        }

        const size_t vocabulary_size = data->ngram_to_index.size();

        /// Index the classes densely in ascending order.
        std::vector<UInt32> classes;
        classes.reserve(data->class_totals.size());
        for (const auto & entry : data->class_totals)
            classes.push_back(entry.getKey());
        std::sort(classes.begin(), classes.end());

        const size_t num_classes = classes.size();
        ClassIndexMap class_to_index;
        data->class_id_of.resize(num_classes);
        data->log_prior.resize(num_classes);
        data->base.resize(num_classes);

        const double log_alpha = std::log(data->alpha);
        const double smoothing = data->alpha * static_cast<double>(vocabulary_size);
        for (size_t c = 0; c < num_classes; ++c)
        {
            const UInt32 class_id = classes[c];
            class_to_index[class_id] = static_cast<UInt32>(c);
            data->class_id_of[c] = class_id;
            data->log_prior[c] = log_class_priors[class_id];
            const double denom = static_cast<double>(data->class_totals[class_id]) + smoothing;
            data->base[c] = static_cast<Float32>(log_alpha - std::log(denom));
        }

        /// Sort the observations by `key = (n-gram index << 32) | class`, then group them into the flat CSR
        /// arrays in a single linear pass. Because the sort orders by n-gram index first, the rows of each
        /// n-gram are contiguous and appear in ascending index order, so `slice_offsets` is filled directly;
        /// equal `(n-gram, class)` rows are adjacent and their counts are summed, which folds duplicate source
        /// rows. The n-gram keys and the arena are left untouched. This replaces a per-n-gram count map and so
        /// avoids allocating one hash table per distinct n-gram during load.
        auto & entries = data->entries;
        ::sort(entries.begin(), entries.end(), [](const NaiveBayesEntry & a, const NaiveBayesEntry & b) { return a.key < b.key; });

        data->slice_offsets.resize(vocabulary_size + 1);
        data->slice_class_index.reserve(entries.size());
        data->slice_delta.reserve(entries.size());

        const size_t num_entries = entries.size();
        size_t i = 0;
        for (size_t index = 0; index < vocabulary_size; ++index)
        {
            data->slice_offsets[index] = static_cast<UInt32>(data->slice_class_index.size());
            while (i < num_entries && static_cast<size_t>(entries[i].key >> 32) == index)
            {
                const UInt64 key = entries[i].key;
                const UInt32 class_id = static_cast<UInt32>(key & 0xFFFFFFFFULL);
                UInt64 summed_count = entries[i].count;
                for (++i; i < num_entries && entries[i].key == key; ++i)
                    summed_count += entries[i].count;
                data->slice_class_index.push_back(class_to_index[class_id]);
                data->slice_delta.push_back(static_cast<Float32>(std::log(static_cast<double>(summed_count) + data->alpha) - log_alpha));
            }
        }
        data->slice_offsets[vocabulary_size] = static_cast<UInt32>(data->slice_class_index.size());

        /// Reclaim the observation buffer now that the CSR arrays hold the same information.
        data->entries = PODArray<NaiveBayesEntry>{};
        data->class_totals = ClassCountMap{};

        return NaiveBayesModel<Tok>(std::move(data));
    }

private:
    void computeUniformPriors(LogProbabilityMap & log_class_priors) const
    {
        const double uniform_log_prob = std::log(1.0 / static_cast<double>(data->class_totals.size()));
        for (const auto & [class_id, _] : data->class_totals)
            log_class_priors[class_id] = uniform_log_prob;
    }

    void computeProportionalPriors(LogProbabilityMap & log_class_priors) const
    {
        UInt64 total = 0;
        for (const auto & [_, count] : data->class_totals)
            total += count;

        for (const auto & [class_id, count] : data->class_totals)
            log_class_priors[class_id] = std::log(static_cast<double>(count) / static_cast<double>(total));
    }

    void setExplicitPriors(const ProbabilityMap & priors, LogProbabilityMap & log_class_priors) const
    {
        /// Every class in the model must have exactly one prior. Together with the requirement that the
        /// number of priors equals the number of classes, this guarantees that no prior refers to a class
        /// that is absent from the model.
        if (priors.size() != data->class_totals.size())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Number of classes in priors ({}) does not match the number of classes in the model ({})",
                priors.size(),
                data->class_totals.size());

        for (const auto & prior : priors)
        {
            const UInt32 class_id = prior.getKey();
            if (!data->class_totals.contains(class_id))
            {
                VectorWithMemoryTracking<UInt32> available_classes;
                available_classes.reserve(data->class_totals.size());
                for (const auto & class_entry : data->class_totals)
                    available_classes.push_back(class_entry.getKey());

                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Class {} from priors not found in the model. Available classes: {}",
                    class_id,
                    fmt::join(available_classes, ", "));
            }
        }

        for (const auto & [class_id, prior] : priors)
            log_class_priors[class_id] = std::log(prior);
    }

    std::unique_ptr<NaiveBayesData<Tok>> data;
};

}
