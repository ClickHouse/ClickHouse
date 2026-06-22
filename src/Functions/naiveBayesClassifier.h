#pragma once

#include <cmath>
#include <concepts>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>
#include <base/StringViewHash.h>
#include <base/defines.h>
#include <fmt/ranges.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/UTF8Helpers.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Out-of-line wrapper around `std::log`. At `-march=x86-64-v3` with LTO clang inlines the libc
/// `log` polynomial (9× `vfmadd` + supporting math) into the per-row classify hot loop, expanding
/// the function by ~10% and pushing the inner `class_totals` HashMap iteration to a less
/// favourable code layout (~2× more samples on the hot bucket-skipping loop).  Keeping the log
/// call out-of-line preserves the master codegen pattern and avoids the regression.
NO_INLINE inline double logNoInline(double x) noexcept
{
    return std::log(x);
}

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

template <class T>
concept Tokenizer = requires(
    T tok, std::string_view text, VectorWithMemoryTracking<std::string_view> & tokens, const std::string_view * start, size_t n, std::string & ngram)
{
    { T::start_token } -> std::convertible_to<std::string_view>;
    { T::end_token } -> std::convertible_to<std::string_view>;
    { tok.tokenize(text, tokens) } -> std::same_as<void>;
    { tok.join(start, n, ngram) } -> std::same_as<void>;
};

/// Byte-level tokenizer: each token is a single byte
struct BytePolicy
{
    static constexpr std::string_view start_token{"\x01", 1};
    static constexpr std::string_view end_token{"\xFF", 1};

    void tokenize(std::string_view text, VectorWithMemoryTracking<std::string_view> & tokens) const
    {
        tokens.reserve(tokens.size() + text.size());
        for (size_t i = 0; i < text.size(); ++i)
            tokens.emplace_back(text.data() + i, 1);
    }

    void join(const std::string_view * start, size_t n, std::string & ngram) const
    {
        chassert(n != 0);
        ngram.resize(n);
        for (size_t i = 0; i < n; ++i)
            ngram[i] = (*start++)[0];
    }
};

/// CodePoint-level tokenizer: each token corresponds to a single Unicode (UTF-8) code point
struct CodePointPolicy
{
    // U+10FFFE -> F4 8F BF BE
    static constexpr std::string_view start_token{"\xF4\x8F\xBF\xBE"};
    // U+10FFFF -> F4 8F BF BF
    static constexpr std::string_view end_token{"\xF4\x8F\xBF\xBF"};

    void tokenize(std::string_view text, VectorWithMemoryTracking<std::string_view> & tokens) const
    {
        tokens.reserve(tokens.size() + text.size());
        size_t pos = 0;
        while (pos < text.size())
        {
            size_t len = DB::UTF8::seqLength(static_cast<UInt8>(text[pos]));
            tokens.emplace_back(text.data() + pos, len);
            pos += len;
        }
    }

    void join(const std::string_view * start, size_t n, std::string & ngram) const
    {
        chassert(n != 0);
        size_t total = 0;
        for (size_t i = 0; i < n; ++i)
            total += start[i].size();
        ngram.resize(total);

        size_t pos = 0;
        for (size_t i = 0; i < n; ++i)
        {
            memcpy(ngram.data() + pos, start[i].data(), start[i].size());
            pos += start[i].size();
        }
    }
};

// Token-level tokenizer: each token corresponds to a space-separated word
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
};

using ClassCountMap = HashMap<UInt32, UInt64, HashCRC32<UInt32>>;
using ClassCountMaps = VectorWithMemoryTracking<ClassCountMap>;

using NGramIndexMap = HashMap<std::string_view, UInt32, StringViewHash>;
using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;
using LogProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

/// How class prior probabilities are determined when finalizing a model.
enum class PriorsMode : uint8_t
{
    Uniform,        /// Equal probability for every class.
    Proportional,   /// Probability proportional to each class's total n-gram count.
    Explicit,       /// Probabilities given explicitly per class.
};

/// Holds all of the trained state of a Naive Bayes model for one tokenizer policy. It owns an arena and
/// is therefore neither copyable nor movable, because the n-gram keys stored in the index are views into
/// that arena and the object must keep the same address. It is always allocated on the heap and referenced
/// through a smart pointer, which lets the owning model be moved cheaply.
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

    /// N-gram size
    UInt32 n;

    /// Laplace smoothing parameter
    double alpha;

    /// Start / end tokens to pad the input string - taken from Tok at compile time
    String start_token;
    String end_token;

    Tok tokenizer;

    /// Index -> single ngram's class count map
    ClassCountMaps all_ngram_class_counts;

    /// Ngram -> index of the class count map in all_ngram_class_counts
    NGramIndexMap ngram_to_class_count_index;

    /// Class -> total count over all ngrams
    ClassCountMap class_totals;

    /// Class -> log prior probability
    LogProbabilityMap log_class_priors;

    /// Vocabulary size is the number of distinct n-grams in the model across all classes
    size_t vocabulary_size = 0;

    /// Arena to own all the key strings
    Arena pool;
};

/// An immutable, ready-to-query Naive Bayes model. An instance can only be produced by finalizing a
/// trainer, which guarantees that a model is always fully built before it can be used for classification.
template <Tokenizer Tok>
class NaiveBayesModel
{
public:
    explicit NaiveBayesModel(std::unique_ptr<NaiveBayesData<Tok>> data_)
        : data(std::move(data_))
    {
    }

    /// Returns the class with the highest log probability.
    UInt32 classify(std::string_view input) const
    {
        const auto log_probs = computeLogProbabilities(input);

        UInt32 best_class = 0;
        double max_log_prob = -std::numeric_limits<double>::infinity();
        for (const auto & entry : log_probs)
        {
            if (entry.getMapped() > max_log_prob)
            {
                max_log_prob = entry.getMapped();
                best_class = entry.getKey();
            }
        }

        return best_class;
    }

    /// Returns the best class and its normalized probability.
    std::pair<UInt32, double> classifyWithProb(std::string_view input) const
    {
        const auto probs = softmax(computeLogProbabilities(input));

        UInt32 best_class = 0;
        double best_prob = -1.0;
        for (const auto & [class_id, prob] : probs)
        {
            if (prob > best_prob)
            {
                best_prob = prob;
                best_class = class_id;
            }
        }

        return {best_class, best_prob};
    }

    /// Returns all classes with their normalized probabilities, sorted by probability descending.
    std::vector<std::pair<UInt32, double>> classifyAllProbs(std::string_view input) const
    {
        auto result = softmax(computeLogProbabilities(input));
        std::sort(result.begin(), result.end(), [](const auto & a, const auto & b) { return a.second > b.second; });
        return result;
    }

    UInt64 getAllocatedBytes() const
    {
        UInt64 total = sizeof(*this) + sizeof(NaiveBayesData<Tok>);

        total += data->pool.allocatedBytes();

        total += data->ngram_to_class_count_index.getBufferSizeInBytes();
        total += data->class_totals.getBufferSizeInBytes();
        total += data->log_class_priors.getBufferSizeInBytes();

        total += data->all_ngram_class_counts.capacity() * sizeof(ClassCountMap);

        for (const auto & m : data->all_ngram_class_counts)
            total += m.getBufferSizeInBytes();

        return total;
    }

    size_t getElementCount() const { return data->ngram_to_class_count_index.size(); }

private:
    /// Computes the unnormalized log-probability of every class for the given input.
    LogProbabilityMap computeLogProbabilities(std::string_view input) const
    {
        LogProbabilityMap class_log_probabilities;
        for (const auto & [class_id, prior] : data->log_class_priors)
            class_log_probabilities[class_id] = prior;

        VectorWithMemoryTracking<std::string_view> tokens;
        data->tokenizer.tokenize(input, tokens);

        if (data->n > 1)
        {
            tokens.insert(tokens.begin(), data->n - 1, std::string_view(data->start_token));
            tokens.insert(tokens.end(), data->n - 1, std::string_view(data->end_token));
        }

        if (tokens.size() >= data->n)
        {
            std::string ngram;
            for (size_t i = 0; i + data->n <= tokens.size(); ++i)
            {
                data->tokenizer.join(&tokens[i], data->n, ngram);

                const auto * const ref_it = data->ngram_to_class_count_index.find(std::string_view(ngram));
                const bool token_exists = (ref_it != data->ngram_to_class_count_index.end());

                const auto * token_class_map = token_exists ? &data->all_ngram_class_counts[ref_it->getMapped()] : nullptr;

                for (const auto & class_entry : data->class_totals)
                {
                    UInt32 class_id = class_entry.getKey();
                    double class_total = static_cast<double>(class_entry.getMapped());
                    double count = 0.0;
                    if (token_exists)
                    {
                        const auto * it = token_class_map->find(class_id);
                        if (it != token_class_map->end())
                            count = static_cast<double>(it->getMapped());
                    }
                    const double probability = (count + data->alpha) / (class_total + data->alpha * static_cast<double>(data->vocabulary_size));
                    class_log_probabilities[class_id] += logNoInline(probability);
                }
            }
        }

        return class_log_probabilities;
    }

    /// Converts log-probabilities into normalized probabilities using the numerically stable log-sum-exp method.
    static std::vector<std::pair<UInt32, double>> softmax(const LogProbabilityMap & log_probs)
    {
        double max_log = -std::numeric_limits<double>::infinity();
        for (const auto & entry : log_probs)
            max_log = std::max(max_log, entry.getMapped());

        double sum_exp = 0.0;
        std::vector<std::pair<UInt32, double>> result;
        result.reserve(log_probs.size());
        for (const auto & [class_id, log_prob] : log_probs)
        {
            double exp_val = std::exp(log_prob - max_log);
            result.emplace_back(class_id, exp_val);
            sum_exp += exp_val;
        }

        for (auto & [_, prob] : result)
            prob /= sum_exp;

        return result;
    }

    std::unique_ptr<NaiveBayesData<Tok>> data;
};

/// Accumulates class, n-gram, and count observations and then produces an immutable model.
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

        data->ngram_to_class_count_index.emplace(key_holder, it, inserted);

        if (inserted)
        {
            it->getMapped() = static_cast<UInt32>(data->all_ngram_class_counts.size());
            data->all_ngram_class_counts.emplace_back();
        }

        data->all_ngram_class_counts[it->getMapped()][class_id] += count;
        data->class_totals[class_id] += count;
    }

    /// Computes the class priors according to the given mode and returns the finished model.
    /// The explicit priors are consulted, and required, only when the mode is explicit.
    NaiveBayesModel<Tok> finalize(PriorsMode mode, const ProbabilityMap & explicit_priors = {})
    {
        if (data->ngram_to_class_count_index.empty())
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No n-grams found in the model");

        switch (mode)
        {
            case PriorsMode::Uniform:
                computeUniformPriors();
                break;
            case PriorsMode::Proportional:
                computeProportionalPriors();
                break;
            case PriorsMode::Explicit:
                setExplicitPriors(explicit_priors);
                break;
        }

        data->vocabulary_size = data->ngram_to_class_count_index.size();
        return NaiveBayesModel<Tok>(std::move(data));
    }

private:
    void computeUniformPriors()
    {
        const double uniform_log_prob = std::log(1.0 / static_cast<double>(data->class_totals.size()));
        for (const auto & [class_id, _] : data->class_totals)
            data->log_class_priors[class_id] = uniform_log_prob;
    }

    void computeProportionalPriors()
    {
        UInt64 total = 0;
        for (const auto & [_, count] : data->class_totals)
            total += count;

        for (const auto & [class_id, count] : data->class_totals)
            data->log_class_priors[class_id] = std::log(static_cast<double>(count) / static_cast<double>(total));
    }

    void setExplicitPriors(const ProbabilityMap & priors)
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
            data->log_class_priors[class_id] = std::log(prior);
    }

    std::unique_ptr<NaiveBayesData<Tok>> data;
};

}
