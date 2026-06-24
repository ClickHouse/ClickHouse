#pragma once

#include <map>
#include <base/sort.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>
#include <Dictionaries/NaiveBayesModel.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int RECEIVED_EMPTY_DATA;
}

/// Accumulates class, n-gram, and count observations and then compiles them into an immutable model.
template <Tokenizer Tok>
class NaiveBayesTrainer
{
public:
    NaiveBayesTrainer(UInt32 n, double alpha, Tok tokenizer = {})
        : data(std::make_unique<NaiveBayesData<Tok>>(n, alpha, std::move(tokenizer)))
    {
    }

    /// Adds a single observation of a class, an n-gram already in canonical form (see `prepareNgram`), and its
    /// count.
    void addNgram(UInt32 class_id, std::string_view ngram, UInt64 count)
    {
        /// A zero-count row records no observation, so it must not enter the vocabulary: the vocabulary size is
        /// the Laplace smoothing denominator, and counting an unobserved n-gram there would shift every class
        /// score. Its entry would also be equivalent to an absent one (a delta of log(alpha) - log(alpha) = 0),
        /// so dropping it changes nothing for the n-grams that do occur.
        if (count == 0)
            return;

        ArenaKeyHolder key_holder{ngram, data->pool};
        NGramIndexMap::LookupResult it = nullptr;
        bool inserted = false;
        data->ngram_to_index.emplace(key_holder, it, inserted);

        if (inserted)
            it->getMapped() = static_cast<UInt32>(data->ngram_to_index.size() - 1);

        const UInt32 index = it->getMapped();
        data->entries.push_back(NaiveBayesEntry{(static_cast<UInt64>(index) << 32) | class_id, count});

        /// The class total is the largest count sum, so checking it here also guards every per-n-gram sum.
        auto & class_total = data->class_totals[class_id];
        if (count > std::numeric_limits<UInt64>::max() - class_total)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the total count for class {} overflows a 64-bit integer", class_id);
        class_total += count;
    }

    /// Returns the canonical key to store for `ngram`, its token count (for validating against n), and whether
    /// it is well-formed for the tokenizer. The key is built in a reused scratch buffer, so it stays valid only
    /// until the next call.
    PreparedNgram prepareNgram(std::string_view ngram)
    {
        return data->tokenizer.prepareNgram(ngram, canonicalization_scratch);
    }

    /// Computes the class priors according to the given mode, compiles the accumulated counts into the flat CSR
    /// arrays (reusing the existing n-gram index and arena), frees the accumulation buffers (`entries` and
    /// `class_totals`), and returns the finished model. The explicit priors are consulted, and required, only
    /// when the mode is explicit.
    NaiveBayesModel<Tok> finalize(PriorsMode mode, const std::map<UInt32, double> & explicit_priors = {})
    {
        if (data->ngram_to_index.empty())
            throw Exception(
                ErrorCodes::RECEIVED_EMPTY_DATA,
                "NaiveBayes dictionary: the model has no n-grams; the source is empty or every count is zero");

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
        if (!std::isfinite(smoothing))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: alpha is too large; the smoothing term (alpha * vocabulary size) is not finite");
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
        /// rows. The n-gram keys and the arena are left untouched.
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
        /// Every class total is positive, because zero-count rows are dropped in addNgram, so their sum is too;
        /// it only needs guarding against 64-bit overflow.
        UInt64 total = 0;
        for (const auto & [_, count] : data->class_totals)
        {
            if (count > std::numeric_limits<UInt64>::max() - total)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: the total n-gram count across all classes overflows a 64-bit integer");
            total += count;
        }

        for (const auto & [class_id, count] : data->class_totals)
            log_class_priors[class_id] = std::log(static_cast<double>(count) / static_cast<double>(total));
    }

    void setExplicitPriors(const std::map<UInt32, double> & priors, LogProbabilityMap & log_class_priors) const
    {
        /// Every class in the model must have exactly one prior. Together with the requirement that the
        /// number of priors equals the number of classes, this guarantees that no prior refers to a class
        /// that is absent from the model.
        if (priors.size() != data->class_totals.size())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the number of classes in priors ({}) does not match the number of classes in the model ({})",
                priors.size(),
                data->class_totals.size());

        for (const auto & [class_id, prior] : priors)
        {
            if (!data->class_totals.contains(class_id))
            {
                VectorWithMemoryTracking<UInt32> available_classes;
                available_classes.reserve(data->class_totals.size());
                for (const auto & class_entry : data->class_totals)
                    available_classes.push_back(class_entry.getKey());

                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: class {} from priors is not in the model; available classes: {}",
                    class_id,
                    fmt::join(available_classes, ", "));
            }

            log_class_priors[class_id] = std::log(prior);
        }
    }

    std::unique_ptr<NaiveBayesData<Tok>> data;

    /// Reused buffers for rewriting each source n-gram into its canonical form during accumulation.
    NaiveBayesScratch canonicalization_scratch;
};

}
