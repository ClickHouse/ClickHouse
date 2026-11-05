#pragma once

#include <Dictionaries/NaiveBayesModel.h>
#include <fmt/ranges.h>
#include <Common/Exception.h>
#include <Common/MapWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int RECEIVED_EMPTY_DATA;
}

/// Accumulates class, n-gram, and count observations, then calls `NaiveBayesModelData::compile` to
/// create an immutable model which is then used for classification.
template <typename Tokenizer>
class NaiveBayesTrainer
{
public:
    NaiveBayesTrainer(UInt32 ngram_size, double alpha_, String start_token, String end_token, Tokenizer tokenizer = {})
        : model_data(
              std::make_unique<NaiveBayesModelData<Tokenizer>>(
                  ngram_size, std::move(start_token), std::move(end_token), std::move(tokenizer)))
        , alpha(alpha_)
    {
    }

    /// Try add one row from training data.
    void addNgram(UInt32 class_id, std::string_view ngram, UInt64 count)
    {
        chassert(model_data);

        /// A zero-count row records no observation, so it does not enter the vocabulary. Zero-count rows are ignored rather
        /// than rejected because some generated sources may emit them.
        if (count == 0)
            return;

        /// Get the compressed dense index for the n-gram. For example: if the first n-gram seen is "the",
        /// it gets index 0; if the second is "is", it gets index 1, and so on.
        const UInt32 index = model_data->getOrAssignNgramIndex(ngram);
        observations.push_back(NaiveBayesEntry(index, class_id, count));

        auto & class_total = class_totals[class_id];
        if (count > std::numeric_limits<UInt64>::max() - class_total)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "NaiveBayes dictionary: the total count for class {} overflows a 64-bit integer", class_id);

        class_total += count;
    }

    /// This normalizes and validates the training data ngrams. For example, for codepoint, it checks whether the ngram is valid
    /// UTF-8 or not. For token mode, it normalizes the ngram by removing extra whitespace. For each mode, it also returns the
    /// number of tokens in the ngram, which we later compare against the configured n and throw on a mismatch.
    PreparedNgram prepareNgram(std::string_view ngram) { return model_data->prepareNgram(ngram, scratch); }

    /// Convert the training data (`observations`, `class_totals`) into an optimized form which is efficient for classification.
    NaiveBayesModel<Tokenizer> finalize(PriorsMode mode, const MapWithMemoryTracking<UInt32, double> & explicit_priors = {})
    {
        chassert(model_data);

        if (model_data->getElementCount() == 0)
            throw Exception(
                ErrorCodes::RECEIVED_EMPTY_DATA,
                "NaiveBayes dictionary: the model has no n-grams; the source is empty or every count is zero");

        ClassLogPriorMap log_class_priors;
        switch (mode)
        {
            case PriorsMode::Uniform: computeUniformPriors(log_class_priors); break;
            case PriorsMode::Proportional: computeProportionalPriors(log_class_priors); break;
            case PriorsMode::Explicit: setExplicitPriors(explicit_priors, log_class_priors); break;
        }

        /// We have collected all the information; now build the optimized model for classification.
        model_data->compile(observations, class_totals, log_class_priors, alpha);

        /// Free the memory as the model for classification is already built.
        observations = PODArray<NaiveBayesEntry>{};
        class_totals = ClassCountMap{};

        return NaiveBayesModel<Tokenizer>(std::move(model_data));
    }

private:
    void computeUniformPriors(ClassLogPriorMap & log_class_priors) const
    {
        chassert(!class_totals.empty());

        const double uniform_log_prob = std::log(1.0 / static_cast<double>(class_totals.size()));
        for (const auto & [class_id, _] : class_totals)
            log_class_priors[class_id] = uniform_log_prob;
    }

    void computeProportionalPriors(ClassLogPriorMap & log_class_priors) const
    {
        UInt64 total = 0;
        for (const auto & [_, count] : class_totals)
        {
            if (count > std::numeric_limits<UInt64>::max() - total)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: the total n-gram count across all classes overflows a 64-bit integer");
            total += count;
        }

        chassert(total > 0);

        for (const auto & [class_id, count] : class_totals)
            log_class_priors[class_id] = std::log(static_cast<double>(count) / static_cast<double>(total));
    }

    void setExplicitPriors(const MapWithMemoryTracking<UInt32, double> & priors, ClassLogPriorMap & log_class_priors) const
    {
        /// Make sure the LAYOUT declared classes of the dictionary are consistent with the classes in the training data.
        if (priors.size() != class_totals.size())
        {
            String priors_text;
            for (const auto & [class_id, prior] : priors)
            {
                if (!priors_text.empty())
                    priors_text += ", ";
                priors_text += fmt::format("{}={}", class_id, prior);
            }

            VectorWithMemoryTracking<UInt32> model_classes;
            model_classes.reserve(class_totals.size());
            for (const auto & entry : class_totals)
                model_classes.push_back(entry.getKey());
            std::sort(model_classes.begin(), model_classes.end());

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "NaiveBayes dictionary: the number of classes in priors ({}) does not match the number of classes in the "
                "model ({}); priors: {}; model classes: {}",
                priors.size(),
                class_totals.size(),
                priors_text,
                fmt::join(model_classes, ", "));
        }

        for (const auto & [class_id, prior] : priors)
        {
            if (!class_totals.contains(class_id))
            {
                VectorWithMemoryTracking<UInt32> available_classes;
                available_classes.reserve(class_totals.size());
                for (const auto & entry : class_totals)
                    available_classes.push_back(entry.getKey());

                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "NaiveBayes dictionary: class {} from priors is not in the model; available classes: {}",
                    class_id,
                    fmt::join(available_classes, ", "));
            }

            /// Priors are validated at parse time to lie in (0, 1] and sum to 1, so each is strictly positive.
            chassert(prior > 0);

            log_class_priors[class_id] = std::log(prior);
        }
    }

    std::unique_ptr<NaiveBayesModelData<Tokenizer>> model_data;

    /// Additive (Lidstone) smoothing parameter.
    double alpha;

    /// Store each row of the training data as a (ngram_index, class_id, count) tuple.
    PODArray<NaiveBayesEntry> observations;

    /// Store the total count of n-grams observed for each class.
    ClassCountMap class_totals;

    /// Reusable buffers to avoid repeated reallocations in `prepareNgram`.
    TokenScratch scratch;
};

}
