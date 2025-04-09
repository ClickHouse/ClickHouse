#pragma once

#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int LOGICAL_ERROR;
extern const int RECEIVED_EMPTY_DATA;
}

using ClassCountMap = HashMap<UInt32, UInt32, HashCRC32<UInt32>>;
using NGramMap = HashMap<StringRef, ClassCountMap, StringRefHash>;
using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

class NaiveBayesClassifier
{
private:
    /// N-gram size
    const UInt32 n;

    /// Laplace smoothing parameter
    const double alpha;

    /// Start and end tokens to pad the input string
    const String start_token;
    const String end_token;

    NGramMap ngram_counts;
    ClassCountMap class_totals;

    /// Precomputed prior ratios for each class
    ProbabilityMap class_priors;

    /// Vocabulary size is the number of distinct tokens in the model across all classes
    size_t vocabulary_size = 0;

    /// Arena to own all the key strings
    Arena pool;

    inline StringRef allocateString(const String & s)
    {
        char * pos = pool.alloc(s.size());
        memcpy(pos, s.data(), s.size());
        return StringRef(pos, s.size());
    }

public:
    NaiveBayesClassifier() = delete;

    /// The model at model_path is expected to be serialized lines of: <class_id> <ngram> <count>
    NaiveBayesClassifier(
        const String & model_path,
        ProbabilityMap && priors,
        const UInt32 given_n,
        const double given_alpha,
        const String & given_start_token,
        const String & given_end_token)
        : n(given_n)
        , alpha(given_alpha)
        , start_token(given_start_token)
        , end_token(given_end_token)
    {
        if (!std::filesystem::exists(model_path))
        {
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", model_path);
        }

        DB::ReadBufferFromFile in(model_path);

        while (!in.eof())
        {
            UInt32 class_id = 0;
            DB::readBinary(class_id, in); // read the 4-byte class id

            UInt32 ngram_length = 0;
            DB::readBinary(ngram_length, in); // read the 4-byte length of the ngram string

            String ngram;
            ngram.resize(ngram_length);
            in.readStrict(ngram.data(), ngram_length); // read the ngram bytes

            UInt32 count = 0;
            DB::readBinary(count, in); // read the 4-byte count

            StringRef temp(ngram.data(), ngram.size());
            auto * it = ngram_counts.find(temp);

            if (it == ngram_counts.end())
            {
                /// The key is not present: allocate the string in the arena
                StringRef key = allocateString(ngram);
                typename NGramMap::LookupResult insert_it;
                bool inserted;
                ngram_counts.emplace(key, insert_it, inserted);
                if (inserted)
                {
                    it = insert_it;
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to insert ngram {} into the map.", ngram);
                }
            }

            it->getMapped()[class_id] += count;
            class_totals[class_id] += count;
        }

        if (ngram_counts.empty())
        {
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No ngrams found in the model file {}", model_path);
        }

        /// Make sure that the classes provided in priors are present in the model
        for (const auto & prior : priors)
        {
            const UInt32 class_id = prior.getKey();
            if (class_totals.find(class_id) == class_totals.end()) /// Class present in config's <priors> not found in model
            {
                String available_classes;
                for (const auto & class_entry : class_totals)
                {
                    available_classes += std::to_string(class_entry.getKey()) + ", ";
                }
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Class {} from <priors> not found in the model at {}. Available classes: {}",
                    class_id,
                    model_path,
                    available_classes);
            }
        }

        /// The following handles the case where the user provides priors for some classes but not all.
        /// After assigning the provided priors, we distribute the remaining probability equally among the classes
        /// that were not provided in the priors map (i.e., the classes that are present in the model but not in the
        /// config's <priors>)
        double provided_total_prior_prob = 0.0;
        for (const auto & prior : priors)
        {
            const double class_prior_prob = prior.getMapped();
            provided_total_prior_prob += class_prior_prob;
        }

        /// Sanity check: the sum of provided priors should not exceed 1.0
        if (provided_total_prior_prob - 1.0 > 1e-6)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Sum of provided priors probability exceeds 1.0. Sum: {}", provided_total_prior_prob);
        }

        size_t missing_count = 0;
        for (const auto & class_entry : class_totals)
        {
            UInt32 class_id = class_entry.getKey();
            if (priors.find(class_id) == priors.end())
                ++missing_count;
        }

        const double remaining_probability = 1.0 - provided_total_prior_prob;

        /// Precompute the class priors
        for (const auto & class_entry : class_totals)
        {
            UInt32 class_id = class_entry.getKey();
            if (priors.find(class_id) != priors.end()) /// Class from model present in config's <priors>
            {
                class_priors[class_entry.getKey()] = priors.at(class_id);
            }
            else
            {
                double equal_share = missing_count > 0 ? remaining_probability / missing_count : 0.0;
                class_priors[class_entry.getKey()] = equal_share;
            }
        }

        /// Vocabulary size is the number of distinct tokens
        vocabulary_size = ngram_counts.size();
    }

    /// Classify an input string. The function splits the input into tokens (by space)
    /// and adds (n - 1) start tokens at the front and (n - 1) end tokens at the back.
    /// Then, it creates n-grams and computes the log probabilities for each class.
    /// Finally, it returns the class with the highest log probability.
    UInt32 classify(const String & input) const
    {
        ProbabilityMap class_log_probabilities;
        for (const auto & entry : class_priors)
        {
            class_log_probabilities[entry.getKey()] = std::log(entry.getMapped());
        }

        std::vector<String> tokens;
        boost::split(tokens, input, boost::is_any_of(" "));

        /// Add (n - 1) start tokens at the front and (n - 1) end tokens at the back
        if (n > 1)
        {
            std::vector<String> padded;
            padded.reserve(tokens.size() + (n - 1) * 2);
            padded.insert(padded.end(), n - 1, start_token);
            padded.insert(padded.end(), tokens.begin(), tokens.end());
            padded.insert(padded.end(), n - 1, end_token);
            tokens = std::move(padded);
        }

        /// Now, create n-grams: each ngram will consist of n consecutive tokens
        std::vector<String> ngrams;
        if (tokens.size() >= n)
        {
            ngrams.reserve(tokens.size() - n + 1);
            for (size_t i = 0; i <= tokens.size() - n; ++i)
            {
                // Pre-calculate the final length of the ngram
                size_t total_length = 0;
                for (size_t j = i; j < i + n; ++j)
                {
                    total_length += tokens[j].size();
                }
                total_length += (n - 1); // spaces between tokens

                String ngram;
                ngram.resize(total_length);
                size_t pos = 0;
                for (size_t j = i; j < i + n; ++j)
                {
                    if (j > i)
                    {
                        ngram[pos++] = ' ';
                    }
                    const String & token = tokens[j];
                    memcpy(&ngram[pos], token.data(), token.size());
                    pos += token.size();
                }
                ngrams.push_back(std::move(ngram));
            }
        }

        for (const auto & ngram : ngrams)
        {
            StringRef ngram_ref(ngram);
            const auto * ref_it = ngram_counts.find(ngram_ref);
            bool token_exists = (ref_it != ngram_counts.end());
            const auto * token_class_map = token_exists ? &ref_it->getMapped() : nullptr;
            for (const auto & class_entry : class_totals)
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
                const double probability = (count + alpha) / (class_total + alpha * vocabulary_size);
                class_log_probabilities[class_id] += std::log(probability);
            }
        }

        /// Find the class with the highest log probability
        UInt32 best_class = 0;
        double max_log_prob = -std::numeric_limits<double>::infinity();
        for (const auto & entry : class_log_probabilities)
        {
            if (entry.getMapped() > max_log_prob)
            {
                max_log_prob = entry.getMapped();
                best_class = entry.getKey();
            }
        }

        return best_class;
    }
};

}
