#pragma once

#include <cctype>
#include <cmath>
#include <concepts>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <fmt/ranges.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/UTF8Helpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int FILE_DOESNT_EXIST;
extern const int RECEIVED_EMPTY_DATA;
}


template <class T>
concept Tokenizer = requires(
    T tok, std::string_view text, std::vector<std::string_view> & tokens, const std::string_view * start, size_t n, std::string & ngram)
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

    void tokenize(std::string_view text, std::vector<std::string_view> & tokens) const
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

    void tokenize(std::string_view text, std::vector<std::string_view> & tokens) const
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

    void tokenize(std::string_view text, std::vector<std::string_view> & tokens) const
    {
        tokens.reserve(tokens.size() + text.size() / 3);

        size_t pos = 0;
        const char * data = text.data();
        const size_t size = text.size();

        while (pos < size)
        {
            // Skip any leading ASCII whitespace
            while (pos < size && std::isspace(static_cast<unsigned char>(data[pos])))
                ++pos;
            if (pos == size)
                break;

            // Find the next whitespace
            size_t start = pos;
            while (pos < size && !std::isspace(static_cast<unsigned char>(data[pos])))
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
using ClassCountMaps = std::vector<ClassCountMap>;

using NGramIndexMap = HashMap<StringRef, UInt32, StringRefHash>;
using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;
using LogProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

template <Tokenizer Tok>
class NaiveBayesClassifier
{
private:
    /// N-gram size
    const UInt32 n;

    /// Laplace smoothing parameter
    const double alpha;

    /// Start / end tokens to pad the input string - taken from Tok at compile time
    const String start_token;
    const String end_token;

    Tok tokenizer;

    /// Index -> single ngram's class count map
    ClassCountMaps all_ngram_class_counts;

    /// Ngram -> index of the class count map in all_ngram_class_counts
    NGramIndexMap ngram_to_class_count_index;

    /// Class -> total count over all ngrams
    ClassCountMap class_totals;

    /// Class -> prior probability
    LogProbabilityMap log_class_priors;

    /// Vocabulary size is the number of distinct tokens in the model across all classes
    size_t vocabulary_size = 0;

    /// Arena to own all the key strings
    Arena pool;

    static constexpr UInt32 MAX_NGRAM_LENGTH = 1024 * 1024;

public:
    ~NaiveBayesClassifier() = default;

    /// The model at model_path is expected to be serialized lines of: <class_id> <ngram> <count>
    NaiveBayesClassifier(
        const String & model_name,
        const String & model_path,
        ProbabilityMap && priors,
        const UInt32 given_n,
        const double given_alpha,
        Tok given_tokenizer = {})
        : n(given_n)
        , alpha(given_alpha)
        , start_token(Tok::start_token)
        , end_token(Tok::end_token)
        , tokenizer(std::move(given_tokenizer))
    {
        if (!std::filesystem::exists(model_path))
        {
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist for model {}", model_path, model_name);
        }

        DB::ReadBufferFromFile in(model_path);

        String ngram; /// Avoid reallocation
        while (!in.eof())
        {
            UInt32 class_id = 0;
            UInt32 ngram_length = 0;
            UInt32 count = 0;

            DB::readBinary(class_id, in); // read the 4-byte class id

            DB::readBinary(ngram_length, in); // read the 4-byte length of the ngram string

            if (ngram_length > MAX_NGRAM_LENGTH)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Corrupt model {} of model {}: ngram length {} exceeds maximum of {}",
                    model_path,
                    model_name,
                    ngram_length,
                    MAX_NGRAM_LENGTH);

            if (ngram_length == 0)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA, "Corrupt model {} of model {}: ngram length given is 0", model_path, model_name);

            ngram.resize(ngram_length);
            in.readStrict(ngram.data(), ngram_length); // read the ngram bytes

            DB::readBinary(count, in); // read the 4-byte count

            ArenaKeyHolder key_holder{StringRef(ngram.data(), ngram_length), pool};
            NGramIndexMap::LookupResult it;
            bool inserted = false;

            ngram_to_class_count_index.emplace(key_holder, it, inserted);

            if (inserted)
            {
                it->getMapped() = static_cast<UInt32>(all_ngram_class_counts.size());
                all_ngram_class_counts.emplace_back();
            }

            all_ngram_class_counts[it->getMapped()][class_id] += count;
            class_totals[class_id] += count;
        }

        if (ngram_to_class_count_index.empty())
        {
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No ngrams found in the model at {} of model {}", model_path, model_name);
        }

        /// If classes are provided in prior, then all classes present in the model must be present in priors.
        /// If prior is empty, then we assign equal probability to all classes.
        if (priors.empty())
        {
            const double uniform_prob = 1.0 / static_cast<double>(class_totals.size());
            const double uniform_log_prob = std::log(uniform_prob);
            for (const auto & [class_id, _] : class_totals)
                log_class_priors[class_id] = uniform_log_prob;
        }
        else /// Priors are provided
        {
            if (priors.size() != class_totals.size())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Number of classes in priors ({}) does not match the number of classes in the model at ({}) of model {}",
                    priors.size(),
                    class_totals.size(),
                    model_name);
            }
            for (const auto & prior : priors)
            {
                const UInt32 class_id = prior.getKey();
                if (!class_totals.contains(class_id)) /// Class present in config's <priors> not found in model
                {
                    /// class_totals does not have begin() and end() methods; therefore cannot use std::ranges::transform
                    /// Manually build a vector of available classes
                    std::vector<UInt32> available_classes;
                    available_classes.reserve(class_totals.size());
                    for (const auto & class_entry : class_totals)
                        available_classes.push_back(class_entry.getKey());

                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Class {} from <priors> not found in the model at {} of model {}. Available classes: {}",
                        class_id,
                        model_path,
                        model_name,
                        fmt::join(available_classes, ", "));
                }
            }

            for (const auto & [class_id, prior] : priors)
                log_class_priors[class_id] = std::log(prior);
        }

        /// Vocabulary size is the number of distinct tokens
        vocabulary_size = ngram_to_class_count_index.size();
    }

    /// Classify an input string. The function splits the input into tokens (by space)
    ///  and adds (n - 1) start tokens at the front and (n - 1) end tokens at the back.
    /// Then, it creates n-grams and computes the log probabilities for each class.
    /// Finally, it returns the class with the highest log probability.
    UInt32 classify(const String & input) const
    {
        LogProbabilityMap class_log_probabilities;
        for (const auto & [class_id, prior] : log_class_priors)
            class_log_probabilities[class_id] = prior;

        std::vector<std::string_view> tokens;
        tokenizer.tokenize(input, tokens);

        /// Add (n - 1) start tokens at the front and (n - 1) end tokens at the back
        if (n > 1)
        {
            tokens.insert(tokens.begin(), n - 1, std::string_view(start_token));
            tokens.insert(tokens.end(), n - 1, std::string_view(end_token));
        }

        /// Now, create n-grams and calculate probability on the fly; each n-gram will consist of n consecutive tokens
        if (tokens.size() >= n)
        {
            std::string ngram;
            for (size_t i = 0; i + n <= tokens.size(); ++i)
            {
                tokenizer.join(&tokens[i], n, ngram);

                StringRef ngram_ref(ngram);
                const auto * const ref_it = ngram_to_class_count_index.find(ngram_ref);
                const bool token_exists = (ref_it != ngram_to_class_count_index.end());

                const auto * token_class_map = token_exists ? &all_ngram_class_counts[ref_it->getMapped()] : nullptr;

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

    UInt64 getAllocatedBytes() const
    {
        UInt64 total = 0;

        total += pool.allocatedBytes();

        total += ngram_to_class_count_index.getBufferSizeInBytes();
        total += class_totals.getBufferSizeInBytes();
        total += log_class_priors.getBufferSizeInBytes();

        total += all_ngram_class_counts.capacity() * sizeof(ClassCountMap);

        for (const auto & m : all_ngram_class_counts)
            total += m.getBufferSizeInBytes();

        total += sizeof(*this);

        return total;
    }
};

}
