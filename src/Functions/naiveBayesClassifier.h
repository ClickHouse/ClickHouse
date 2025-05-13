#pragma once

#include <concepts>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <vector>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/UTF8Helpers.h>

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
        tokens.reserve(tokens.size() + (text.size() / 3));
        size_t pos = 0;
        while (pos < text.size())
        {
            // skip leading spaces
            size_t start = pos;
            while (start < text.size() && text[start] == ' ')
                ++start;
            if (start == text.size())
                break;

            // find next space
            size_t end = start;
            while (end < text.size() && text[end] != ' ')
                ++end;

            tokens.emplace_back(text.data() + start, end - start);
            pos = end;
        }
    }

    void join(const std::string_view * start, size_t n, std::string & ngram) const
    {
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
    NaiveBayesClassifier(const NaiveBayesClassifier &) = delete;
    NaiveBayesClassifier & operator=(const NaiveBayesClassifier &) = delete;

    NaiveBayesClassifier(NaiveBayesClassifier &&) noexcept = default;
    NaiveBayesClassifier & operator=(NaiveBayesClassifier &&) noexcept = default;

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
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to insert ngram {} into the map for model {}", ngram, model_name);
                }
            }

            it->getMapped()[class_id] += count;
            class_totals[class_id] += count;
        }

        if (ngram_counts.empty())
        {
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No ngrams found in the model at {} of model {}", model_path, model_name);
        }

        /// If classes are provided in prior, then all classes present in the model must be present in priors.
        /// If prior is empty, then we assign equal probability to all classes.
        if (priors.empty())
        {
            for (const auto & class_entry : class_totals)
            {
                UInt32 class_id = class_entry.getKey();
                class_priors[class_id] = 1.0 / class_totals.size();
            }
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
                if (class_totals.find(class_id) == class_totals.end()) /// Class present in config's <priors> not found in model
                {
                    String available_classes;
                    for (const auto & class_entry : class_totals)
                    {
                        available_classes += std::to_string(class_entry.getKey()) + ", ";
                    }
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Class {} from <priors> not found in the model at {} of model {}. Available classes: {}",
                        class_id,
                        model_path,
                        model_name,
                        available_classes);
                }
            }

            for (const auto & prior : priors)
            {
                const UInt32 class_id = prior.getKey();
                class_priors[class_id] = prior.getMapped();
            }
        }

        /// Vocabulary size is the number of distinct tokens
        vocabulary_size = ngram_counts.size();
    }

    /// Classify an input string. The function splits the input into tokens (by space)
    ///  and adds (n - 1) start tokens at the front and (n - 1) end tokens at the back.
    /// Then, it creates n-grams and computes the log probabilities for each class.
    /// Finally, it returns the class with the highest log probability.
    UInt32 classify(const String & input) const
    {
        ProbabilityMap class_log_probabilities;
        for (const auto & entry : class_priors)
        {
            class_log_probabilities[entry.getKey()] = std::log(entry.getMapped());
        }

        std::vector<std::string_view> tokens;
        tokenizer.tokenize(input, tokens);

        /// Add (n - 1) start tokens at the front and (n - 1) end tokens at the back
        if (n > 1)
        {
            tokens.insert(tokens.begin(), n - 1, std::string_view(start_token));
            tokens.insert(tokens.end(), n - 1, std::string_view(end_token));
        }

        /// Now, create n-grams and calculate probability on the fly; each ngram will consist of n consecutive tokens
        if (tokens.size() >= n)
        {
            std::string ngram;
            for (size_t i = 0; i + n <= tokens.size(); ++i)
            {
                tokenizer.join(&tokens[i], n, ngram);

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
        UInt64 total = pool.allocatedBytes();
        total += ngram_counts.getBufferSizeInBytes();
        total += class_totals.getBufferSizeInBytes();
        for (const auto & entry : ngram_counts)
        {
            total += entry.getMapped().getBufferSizeInBytes();
        }
        total += sizeof(*this);
        return total;
    }
};

}
