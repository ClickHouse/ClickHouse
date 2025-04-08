#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <base/types.h>
#include <boost/algorithm/string.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int FILE_DOESNT_EXIST;
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int RECEIVED_EMPTY_DATA;
}

namespace
{

using ClassCountMap = HashMap<UInt32, UInt32, HashCRC32<UInt32>>;
using NGramMap = HashMap<StringRef, ClassCountMap, StringRefHash>;
using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

class NaiveBayesModel
{
private:
    /// N-gram size
    const UInt32 n;

    /// Start and end tokens to pad the input string
    const String start_token;
    const String end_token;

    /// Laplace smoothing parameter
    const double alpha = 1.0;

    NGramMap ngram_counts;
    ClassCountMap class_totals;

    /// Precomputed prior ratios for each class
    ProbabilityMap class_priors;

    /// Vocabulary size is the number of distinct tokens in the model across all classes
    size_t vocabulary_size = 0;

    /// Useful to detect if the model has been loaded before classifying
    bool model_loaded = false;

    /// Arena to own all the key strings
    Arena pool;

    inline StringRef allocateString(const String & s)
    {
        char * pos = pool.alloc(s.size());
        memcpy(pos, s.data(), s.size());
        return StringRef(pos, s.size());
    }

public:
    NaiveBayesModel() = delete;

    /// The model at model_path is expected to be serialized lines of: <class_id> <ngram> <count>
    NaiveBayesModel(
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
            in.readStrict(&ngram[0], ngram_length); // read the ngram bytes

            UInt32 count = 0;
            DB::readBinary(count, in); // read the 4-byte count

            StringRef temp(ngram.data(), ngram.size());
            auto it = ngram_counts.find(temp);

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
                String available_classes = "";
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

        /// Mark the model as loaded
        model_loaded = true;
    }

    /// Classify an input string. The function splits the input into tokens (by space)
    /// and computes a log-probability for each class using Laplace smoothing
    UInt32 classify(const String & input) const
    {
        if (!model_loaded)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Model not loaded. Load the model using loadModel() before classifying.");
        }

        ProbabilityMap class_log_probabilities;
        for (const auto & entry : class_priors)
        {
            class_log_probabilities[entry.getKey()] = std::log(entry.getMapped());
        }

        std::vector<String> tokens;
        boost::split(tokens, input, boost::is_any_of(" "));

        // If n > 1, add (n - 1) start tokens at the front and (n - 1) end tokens at the back
        if (n > 1)
        {
            std::vector<String> padded;
            padded.insert(padded.end(), n - 1, start_token);
            padded.insert(padded.end(), tokens.begin(), tokens.end());
            padded.insert(padded.end(), n - 1, end_token);
            tokens = std::move(padded);
        }

        // Now, create n-grams: each ngram will consist of n consecutive tokens
        std::vector<String> ngrams;
        if (tokens.size() >= n)
        {
            for (size_t i = 0; i <= tokens.size() - n; ++i)
            {
                String ngram;
                for (size_t j = 0; j < n; ++j)
                {
                    if (j > 0)
                        ngram += " ";
                    ngram += tokens[i + j];
                }
                ngrams.push_back(ngram);
            }
        }

        for (const auto & ngram : ngrams)
        {
            StringRef token_ref(ngram);
            bool token_exists = (ngram_counts.find(token_ref) != ngram_counts.end());
            const auto * token_class_map = token_exists ? &ngram_counts.find(token_ref)->getMapped() : nullptr;
            for (const auto & class_entry : class_totals)
            {
                UInt32 class_id = class_entry.getKey();
                double class_total = static_cast<double>(class_entry.getMapped());
                double count = 0.0;
                if (token_exists)
                {
                    auto it = token_class_map->find(class_id);
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

class FunctionNaiveBayesClassifier : public IFunction
{
private:
    ContextPtr context;

    /// Use static cache to ensure model loading happens only once
    static std::map<String, NaiveBayesModel> & getModelCache()
    {
        static std::map<String, NaiveBayesModel> models;
        return models;
    }

public:
    static constexpr auto name = "naiveBayesClassifier";

    explicit FunctionNaiveBayesClassifier(ContextPtr context_)
        : context(context_)
    {
        auto & models = getModelCache();

        if (!models.empty())
            return; // already loaded

        const auto & config = context->getConfigRef();

        if (!config.has("nb_models"))
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing 'nb_models' key in config.");
        }

        /// Iterate over each <model> element in <nb_models>
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("nb_models", keys);
        for (const auto & key : keys)
        {
            const String model_name_path = "nb_models." + key + ".name";
            const String model_data_path = "nb_models." + key + ".path";
            const String model_n_path = "nb_models." + key + ".n";
            if (!config.has(model_name_path))
            {
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model name via 'name' key in <nb_models> for model {}", key);
            }
            if (!config.has(model_data_path))
            {
                throw Exception(
                    ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model data path via 'path' key in <nb_models> for model {}", key);
            }
            if (!config.has(model_n_path))
            {
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model ngram 'n' via 'n' key in <nb_models> for model {}", key);
            }

            const String model_name = config.getString(model_name_path);
            const String model_data = config.getString(model_data_path);
            const UInt32 n = config.getInt(model_n_path);

            if (n == 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngram size 'n' must be greater than 0 for model {}", model_name);
            }

            /// Extract the priors from the config if they exist
            Poco::Util::AbstractConfiguration::Keys prior_keys;

            const String model_priors_path = "nb_models." + key + ".priors";
            config.keys(model_priors_path, prior_keys);

            ProbabilityMap priors;
            for (const auto & prior_key : prior_keys)
            {
                const String model_prior_path = model_priors_path + "." + prior_key;
                if (!config.has(model_prior_path + ".class") or !config.has(model_prior_path + ".value"))
                {
                    throw Exception(
                        ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing 'class' or 'value' key in <priors> for model {}", model_name);
                }
                const UInt32 class_id = config.getInt(model_prior_path + ".class");
                const double prior = config.getDouble(model_prior_path + ".value");
                priors[class_id] = prior;
            }

            const double alpha = config.getDouble("nb_models." + key + ".alpha", 1.0);

            if (alpha <= 0.0)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Laplace smoothing parameter 'alpha' must be greater than 0 for model {}", model_name);
            }

            models.emplace(
                std::piecewise_construct,
                std::make_tuple(model_name),
                std::make_tuple(model_data, std::move(priors), n, alpha, start_token, end_token));
        }

        if (models.empty())
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No models found under <nb_models> in config.");
        }
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifier>(context); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto model_name_input_argument_type = WhichDataType(arguments[0].type);
        if (!model_name_input_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String. Actual {}",
                getName(),
                arguments[0].type->getName());

        const auto input_string_input_argument_type = WhichDataType(arguments[1].type);
        if (!input_string_input_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} second argument type should be String. Actual {}",
                getName(),
                arguments[1].type->getName());

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * input_string_column = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        auto result_column = ColumnUInt32::create();

        const auto & models = getModelCache();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String model_name = "";
            if (const auto * model_name_col_const = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            {
                model_name = model_name_col_const->getValue<String>();
            }
            else
            {
                const auto * model_name_col = checkAndGetColumn<ColumnString>(arguments[0].column.get());
                model_name = model_name_col->getDataAt(i).toString();
            }

            if (models.find(model_name) == models.end())
            {
                String available_models;
                for (const auto & model : models)
                    available_models += model.first + ", ";

                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Model {} not found. Available models: {}", model_name, available_models);
            }

            const auto & model = models.at(model_name);
            String input_string = input_string_column->getDataAt(i).toString();
            UInt32 predicted_class = model.classify(input_string);
            result_column->insert(predicted_class);
        }

        return result_column;
    }
};
}

REGISTER_FUNCTION(NaiveBayesClassifier)
{
    factory.registerFunction<FunctionNaiveBayesClassifier>();
}
}
