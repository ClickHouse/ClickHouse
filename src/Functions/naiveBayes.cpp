#include <Columns/ColumnVector.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/StringHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ITokenExtractor.h>
#include <boost/algorithm/string.hpp>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>

#include <base/types.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>

#include <Common/Arena.h>

#include <fstream>
#include <iostream>
#include <vector>

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

using ClassCountMap = HashMap<StringRef, UInt32, StringRefHash>;
using NGramMap = HashMap<StringRef, ClassCountMap, StringRefHash>;
using ProbabilityMap = HashMap<StringRef, double, StringRefHash>;

class NaiveBayesModel
{
private:
    // Laplace smoothing parameter
    const double alpha = 1.0;

    NGramMap ngram_counts;
    ClassCountMap class_totals;

    // Precomputed prior ratios for each class
    ProbabilityMap class_priors;

    // Vocabulary size is the number of distinct tokens in the model across all classes
    size_t vocabulary_size = 0;

    // Useful to detect if the model has been loaded before classifying
    bool model_loaded = false;

    // Arena to own all the key strings
    Arena pool;

    inline StringRef allocateString(const String & s)
    {
        char * pos = pool.alloc(s.size());
        memcpy(pos, s.data(), s.size());
        return StringRef(pos, s.size());
    }

public:
    // Loads the model from file and the provided prior probability for each class. The file is expected to have lines like:
    // <ngram> <class_label> <count>
    void loadModel(const String & file_path, std::map<String, double> priors)
    {
        if (!std::filesystem::exists(file_path))
        {
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", file_path);
        }

        DB::ReadBufferFromFile in(file_path);

        in.ignore();

        while (!in.eof())
        {
            String ngram, class_label;
            int count = 0;

            DB::readStringUntilWhitespace(ngram, in);
            in.ignore();

            if (ngram.empty())
                break;

            DB::readStringUntilWhitespace(class_label, in);
            in.ignore();

            DB::readIntText(count, in);
            in.ignore();

            StringRef ngram_ref = allocateString(ngram);
            StringRef class_label_ref = allocateString(class_label);

            auto & class_map = ngram_counts[ngram_ref];
            class_map[class_label_ref] += count;

            class_totals[class_label_ref] += count;
        }

        if (ngram_counts.empty())
        {
            throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "No ngrams found in the model file {}", file_path);
        }

        // Make sure that the classes provided in priors are present in the model
        for (const auto & prior : priors)
        {
            String class_name = prior.first;
            if (class_totals.find(class_name) == class_totals.end()) // Class present in config's <priors> not found in model
            {
                String available_classes = "";
                for (const auto & class_entry : class_totals)
                {
                    available_classes += class_entry.getKey().toString() + ", ";
                }
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Class {} from <priors> not found in the model at {}. Available classes: {}",
                    class_name,
                    file_path,
                    available_classes);
            }
        }

        // The following handles the case where the user provides priors for some classes but not all.
        // After assigning the provided priors, we distribute the remaining probability equally among the classes
        // that were not provided in the priors map (i.e., the classes that are present in the model but not in the
        // config's <priors>)
        double provided_total_prior_prob = 0.0;
        for (const auto & prior : priors)
        {
            const double class_prior_prob = prior.second;
            provided_total_prior_prob += class_prior_prob;
        }

        // Sanity check: the sum of provided priors should not exceed 1.0
        if (provided_total_prior_prob - 1.0 > 1e-6)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Sum of provided priors probability exceeds 1.0. Sum: {}", provided_total_prior_prob);
        }

        size_t missing_count = 0;
        for (const auto & class_entry : class_totals)
        {
            String class_name = class_entry.getKey().toString();
            if (priors.find(class_name) == priors.end())
                ++missing_count;
        }

        const double remaining_probability = 1.0 - provided_total_prior_prob;

        // Precompute the class priors
        for (const auto & class_entry : class_totals)
        {
            String class_name = class_entry.getKey().toString();
            if (priors.find(class_name) != priors.end()) // Class from model present in config's <priors>
            {
                class_priors[class_entry.getKey()] = priors.at(class_name);
            }
            else
            {
                double equal_share = missing_count > 0 ? remaining_probability / missing_count : 0.0;
                class_priors[class_entry.getKey()] = equal_share;
            }
        }

        // Vocabulary size is the number of distinct tokens
        vocabulary_size = ngram_counts.size();

        // Mark the model as loaded
        model_loaded = true;
    }

    // Classify an input string. The function splits the input into tokens (by space)
    // and computes a log-probability for each class using Laplace smoothing
    String classify(const String & input) const
    {
        if (!model_loaded)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Model not loaded. Load the model using loadModel() before classifying.");
        }


        ProbabilityMap class_log_probabilities;
        for (const auto & entry : class_priors)
        {
            class_log_probabilities[entry.getKey()] = std::log(entry.getMapped());
        }

        std::vector<String> tokens;
        boost::split(tokens, input, boost::is_any_of(" "));

        for (const auto & token : tokens)
        {
            StringRef token_ref(token);
            bool token_exists = (ngram_counts.find(token_ref) != ngram_counts.end());
            const auto * token_class_map = token_exists ? &ngram_counts.find(token_ref)->getMapped() : nullptr;
            for (const auto & class_entry : class_totals)
            {
                StringRef class_label = class_entry.getKey();
                double class_total = static_cast<double>(class_entry.getMapped());
                double count = 0.0;
                if (token_exists)
                {
                    auto it = token_class_map->find(class_label);
                    if (it != token_class_map->end())
                        count = static_cast<double>(it->getMapped());
                }
                double probability = (count + alpha) / (class_total + alpha * vocabulary_size);
                class_log_probabilities[class_label] += std::log(probability);
            }
        }

        // Find the class with the highest log probability
        StringRef best_class;
        double max_log_prob = -std::numeric_limits<double>::infinity();
        for (const auto & entry : class_log_probabilities)
        {
            if (entry.getMapped() > max_log_prob)
            {
                max_log_prob = entry.getMapped();
                best_class = entry.getKey();
            }
        }

        return best_class.toString();
    }
};

class FunctionNaiveBayesClassifier : public IFunction
{
private:
    ContextPtr context;
    NaiveBayesModel model;

public:
    static constexpr auto name = "naiveBayesClassifier";

    explicit FunctionNaiveBayesClassifier(ContextPtr context_)
        : context(context_)
    {
        String model_path = "/etc/clickhouse-server/config.d/naive_bayes_test.txt";
        model.loadModelFromFile(model_path);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifier>(context); }

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto model_name_input_argument_type = WhichDataType(arguments[0].type);
        if (!model_name_input_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String. Actual {}",
                getName(),
                arguments[0].type->getName());

        auto input_string_input_argument_type = WhichDataType(arguments[1].type);
        if (!input_string_input_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} second argument type should be String. Actual {}",
                getName(),
                arguments[1].type->getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        [[maybe_unused]] const auto * model_name_column = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const auto * input_string_column = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        auto result_column = ColumnString::create();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String input_string = input_string_column->getDataAt(i).toString();
            String predicted_class = model.classify(input_string);
            result_column->insertData(predicted_class.data(), predicted_class.size());
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
