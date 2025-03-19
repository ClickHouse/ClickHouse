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
    // Loads the model from file. The file is expected to have lines like:
    // <ngram> <class_label> <count>
    void loadModelFromFile(const String & file_path)
    {
        DB::ReadBufferFromFile in(file_path);

        while (!in.eof())
        {
            String ngram, class_label;
            int count = 0;

            DB::readStringUntilWhitespace(ngram, in);
            in.ignore();

            // If ngram is empty, then break out
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

        double total_class_count = 0.0;
        for (const auto & class_entry : class_totals)
            total_class_count += class_entry.getMapped();

        for (const auto & class_entry : class_totals)
        {
            double prior_ratio = static_cast<double>(class_entry.getMapped()) / total_class_count;
            class_priors[class_entry.getKey()] = prior_ratio;
        }

        vocabulary_size = ngram_counts.size();

        // Mark the model as loaded
        model_loaded = true;
    }

    // Classify an input string. The function splits the input into tokens (by space)
    // and computes a log-probability for each class using Laplace smoothing
    String classify(const String & input) const
    {
        std::ofstream debug_out("/dev/tty");
        if (!model_loaded)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Model not loaded");
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

        return best_class.empty() ? "unknown" : best_class.toString();
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
