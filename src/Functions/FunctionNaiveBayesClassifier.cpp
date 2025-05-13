#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/naiveBayesClassifier.h>
#include <Interpreters/Context.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{

using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;
using Models = std::map<String, NaiveBayesClassifier>;

NaiveBayesClassifier::Mode stringToMode(const std::string & s)
{
    if (s == "byte")
    {
        return NaiveBayesClassifier::Mode::Byte;
    }
    else if (s == "codepoint")
    {
        return NaiveBayesClassifier::Mode::CodePoint;
    }
    else if (s == "token")
    {
        return NaiveBayesClassifier::Mode::Token;
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode: {}. Valid modes are: byte, codepoint, token", s);
    }
}

class FunctionNaiveBayesClassifier : public IFunction
{
private:
    ContextPtr context;

    /// Use static cache to ensure model loading happens only once
    static Models & getModelCache()
    {
        static Models models;
        return models;
    }

public:
    static constexpr auto name = "naiveBayesClassifier";

    explicit FunctionNaiveBayesClassifier(ContextPtr context_)
        : context(context_)
    {
        auto & models = getModelCache();

        if (!models.empty())
        {
            return; // models already loaded
        }

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
            const String model_mode_path = "nb_models." + key + ".mode";
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
            if (!config.has(model_mode_path))
            {
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model mode via 'mode' key in <nb_models> for model {}", key);
            }

            const String model_name = config.getString(model_name_path);
            const String model_data = config.getString(model_data_path);
            const UInt32 n = config.getInt(model_n_path);

            if (n == 0)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ngram size 'n' must be greater than 0 for model {}", model_name);
            }

            const String model_mode_str = config.getString(model_mode_path);
            NaiveBayesClassifier::Mode model_mode;
            try
            {
                model_mode = stringToMode(model_mode_str);
            }
            catch (const Exception & e)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode for model {}. {}", model_name, e.message());
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
                std::make_tuple(model_data, std::move(priors), n, alpha, model_mode));
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
            String model_name;
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
    FunctionDocumentation::Description description = "Classifies input text using a Naive Bayes model with ngrams and Laplace smoothing. "
                                                     "The model must be configured in ClickHouse before use.";
    FunctionDocumentation::Syntax syntax = "naiveBayesClassifier(model_name, input_text);";
    FunctionDocumentation::Arguments arguments
        = {{"model_name",
            "Name of the pre-configured model. [String](../data-types/string.md) The model must be defined in ClickHouse's configuration "
            "files."},
           {"input_text",
            "Text to classify. [String](../data-types/string.md) Input is processed exactly as provided (case/punctuation preserved)."}};
    FunctionDocumentation::ReturnedValue returned_value = "Predicted class ID as an unsigned integer. [UInt32](../data-types/int-uint.md) "
                                                          "Class IDs correspond to categories defined during model construction.";
    FunctionDocumentation::Examples examples
        = {{"Example",
            "SELECT naiveBayesClassifier('language', 'How are you?');",
            R"(
          ┌─naiveBayesClassifier('language', 'How are you?')─┐
          │ 0                                                │
          └──────────────────────────────────────────────────┘
          
          Result 0 might represent English, while 1 could indicate French - class meanings depend on your training data.
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 5};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::MachineLearning;

    FunctionDocumentation function_documentation
        = {.description = description,
           .syntax = syntax,
           .arguments = arguments,
           .returned_value = returned_value,
           .examples = examples,
           .introduced_in = introduced_in,
           .category = category};

    factory.registerFunction<FunctionNaiveBayesClassifier>(function_documentation);
}
}
