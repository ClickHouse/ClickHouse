#include <ranges>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/naiveBayesClassifier.h>
#include <Interpreters/Context.h>
#include <fmt/ranges.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Exception.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
extern const Event NaiveBayesClassifierModelsLoaded;
extern const Event NaiveBayesClassifierModelsAllocatedBytes;
}

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

namespace
{


class NBModelRegistry
{
public:
    NBModelRegistry(const NBModelRegistry &) = delete;
    NBModelRegistry & operator=(const NBModelRegistry &) = delete;

    using ProbabilityMap = HashMap<UInt32, double, HashCRC32<UInt32>>;

    using ByteNBC = NaiveBayesClassifier<BytePolicy>;
    using CodeNBC = NaiveBayesClassifier<CodePointPolicy>;
    using TokenNBC = NaiveBayesClassifier<TokenPolicy>;

    using Model = std::variant<ByteNBC, CodeNBC, TokenNBC>;
    using Models = std::unordered_map<String, Model>;

    // context from the FIRST call is used to build the registry.
    // Later calls ignore their argument — they only return the map.
    static const Models & instance(ContextPtr context)
    {
        static NBModelRegistry reg(context);
        return reg.models;
    }

private:
    Models models;

    explicit NBModelRegistry(ContextPtr context) { load(context); }

    void load(ContextPtr context);
};

void NBModelRegistry::load(ContextPtr context)
{
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
        if (!config.has(model_name_path))
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model name via 'name' key in <nb_models> in key {}", key);
        }
        const String model_name = config.getString(model_name_path);

        /// Check if there is already a model with the same name
        if (models.contains(model_name))
        {
            throw Exception(
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                "Duplicate model name {} found in <nb_models>. Please use unique names for each model",
                model_name);
        }

        const String model_data_path = "nb_models." + key + ".path";
        const String model_n_path = "nb_models." + key + ".n";
        const String model_mode_path = "nb_models." + key + ".mode";

        if (!config.has(model_data_path))
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model data path via 'path' key in <nb_models> for model {}", key);
        }
        if (!config.has(model_n_path))
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model ngram 'n' via 'n' key in <nb_models> for model {}", key);
        }
        if (!config.has(model_mode_path))
        {
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing model mode via 'mode' key in <nb_models> for model {}", key);
        }

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
        double total_prior_prob = 0.0;
        for (const auto & prior_key : prior_keys)
        {
            const String model_prior_path = model_priors_path + "." + prior_key;
            if (!config.has(model_prior_path + ".class") or !config.has(model_prior_path + ".value"))
            {
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Missing 'class' or 'value' key in <priors> for model {}", model_name);
            }
            const UInt32 class_id = config.getInt(model_prior_path + ".class");
            const double prior = config.getDouble(model_prior_path + ".value");
            priors[class_id] = prior;
            total_prior_prob += prior;

            if (prior <= 0.0)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Prior probability must be greater than 0 for model {} and class {}. Got {}",
                    model_name,
                    class_id,
                    prior);
            }

            if (prior > 1.0)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Prior probability must be less than or equal to 1 for model {} and class {}. Got {}",
                    model_name,
                    class_id,
                    prior);
            }
        }

        if (!priors.empty() && fabs(total_prior_prob - 1.0) > 1e-6)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Sum of provided priors probability is not equal to 1.0 for model {}. Sum: {}",
                model_name,
                total_prior_prob);
        }

        const double alpha = config.getDouble("nb_models." + key + ".alpha", 1.0);

        if (alpha <= 0.0)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Laplace smoothing parameter 'alpha' must be greater than 0 for model {}", model_name);
        }

        const String mode = config.getString(model_mode_path);

        if (mode == "byte")
        {
            models.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(model_name),
                std::forward_as_tuple(std::in_place_type<ByteNBC>, model_name, model_data, std::move(priors), n, alpha));
        }
        else if (mode == "codepoint")
        {
            models.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(model_name),
                std::forward_as_tuple(std::in_place_type<CodeNBC>, model_name, model_data, std::move(priors), n, alpha));
        }
        else if (mode == "token")
        {
            models.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(model_name),
                std::forward_as_tuple(std::in_place_type<TokenNBC>, model_name, model_data, std::move(priors), n, alpha));
        }
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid mode {} for model {}. Only 'byte', 'codepoint', and 'token' are available",
                mode,
                model_name);

        /// Increment profile events for loaded models
        ProfileEvents::increment(ProfileEvents::NaiveBayesClassifierModelsLoaded);
        std::visit(
            [&](const auto & concrete_classifier)
            { ProfileEvents::increment(ProfileEvents::NaiveBayesClassifierModelsAllocatedBytes, concrete_classifier.getAllocatedBytes()); },
            models.at(model_name));
    }

    if (models.empty())
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No models found under <nb_models> in config");
    }
}

class FunctionNaiveBayesClassifier : public IFunction
{
private:
    ContextPtr context;

public:
    static constexpr auto name = "naiveBayesClassifier";

    explicit FunctionNaiveBayesClassifier(ContextPtr context_)
        : context(context_)
    {
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifier>(context); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto model_name_argument_type = WhichDataType(arguments[0].type);
        if (!model_name_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String. Actual {}",
                getName(),
                arguments[0].type->getName());

        const auto input_text_argument_type = WhichDataType(arguments[1].type);
        if (!input_text_argument_type.isStringOrFixedString())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Function {} second argument type should be String. Actual {}",
                getName(),
                arguments[1].type->getName());

        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & models = NBModelRegistry::instance(context);

        const auto * const_model_name_col = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
        const auto * const_input_text_col = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
        if (const_model_name_col and const_input_text_col)
        {
            const String model_name = const_model_name_col->getValue<String>();
            validateModelName(model_name);

            const String input_text = const_input_text_col->getValue<String>();
            validateInputText(input_text, model_name);

            UInt32 predicted_class = std::visit([&](const auto & model) { return model.classify(input_text); }, models.at(model_name));
            return result_type->createColumnConst(input_rows_count, predicted_class);
        }

        ColumnPtr model_name_column = arguments[0].column->convertToFullColumnIfConst();
        ColumnPtr input_text_column = arguments[1].column->convertToFullColumnIfConst();

        auto result_column = ColumnVector<UInt32>::create(input_rows_count);
        auto & data = result_column->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const String model_name = model_name_column->getDataAt(i).toString();
            validateModelName(model_name);

            const String input_text = input_text_column->getDataAt(i).toString();
            validateInputText(input_text, model_name);

            UInt32 predicted_class = std::visit([&](const auto & model) { return model.classify(input_text); }, models.at(model_name));
            data[i] = predicted_class;
        }

        return result_column;
    }

private:
    void validateModelName(const String & model_name) const
    {
        const auto & models = NBModelRegistry::instance(context);

        if (!models.contains(model_name))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Model {} not found. Available models: {}",
                model_name,
                fmt::join(models | std::views::transform([](const auto & model) { return model.first; }), ", "));
        }
    }

    void validateInputText(const String & input_text, const String & model_name) const
    {
        if (input_text.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input text is empty for model {}. Please provide a non-empty string.", model_name);
        }
    }
};
}

REGISTER_FUNCTION(NaiveBayesClassifier)
{
    FunctionDocumentation::Description description = "Classifies input text using a Naive Bayes model with ngrams and Laplace smoothing. "
                                                     "The model must be configured in ClickHouse before use.";
    FunctionDocumentation::Syntax syntax = "naiveBayesClassifier(model_name, input_text)";
    FunctionDocumentation::Arguments arguments
        = {{"model_name", "Name of the pre-configured model. The model must be defined in ClickHouse's configuration files.", {"String"}},
           {"input_text", "Text to classify. Input is processed exactly as provided (case/punctuation preserved).", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value
        = {"Predicted class ID as an unsigned integer. Class IDs correspond to categories defined during model construction.", {"UInt32"}};
    FunctionDocumentation::Examples examples
        = {{"Classify the language of a text",
            "SELECT naiveBayesClassifier('language', 'How are you?');",
            R"(
          ┌─naiveBayesClassifier('language', 'How are you?')─┐
          │ 0                                                │
          └──────────────────────────────────────────────────┘

          Result 0 might represent English, while 1 could indicate French - class meanings depend on your training data.
        )"}};
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
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
