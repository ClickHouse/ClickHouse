#include <atomic>
#include <string_view>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/NaiveBayesDictionary.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Access/Common/AccessFlags.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

void validateNBArguments(const String & func_name, const ColumnsWithTypeAndName & arguments)
{
    if (!WhichDataType(arguments[0].type).isStringOrFixedString())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Function {} first argument (dictionary name) must be String, got {}",
            func_name,
            arguments[0].type->getName());

    if (!WhichDataType(arguments[1].type).isStringOrFixedString())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Function {} second argument (input text) must be String, got {}",
            func_name,
            arguments[1].type->getName());
}

/// Drives the per-row work shared by the three Naive Bayes functions. It resolves the dictionary by
/// name, caches the most recently used one, checks the dictionary access right once per query, validates
/// the input text, and invokes the callback with the dictionary, the input text, and the row index.
template <typename PerRow>
void executeNaiveBayes(
    const ContextPtr & context,
    std::atomic<bool> & access_checked,
    const ColumnsWithTypeAndName & arguments,
    size_t input_rows_count,
    PerRow && per_row)
{
    if (input_rows_count == 0)
        return;

    ColumnPtr name_column = arguments[0].column->convertToFullColumnIfConst();
    ColumnPtr text_column = arguments[1].column->convertToFullColumnIfConst();

    const auto & loader = context->getExternalDictionariesLoader();

    std::string_view cached_name;
    std::shared_ptr<const IDictionary> dict_holder;
    const NaiveBayesDictionary * nb_dict = nullptr;

    for (size_t i = 0; i < input_rows_count; ++i)
    {
        const std::string_view name = name_column->getDataAt(i);
        const std::string_view text = text_column->getDataAt(i);

        if (text.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Input text is empty for dictionary {}. Please provide a non-empty string.", name);

        if (!nb_dict || name != cached_name)
        {
            const String name_str{name};
            dict_holder = loader.getDictionary(name_str, context);

            if (!access_checked.load(std::memory_order_relaxed))
            {
                context->checkAccess(
                    AccessType::dictGet, dict_holder->getDatabaseOrNoDatabaseTag(), dict_holder->getDictionaryID().getTableName());
                access_checked.store(true, std::memory_order_relaxed);
            }

            nb_dict = typeid_cast<const NaiveBayesDictionary *>(dict_holder.get());
            if (!nb_dict)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary '{}' is not a NaiveBayes dictionary", name_str);

            cached_name = name;
        }

        per_row(*nb_dict, text, i);
    }
}

DataTypePtr makeClassProbTuple()
{
    return std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()}, Strings{"class_id", "probability"});
}


/// Implements `naiveBayesClassifier(dictionary_name, input_text)`, returning the predicted class id.
class FunctionNaiveBayesClassifier : public IFunction
{
    ContextPtr context;
    mutable std::atomic<bool> access_checked{false};

public:
    static constexpr auto name = "naiveBayesClassifier";
    explicit FunctionNaiveBayesClassifier(ContextPtr context_) : context(context_) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifier>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(getName(), arguments);
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt32::create(input_rows_count);
        auto & data = result_column->getData();

        executeNaiveBayes(context, access_checked, arguments, input_rows_count,
            [&](const NaiveBayesDictionary & dict, std::string_view text, size_t i) { data[i] = dict.classifyText(text); });

        return result_column;
    }
};


/// Implements `naiveBayesClassifierWithProb(dictionary_name, input_text)`, returning the predicted class
/// id together with its probability as a tuple.
class FunctionNaiveBayesClassifierWithProb : public IFunction
{
    ContextPtr context;
    mutable std::atomic<bool> access_checked{false};

public:
    static constexpr auto name = "naiveBayesClassifierWithProb";
    explicit FunctionNaiveBayesClassifierWithProb(ContextPtr context_) : context(context_) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifierWithProb>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(getName(), arguments);
        return makeClassProbTuple();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto class_col = ColumnUInt32::create(input_rows_count);
        auto prob_col = ColumnFloat64::create(input_rows_count);
        auto & class_data = class_col->getData();
        auto & prob_data = prob_col->getData();

        executeNaiveBayes(context, access_checked, arguments, input_rows_count,
            [&](const NaiveBayesDictionary & dict, std::string_view text, size_t i)
            {
                auto [best_class, best_prob] = dict.classifyTextWithProb(text);
                class_data[i] = best_class;
                prob_data[i] = best_prob;
            });

        Columns tuple_columns;
        tuple_columns.emplace_back(std::move(class_col));
        tuple_columns.emplace_back(std::move(prob_col));
        return ColumnTuple::create(std::move(tuple_columns));
    }
};


/// Implements `naiveBayesClassifierAllProbs(dictionary_name, input_text)`, returning every class with its
/// probability as an array of tuples, sorted by probability descending.
class FunctionNaiveBayesClassifierAllProbs : public IFunction
{
    ContextPtr context;
    mutable std::atomic<bool> access_checked{false};

public:
    static constexpr auto name = "naiveBayesClassifierAllProbs";
    explicit FunctionNaiveBayesClassifierAllProbs(ContextPtr context_) : context(context_) {}
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionNaiveBayesClassifierAllProbs>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(getName(), arguments);
        return std::make_shared<DataTypeArray>(makeClassProbTuple());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto class_col = ColumnUInt32::create();
        auto prob_col = ColumnFloat64::create();
        auto & class_data = class_col->getData();
        auto & prob_data = prob_col->getData();
        auto offsets_col = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & offsets = offsets_col->getData();

        executeNaiveBayes(context, access_checked, arguments, input_rows_count,
            [&](const NaiveBayesDictionary & dict, std::string_view text, size_t i)
            {
                for (const auto & [class_id, prob] : dict.classifyTextAllProbs(text))
                {
                    class_data.push_back(class_id);
                    prob_data.push_back(prob);
                }
                offsets[i] = class_data.size();
            });

        Columns tuple_columns;
        tuple_columns.emplace_back(std::move(class_col));
        tuple_columns.emplace_back(std::move(prob_col));
        auto nested_col = ColumnTuple::create(std::move(tuple_columns));
        return ColumnArray::create(std::move(nested_col), std::move(offsets_col));
    }
};

}

REGISTER_FUNCTION(NaiveBayesClassifier)
{
    factory.registerFunction<FunctionNaiveBayesClassifier>(FunctionDocumentation{
        .description = "Classifies input text using a Naive Bayes dictionary (NAIVE_BAYES layout). "
                       "Equivalent to dictGet(dictionary_name, 'class_id', input_text).",
        .syntax = "naiveBayesClassifier(dictionary_name, input_text)",
        .arguments = {
            {"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
            {"input_text", "Text to classify.", {"String"}}},
        .returned_value = {"Predicted class ID.", {"UInt32"}},
        .examples = {{"Classify text", "SELECT naiveBayesClassifier('model', 'some text');", "0"}},
        .introduced_in = {25, 11},
        .category = FunctionDocumentation::Category::MachineLearning});

    factory.registerFunction<FunctionNaiveBayesClassifierWithProb>(FunctionDocumentation{
        .description = "Classifies input text using a Naive Bayes dictionary and returns the predicted class with its probability.",
        .syntax = "naiveBayesClassifierWithProb(dictionary_name, input_text)",
        .arguments = {
            {"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
            {"input_text", "Text to classify.", {"String"}}},
        .returned_value = {"Tuple of (class_id, probability).", {"Tuple(UInt32, Float64)"}},
        .examples = {{"Classify with probability", "SELECT naiveBayesClassifierWithProb('model', 'some text');", "(0,0.85)"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::MachineLearning});

    factory.registerFunction<FunctionNaiveBayesClassifierAllProbs>(FunctionDocumentation{
        .description = "Classifies input text using a Naive Bayes dictionary and returns all classes with their probabilities, "
                       "sorted by probability descending.",
        .syntax = "naiveBayesClassifierAllProbs(dictionary_name, input_text)",
        .arguments = {
            {"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
            {"input_text", "Text to classify.", {"String"}}},
        .returned_value = {"Array of (class_id, probability) tuples sorted by probability descending.", {"Array(Tuple(UInt32, Float64))"}},
        .examples = {{"All class probabilities", "SELECT naiveBayesClassifierAllProbs('model', 'some text');", "[(0,0.85),(1,0.15)]"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::MachineLearning});
}
}
