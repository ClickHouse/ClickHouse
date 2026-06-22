#include <atomic>
#include <string_view>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
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

/// The dictionary name must be a constant String so the dictionary can be resolved once per block;
/// the input text is any String column. `validateFunctionArguments` rejects a non-constant name with
/// ILLEGAL_COLUMN and a wrong type with ILLEGAL_TYPE_OF_ARGUMENT.
void validateNBArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
{
    validateFunctionArguments(
        func,
        arguments,
        {
            {"dictionary_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"input_text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String"},
        });
}

/// Drives the per-row work shared by the three Naive Bayes functions. The dictionary name (argument 0) is
/// validated as a constant in getReturnTypeImpl, so the dictionary is resolved and the dictGet access right
/// is checked once for the whole block. The tokenizer-policy variant is then resolved once and a single
/// scratch is reused across rows; the callback is invoked with the concrete model, that scratch, the input
/// text, and the row index for every row.
template <typename ClassifyRow>
void executeNaiveBayes(
    const ContextPtr & context,
    std::atomic<bool> & access_checked,
    const ColumnsWithTypeAndName & arguments,
    size_t input_rows_count,
    ClassifyRow && classify_row)
{
    if (input_rows_count == 0)
        return;

    const String dictionary_name{arguments[0].column->getDataAt(0)};

    auto dictionary = context->getExternalDictionariesLoader().getDictionary(dictionary_name, context);

    if (!access_checked.load(std::memory_order_relaxed))
    {
        context->checkAccess(
            AccessType::dictGet, dictionary->getDatabaseOrNoDatabaseTag(), dictionary->getDictionaryID().getTableName());
        access_checked.store(true, std::memory_order_relaxed);
    }

    const auto * nb_dict = typeid_cast<const NaiveBayesDictionary *>(dictionary.get());
    if (!nb_dict)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary '{}' is not a NaiveBayes dictionary", dictionary_name);

    ColumnPtr text_column = arguments[1].column->convertToFullColumnIfConst();

    nb_dict->visitModel([&](const auto & model)
    {
        NaiveBayesScratch scratch;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const std::string_view text = text_column->getDataAt(i);

            if (text.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Input text is empty for dictionary {}. Please provide a non-empty string.",
                    dictionary_name);

            classify_row(model, scratch, text, i);
        }
    });
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(*this, arguments);
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt32::create(input_rows_count);
        auto & data = result_column->getData();

        executeNaiveBayes(context, access_checked, arguments, input_rows_count,
            [&](const auto & model, NaiveBayesScratch & scratch, std::string_view text, size_t i)
            { data[i] = model.classify(text, scratch); });

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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(*this, arguments);
        return makeClassProbTuple();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto class_col = ColumnUInt32::create(input_rows_count);
        auto prob_col = ColumnFloat64::create(input_rows_count);
        auto & class_data = class_col->getData();
        auto & prob_data = prob_col->getData();

        executeNaiveBayes(context, access_checked, arguments, input_rows_count,
            [&](const auto & model, NaiveBayesScratch & scratch, std::string_view text, size_t i)
            {
                auto [best_class, best_prob] = model.classifyWithProb(text, scratch);
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
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateNBArguments(*this, arguments);
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
            [&](const auto & model, NaiveBayesScratch & scratch, std::string_view text, size_t i)
            {
                model.classifyAllProbs(text, scratch);
                for (const auto & [class_id, prob] : scratch.probabilities)
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
