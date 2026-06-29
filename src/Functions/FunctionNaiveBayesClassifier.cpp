#include <atomic>
#include <string_view>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
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
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

void validateArguments(const IFunction & func, const ColumnsWithTypeAndName & arguments)
{
    validateFunctionArguments(
        func,
        arguments,
        {
            {"dictionary_name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"input_text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        });
}

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
        context->checkAccess(AccessType::dictGet, dictionary->getDatabaseOrNoDatabaseTag(), dictionary->getDictionaryID().getTableName());
        access_checked.store(true, std::memory_order_relaxed);
    }

    const auto * nb_dict = typeid_cast<const NaiveBayesDictionary *>(dictionary.get());
    if (!nb_dict)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary '{}' is not a Naive Bayes dictionary", dictionary_name);

    const ColumnPtr & text_column = arguments[1].column;

    nb_dict->visitModel(
        [&](const auto & model)
        {
            NaiveBayesScratch scratch;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const std::string_view text = text_column->getDataAt(i);
                classify_row(model, scratch, text, i);
            }
        });

    /// These functions bypass `getColumn`, so record the classified rows here to keep the dictionary query
    /// statistics consistent with the equivalent `dictGet` path.
    nb_dict->incrementQueryCount(input_rows_count);
}

DataTypePtr makeClassProbTuple()
{
    return std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeUInt32>(), std::make_shared<DataTypeFloat64>()}, Strings{"class_id", "probability"});
}


/// Common state and traits shared by the three naiveBayesClassifier* functions.
class FunctionNaiveBayesBase : public IFunction, protected WithContext
{
protected:
    mutable std::atomic<bool> access_checked{false};

public:
    explicit FunctionNaiveBayesBase(ContextPtr context_)
        : WithContext(context_)
    {
    }

    bool isVariadic() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
};


/// Implements `naiveBayesClassifier(dictionary_name, input_text)`, returning the predicted class id.
class FunctionNaiveBayesClassifier : public FunctionNaiveBayesBase
{
public:
    using FunctionNaiveBayesBase::FunctionNaiveBayesBase;

    static constexpr auto name = "naiveBayesClassifier";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionNaiveBayesClassifier>(context_); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateArguments(*this, arguments);
        return std::make_shared<DataTypeUInt32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto result_column = ColumnUInt32::create(input_rows_count);
        auto & data = result_column->getData();

        executeNaiveBayes(
            getContext(),
            access_checked,
            arguments,
            input_rows_count,
            [&](const auto & model, NaiveBayesScratch & scratch, std::string_view text, size_t i)
            { data[i] = model.classify(text, scratch); });

        return result_column;
    }
};


/// Implements `naiveBayesClassifierWithProb(dictionary_name, input_text)`, returning the predicted class
/// id together with its probability as a tuple.
class FunctionNaiveBayesClassifierWithProb : public FunctionNaiveBayesBase
{
public:
    using FunctionNaiveBayesBase::FunctionNaiveBayesBase;

    static constexpr auto name = "naiveBayesClassifierWithProb";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionNaiveBayesClassifierWithProb>(context_); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateArguments(*this, arguments);
        return makeClassProbTuple();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto class_col = ColumnUInt32::create(input_rows_count);
        auto prob_col = ColumnFloat64::create(input_rows_count);
        auto & class_data = class_col->getData();
        auto & prob_data = prob_col->getData();

        executeNaiveBayes(
            getContext(),
            access_checked,
            arguments,
            input_rows_count,
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


/// Implements `naiveBayesClassifierWithAllProbs(dictionary_name, input_text)`, returning every class with its
/// probability as an array of tuples, ordered from most to least probable.
class FunctionNaiveBayesClassifierWithAllProbs : public FunctionNaiveBayesBase
{
public:
    using FunctionNaiveBayesBase::FunctionNaiveBayesBase;

    static constexpr auto name = "naiveBayesClassifierWithAllProbs";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionNaiveBayesClassifierWithAllProbs>(context_); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateArguments(*this, arguments);
        return std::make_shared<DataTypeArray>(makeClassProbTuple());
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        auto class_col = ColumnUInt32::create();
        auto prob_col = ColumnFloat64::create();
        auto & class_data = class_col->getData();
        auto & prob_data = prob_col->getData();
        auto offsets_col = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto & offsets = offsets_col->getData();

        executeNaiveBayes(
            getContext(),
            access_checked,
            arguments,
            input_rows_count,
            [&](const auto & model, NaiveBayesScratch & scratch, std::string_view text, size_t i)
            {
                const auto & probabilities = model.classifyWithAllProbs(text, scratch);
                for (const auto & [class_id, prob] : probabilities)
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
        .description = "Classifies input text using a Naive Bayes dictionary. "
                       "Equivalent to dictGet(dictionary_name, 'class_id', input_text).",
        .syntax = "naiveBayesClassifier(dictionary_name, input_text)",
        .arguments
        = {{"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
           {"input_text", "Text to classify.", {"String"}}},
        .returned_value = {"Predicted class ID.", {"UInt32"}},
        .examples = {{"Classify text", "SELECT naiveBayesClassifier('model', 'some text');", "0"}},
        .introduced_in = {25, 11},
        .category = FunctionDocumentation::Category::MachineLearning});

    factory.registerFunction<FunctionNaiveBayesClassifierWithProb>(FunctionDocumentation{
        .description = "Classifies input text using a Naive Bayes dictionary and returns the predicted class with its probability.",
        .syntax = "naiveBayesClassifierWithProb(dictionary_name, input_text)",
        .arguments
        = {{"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
           {"input_text", "Text to classify.", {"String"}}},
        .returned_value = {"Tuple of (class_id, probability).", {"Tuple(UInt32, Float64)"}},
        .examples = {{"Classify with probability", "SELECT naiveBayesClassifierWithProb('model', 'some text');", "(0,0.85)"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::MachineLearning});

    factory.registerFunction<FunctionNaiveBayesClassifierWithAllProbs>(FunctionDocumentation{
        .description = "Classifies input text using a Naive Bayes dictionary and returns all classes with their probabilities, "
                       "ordered from most to least probable.",
        .syntax = "naiveBayesClassifierWithAllProbs(dictionary_name, input_text)",
        .arguments
        = {{"dictionary_name", "Name of a dictionary with the NAIVE_BAYES layout.", {"String"}},
           {"input_text", "Text to classify.", {"String"}}},
        .returned_value
        = {"Array of (class_id, probability) tuples ordered from most to least probable.", {"Array(Tuple(UInt32, Float64))"}},
        .examples = {{"All class probabilities", "SELECT naiveBayesClassifierWithAllProbs('model', 'some text');", "[(0,0.85),(1,0.15)]"}},
        .introduced_in = {26, 7},
        .category = FunctionDocumentation::Category::MachineLearning});
}
}
