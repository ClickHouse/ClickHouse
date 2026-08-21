#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenizer.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionNgrams : public IFunction
{
public:

    static constexpr auto name = "ngrams";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNgrams>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isVariadic() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return ColumnNumbers{1}; }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto ngram_input_argument_type = WhichDataType(arguments[0].type);
        if (!ngram_input_argument_type.isStringOrFixedString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String or FixedString. Actual {}",
                getName(),
                arguments[0].type->getName());

        const auto & column_with_type = arguments[1];
        const auto & ngram_argument_column = arguments[1].column;
        auto ngram_argument_type = WhichDataType(column_with_type.type);

        if (!ngram_argument_type.isNativeUInt() || !ngram_argument_column || !isColumnConst(*ngram_argument_column))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} second argument type should be constant UInt. Actual {}",
                getName(),
                arguments[1].type->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column_offsets = ColumnArray::ColumnOffsets::create();

        Field ngram_argument_value;
        arguments[1].column->get(0, ngram_argument_value);
        auto ngram_value = ngram_argument_value.safeGet<UInt64>();

        NgramsTokenizer tokenizer(ngram_value);

        auto result_column_string = ColumnString::create();

        auto input_column = arguments[0].column;

        if (const auto * column_string = checkAndGetColumn<ColumnString>(input_column.get()))
            executeImpl(tokenizer, *column_string, *result_column_string, *column_offsets, input_rows_count);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(input_column.get()))
            executeImpl(tokenizer, *column_fixed_string, *result_column_string, *column_offsets, input_rows_count);

        return ColumnArray::create(std::move(result_column_string), std::move(column_offsets));
    }

private:

    template <typename ExtractorType, typename StringColumnType, typename ResultStringColumnType>
    void executeImpl(
        const ExtractorType & tokenizer,
        StringColumnType & input_data_column,
        ResultStringColumnType & result_data_column,
        ColumnArray::ColumnOffsets & offsets_column,
        size_t input_rows_count) const
    {
        size_t current_tokens_size = 0;
        auto & offsets_data = offsets_column.getData();

        offsets_data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto data = input_data_column.getDataAt(i);

            size_t cur = 0;
            size_t token_start = 0;
            size_t token_length = 0;

            while (cur < data.size() && tokenizer.nextInString(data.data(), data.size(), cur, token_start, token_length))
            {
                result_data_column.insertData(data.data() + token_start, token_length);
                ++current_tokens_size;
            }

            offsets_data[i] = current_tokens_size;
        }
    }
};

REGISTER_FUNCTION(Ngrams)
{
    FunctionDocumentation::Description description = R"(
Splits a UTF-8 string into n-grams of length `N`.
)";
    FunctionDocumentation::Syntax syntax = "ngrams(s, N)";
    FunctionDocumentation::Arguments arguments = {
        {"s", "Input string.", {"String", "FixedString"}},
        {"N", "The n-gram length.", {"const UInt8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array with n-grams.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        "SELECT ngrams('ClickHouse', 3);",
        R"(
['Cli','lic','ick','ckH','kHo','Hou','ous','use']
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::StringSplitting;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionNgrams>(documentation);
}

}
