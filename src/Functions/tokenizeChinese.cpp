#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Common/Tokenizer/ChineseTokenizer.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ITokenExtractor.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class FunctionTokenizeChinese : public IFunction
{
public:
    static constexpr auto name = "tokenizeChinese";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionTokenizeChinese>();
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isVariadic() const override { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return ColumnNumbers{}; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto input_argument_type = WhichDataType(arguments[0].type);
        if (!input_argument_type.isStringOrFixedString())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} first argument type should be String or FixedString. Actual {}",
                getName(),
                arguments[0].type->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column_offsets = ColumnArray::ColumnOffsets::create();
        auto result_column = ColumnString::create();

        auto input_column = arguments[0].column;

        if (const auto * column_string = checkAndGetColumn<ColumnString>(input_column.get()))
            executeImpl(*column_string, *column_offsets, input_rows_count, *result_column);
        else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(input_column.get()))
            executeImpl(*column_fixed_string, *column_offsets, input_rows_count, *result_column);

        return ColumnArray::create(std::move(result_column), std::move(column_offsets));
    }

private:
    template <typename StringColumnType>
    void executeImpl(
        StringColumnType & input_data_column,
        ColumnArray::ColumnOffsets & input_column_offsets,
        size_t input_rows_count,
        ColumnString & result_column) const
    {
        auto & offsets_data = input_column_offsets.getData();

        offsets_data.resize(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const auto data = input_data_column.getDataAt(i);
            const auto words = ChineseTokenizer::instance().tokenize(data.data);
            for (const auto & word : words)
            {
                result_column.insertData(word.data(), word.size());
            }

            offsets_data[i] = words.size();
        }
    }
};

REGISTER_FUNCTION(TokenizerChinese)
{
    factory.registerFunction<FunctionTokenizeChinese>();
}

}


