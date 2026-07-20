#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

UInt8 isValidASCII(const UInt8 * data, UInt64 len)
{
    /// https://lemire.me/blog/2025/12/20/performance-trick-optimistic-vs-pessimistic-checks/
    UInt8 res = 0;
    for (UInt64 i = 0; i < len; ++i)
        res |= data[i];
    return res <= 0x7F;
}

}

class FunctionIsValidASCII : public IFunction
{
public:
    static constexpr auto name = "isValidASCII";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<DB::FunctionIsValidASCII>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & column = arguments[0];

        if (const auto * col_str = checkAndGetColumn<ColumnString>(column.column.get()))
        {
            return executeString(col_str, input_rows_count);
        }
        else if (const auto * col_fixed_str = checkAndGetColumn<ColumnFixedString>(column.column.get()))
        {
            return executeFixedString(col_fixed_str, input_rows_count);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Cannot apply function {} to column {}, expected String or FixedString",
                getName(), column.column->getName());
        }
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

private:
    ColumnPtr executeString(const ColumnString * col_str, size_t input_rows_count) const
    {
        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();

        const auto & chars = col_str->getChars();
        const auto & offsets = col_str->getOffsets();

        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            size_t current_offset = offsets[i];
            size_t string_size = current_offset - prev_offset;
            result_data[i] = isValidASCII(chars.data() + prev_offset, string_size);
            prev_offset = current_offset;
        }

        return result;
    }

    ColumnPtr executeFixedString(const ColumnFixedString * col_fixed_str, size_t input_rows_count) const
    {
        auto result = ColumnUInt8::create(input_rows_count);
        auto & result_data = result->getData();

        const auto & chars = col_fixed_str->getChars();
        size_t string_size = col_fixed_str->getN();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            result_data[i] = isValidASCII(chars.data() + i * string_size, string_size);
        }

        return result;
    }
};

}

REGISTER_FUNCTION(IsValidASCII)
{
    factory.registerFunction<DB::FunctionIsValidASCII>(DB::FunctionDocumentation{
        .description = R"(Returns 1 if the input String or FixedString contains only ASCII bytes (0x00–0x7F), otherwise 0. Optimized for the positive case (the input _is_ valid ASCII).)",
        .syntax = "isValidASCII(str)",
        .examples = {{"isValidASCII", "SELECT isValidASCII('hello') AS is_ascii, isValidASCII('你好') AS is_not_ascii", ""}},
        .introduced_in = {25, 9},
        .category = DB::FunctionDocumentation::Category::String,
    });
    factory.registerAlias("isASCII", "isValidASCII", DB::FunctionFactory::Case::Sensitive);
}
