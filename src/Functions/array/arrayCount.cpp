#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

/** arrayCount(x1,...,xn -> expression, array1,...,arrayn) - for how many elements of the array the expression is true.
  * An overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */
struct ArrayCountImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            const auto * column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of filter column: {}; The result is expected to be a UInt8", mapped->getDataType());

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                ColumnUInt32::Container & out_counts = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_counts[i] = static_cast<UInt32>(offsets[i] - pos);
                    pos = offsets[i];
                }

                return out_column;
            }
            return DataTypeUInt32().createColumnConst(array.size(), 0u);
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt32::create(offsets.size());
        ColumnUInt32::Container & out_counts = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t count = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                    ++count;
            }
            out_counts[i] = static_cast<UInt32>(count);
        }

        return out_column;
    }
};

struct NameArrayCount { static constexpr auto name = "arrayCount"; };
using FunctionArrayCount = FunctionArrayMapped<ArrayCountImpl, NameArrayCount>;

REGISTER_FUNCTION(ArrayCount)
{
    FunctionDocumentation::Description description = R"(
Returns the number of elements for which `func(arr1[i], ..., arrN[i])` returns true.
If `func` is not specified, it returns the number of non-zero elements in the array.

`arrayCount` is a [higher-order function](/sql-reference/functions/overview#higher-order-functions).
    )";
    FunctionDocumentation::Syntax syntax = "arrayCount([func, ] arr1, ...)";
    FunctionDocumentation::Arguments arguments = {
        {"func", "Optional. Function to apply to each element of the array(s).", {"Lambda function"}},
        {"arr1, ..., arrN", "N arrays.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of elements for which `func` returns true. Otherwise, returns the number of non-zero elements in the array.", {"UInt32"}};
    FunctionDocumentation::Examples example = {{"Usage example", "SELECT arrayCount(x -> (x % 2), groupArray(number)) FROM numbers(10)", "5"}};
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};

    factory.registerFunction<FunctionArrayCount>(documentation);
}

}


