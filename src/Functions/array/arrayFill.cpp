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


/** Replaces values where condition is met with the previous value that have condition not met
  * (or with the first value if condition was true for all elements before).
  * Looks somewhat similar to arrayFilter, but instead removing elements, it fills gaps with the value of previous element.
  */
template <bool reverse>
struct ArrayFillImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_fill = typeid_cast<const ColumnUInt8 *>(&*mapped);

        const IColumn & in_data = array.getData();
        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_data = in_data.cloneEmpty();
        IColumn & out_data = *column_data.get();

        if (column_fill)
        {
            const IColumn::Filter & fill = column_fill->getData();

            size_t array_begin = 0;
            size_t array_end = 0;
            size_t begin = 0;
            size_t end = 0;

            out_data.reserve(in_data.size());

            for (auto in_offset : in_offsets)
            {
                array_end = in_offset;

                for (; end < array_end; ++end)
                {
                    if (end + 1 == array_end || fill[end + 1] != fill[begin])
                    {
                        if (fill[begin])
                            out_data.insertRangeFrom(in_data, begin, end + 1 - begin);
                        else
                        {
                            if constexpr (reverse)
                            {
                                if (end + 1 == array_end)
                                    out_data.insertManyFrom(in_data, end, end + 1 - begin);
                                else
                                    out_data.insertManyFrom(in_data, end + 1, end + 1 - begin);
                            }
                            else
                            {
                                if (begin == array_begin)
                                    out_data.insertManyFrom(in_data, array_begin, end + 1 - begin);
                                else
                                    out_data.insertManyFrom(in_data, begin - 1, end + 1 - begin);
                            }
                        }

                        begin = end + 1;
                    }
                }

                array_begin = array_end;
            }
        }
        else
        {
            const auto * column_fill_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_fill_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of cut column");

            if (column_fill_const->getValue<UInt8>())
                return ColumnArray::create(
                    array.getDataPtr(),
                    array.getOffsetsPtr());

            size_t array_begin = 0;
            size_t array_end = 0;

            out_data.reserve(in_data.size());

            for (auto in_offset : in_offsets)
            {
                array_end = in_offset - 1;

                if constexpr (reverse)
                    out_data.insertManyFrom(in_data, array_end, array_end + 1 - array_begin);
                else
                    out_data.insertManyFrom(in_data, array_begin, array_end + 1 - array_begin);

                array_begin = array_end + 1;
            }
        }

        return ColumnArray::create(
            std::move(column_data),
            array.getOffsetsPtr()
        );
    }
};

struct NameArrayFill { static constexpr auto name = "arrayFill"; };
struct NameArrayReverseFill { static constexpr auto name = "arrayReverseFill"; };
using FunctionArrayFill = FunctionArrayMapped<ArrayFillImpl<false>, NameArrayFill>;
using FunctionArrayReverseFill = FunctionArrayMapped<ArrayFillImpl<true>, NameArrayReverseFill>;

REGISTER_FUNCTION(ArrayFill)
{
    FunctionDocumentation::Description description = R"(
The `arrayFill` function sequentially processes a source array from the first element
to the last, evaluating a lambda condition at each position using elements from
the source and condition arrays. When the lambda function evaluates to false at
position i, the function replaces that element with the element at position i-1
from the current state of the array. The first element is always preserved
regardless of any condition.
)";
    FunctionDocumentation::Syntax syntax = "arrayFill(func(x [, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments = {
        {"func(x [, y1, ..., yN])", "A lambda function `func(x [, y1, y2, ... yN]) → F(x [, y1, y2, ... yN])` which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Lambda function"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array", {"Array(T)"}};    FunctionDocumentation::Examples examples = {
        {"Example with single array", "SELECT arrayFill(x -> not isNull(x), [1, null, 2, null]) AS res", "[1, 1, 2, 2]"},
        {"Example with two arrays", "SELECT arrayFill(x, y, z -> x > y AND x < z, [5, 3, 6, 2], [4, 7, 1, 3], [10, 2, 8, 5]) AS res", "[5, 5, 6, 6]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayFill>(documentation);

    FunctionDocumentation::Description description_reverse = R"(
The `arrayReverseFill` function sequentially processes a source array from the last
element to the first, evaluating a lambda condition at each position using elements
from the source and condition arrays. When the condition evaluates to false at
position i, the function replaces that element with the element at position i+1
from the current state of the array. The last element is always preserved
regardless of any condition.
    )";
    FunctionDocumentation::Syntax syntax_reverse = "arrayReverseFill(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_reverse = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}},
    };
    FunctionDocumentation::ReturnedValue returned_value_reverse = {"Returns an array with elements of the source array replaced by the results of the lambda.", {"Array(T)"}};
    FunctionDocumentation::Examples examples_reverse = {
        {"Example with a single array", "SELECT arrayReverseFill(x -> not isNull(x), [1, null, 2, null]) AS res", "[1, 2, 2, NULL]"},
        {"Example with two arrays", "SELECT arrayReverseFill(x, y, z -> x > y AND x < z, [5, 3, 6, 2], [4, 7, 1, 3], [10, 2, 8, 5]) AS res;", "[5, 6, 6, 2]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_reverse = {20, 1};
    FunctionDocumentation::Category category_reverse = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_reverse = {description_reverse, syntax_reverse, arguments_reverse, returned_value_reverse, examples_reverse, introduced_in_reverse, category_reverse};

    factory.registerFunction<FunctionArrayReverseFill>(documentation_reverse);
}

}
