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

template <bool reverse>
struct ArraySplitImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeArray>(array_element)
        );
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_cut = typeid_cast<const ColumnUInt8 *>(&*mapped);

        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_offsets_2 = ColumnArray::ColumnOffsets::create();
        auto column_offsets_1 = ColumnArray::ColumnOffsets::create();
        IColumn::Offsets & out_offsets_2 = column_offsets_2->getData();
        IColumn::Offsets & out_offsets_1 = column_offsets_1->getData();

        if (column_cut)
        {
            const IColumn::Filter & cut = column_cut->getData();

            size_t pos = 0;

            out_offsets_2.reserve(in_offsets.size()); // assume the actual size to be equal or larger
            out_offsets_1.reserve(in_offsets.size());

            for (auto in_offset : in_offsets)
            {
                if (pos < in_offset)
                {
                    pos += !reverse;
                    for (; pos < in_offset - reverse; ++pos)
                        if (cut[pos])
                            out_offsets_2.push_back(pos + reverse);
                    pos += reverse;

                    out_offsets_2.push_back(pos);
                }

                out_offsets_1.push_back(out_offsets_2.size());
            }
        }
        else
        {
            const auto * column_cut_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_cut_const)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected type of cut column");

            if (column_cut_const->getValue<UInt8>())
            {
                out_offsets_2.reserve(in_offsets.back());
                out_offsets_1.reserve(in_offsets.size());

                for (size_t i = 0; i < in_offsets.back(); ++i)
                    out_offsets_2.push_back(i + 1);
                for (auto in_offset : in_offsets)
                    out_offsets_1.push_back(in_offset);
            }
            else
            {
                size_t pos = 0;

                out_offsets_2.reserve(in_offsets.size());
                out_offsets_1.reserve(in_offsets.size());

                for (auto in_offset : in_offsets)
                {
                    if (pos < in_offset)
                    {
                        pos = in_offset;
                        out_offsets_2.push_back(pos);
                    }

                    out_offsets_1.push_back(out_offsets_2.size());
                }
            }
        }

        return ColumnArray::create(
            ColumnArray::create(
                array.getDataPtr(),
                std::move(column_offsets_2)
            ),
            std::move(column_offsets_1)
        );
    }
};

struct NameArraySplit { static constexpr auto name = "arraySplit"; };
struct NameArrayReverseSplit { static constexpr auto name = "arrayReverseSplit"; };
using FunctionArraySplit = FunctionArrayMapped<ArraySplitImpl<false>, NameArraySplit>;
using FunctionArrayReverseSplit = FunctionArrayMapped<ArraySplitImpl<true>, NameArrayReverseSplit>;

REGISTER_FUNCTION(ArraySplit)
{
    FunctionDocumentation::Description description = "Split a source array into multiple arrays. When `func(x [, y1, ..., yN])` returns something other than zero, the array will be split to the left of the element. The array will not be split before the first element.";
    FunctionDocumentation::Syntax syntax = "arraySplit(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).[Lambda function](/sql-reference/functions/overview#arrow-operator-and-lambda)."},
        {"source_arr", "The source array to split [`Array(T)`](/sql-reference/data-types/array)."},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function. [`Array(T)`](/sql-reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of arrays", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples = {{"Usage example", "SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res", "[[1, 2, 3], [4, 5]]"}};
    FunctionDocumentation::IntroducedIn introduced_in = {20, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArraySplit>(documentation);

    FunctionDocumentation::Description description_split = "Split a source array into multiple arrays. When `func(x[, y1, ..., yN])` returns something other than zero, the array will be split to the right of the element. The array will not be split after the last element.";
    FunctionDocumentation::Syntax syntax_split = "arrayReverseSplit(func(x[, y1, ..., yN]), source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_split = {
        {"func(x[, y1, ..., yN])", "A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Lambda function"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_split = {"Returns an array of arrays.", {"Array(Array(T))"}};
    FunctionDocumentation::Examples examples_split = {{"Usage example", "SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res", "[[1], [2, 3, 4], [5]]"}};
    FunctionDocumentation::IntroducedIn introduced_in_split = {20, 1};
    FunctionDocumentation::Category category_split = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_split = {description_split, syntax_split, arguments_split, returned_value_split, examples_split, introduced_in_split, category_split};

    factory.registerFunction<FunctionArrayReverseSplit>(documentation_split);
}

}
