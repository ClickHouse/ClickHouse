#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Functions/array/FunctionArrayMapped.h>

namespace DB
{

enum class ArrayMinMaxIndexStrategy : uint8_t
{
    Min,
    Max,
};

template <ArrayMinMaxIndexStrategy strategy>
struct ArrayMinMaxIndexImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr &, const DataTypePtr &)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const auto & offsets = array.getOffsets();
        static constexpr int nan_null_direction_hint = strategy == ArrayMinMaxIndexStrategy::Min ? 1 : -1;

        auto result = ColumnUInt32::create(offsets.size());
        auto & result_data = result->getData();
        size_t begin = 0;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            const size_t end = offsets[row];
            if (begin == end)
            {
                result_data[row] = 0;
                begin = end;
                continue;
            }

            size_t best = begin;
            for (size_t i = begin + 1; i < end; ++i)
            {
                const int comparison = mapped->compareAt(i, best, *mapped, nan_null_direction_hint);
                if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
                {
                    if (comparison < 0)
                        best = i;
                }
                else
                {
                    if (comparison > 0)
                        best = i;
                }
            }

            result_data[row] = static_cast<UInt32>(best - begin + 1);
            begin = end;
        }

        return result;
    }
};

struct NameArrayMinIndex { static constexpr auto name = "arrayMinIndex"; };
using FunctionArrayMinIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Min>, NameArrayMinIndex>;

struct NameArrayMaxIndex { static constexpr auto name = "arrayMaxIndex"; };
using FunctionArrayMaxIndex = FunctionArrayMapped<ArrayMinMaxIndexImpl<ArrayMinMaxIndexStrategy::Max>, NameArrayMaxIndex>;

REGISTER_FUNCTION(ArrayMinMaxIndex)
{
    FunctionDocumentation::Description description_min = R"(
Returns the 1-based index of the minimum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the minimum lambda result. If multiple elements have the same minimum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_min = "arrayMinIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_min = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_min = {"Returns the 1-based index of the first minimum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_min = {
        {"Basic example", "SELECT arrayMinIndex([5, 3, 2, 7]);", "3"},
        {"First equal minimum", "SELECT arrayMinIndex([5, 3, 3, 7]);", "2"},
        {"Usage with lambda function", "SELECT arrayMinIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "1"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_min = {26, 10};
    FunctionDocumentation::Category category_min = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_min = {description_min, syntax_min, arguments_min, {}, returned_value_min, examples_min, introduced_in_min, category_min};
    factory.registerFunction<FunctionArrayMinIndex>(documentation_min);

    FunctionDocumentation::Description description_max = R"(
Returns the 1-based index of the maximum element in the source array, or `0` if the array is empty.

If a lambda function `func` is specified, returns the index of the element corresponding to the maximum lambda result. If multiple elements have the same maximum value, returns the index of the first one.
    )";
    FunctionDocumentation::Syntax syntax_max = "arrayMaxIndex([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_max = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_max = {"Returns the 1-based index of the first maximum element, or `0` for an empty array.", {"UInt32"}};
    FunctionDocumentation::Examples examples_max = {
        {"Basic example", "SELECT arrayMaxIndex([5, 3, 2, 7]);", "4"},
        {"First equal maximum", "SELECT arrayMaxIndex([5, 7, 7, 3]);", "2"},
        {"Usage with lambda function", "SELECT arrayMaxIndex(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "3"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_max = {26, 10};
    FunctionDocumentation::Category category_max = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_max = {description_max, syntax_max, arguments_max, {}, returned_value_max, examples_max, introduced_in_max, category_max};
    factory.registerFunction<FunctionArrayMaxIndex>(documentation_max);
}

}
