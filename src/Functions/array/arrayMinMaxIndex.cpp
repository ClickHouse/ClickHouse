#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Functions/array/FunctionArrayMapped.h>
#include <Functions/castTypeToEither.h>

#include <Common/findExtreme.h>

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
        constexpr bool is_min = strategy == ArrayMinMaxIndexStrategy::Min;
        const auto & offsets = array.getOffsets();
        auto result = ColumnUInt32::create(offsets.size());
        auto & result_data = result->getData();

        /// extreme_index(begin, end) returns the absolute index of the first extreme in a non-empty range.
        auto fill = [&](auto && extreme_index)
        {
            size_t begin = 0;
            for (size_t row = 0; row < offsets.size(); ++row)
            {
                const size_t end = offsets[row];
                result_data[row] = begin == end ? 0 : static_cast<UInt32>(extreme_index(begin, end) - begin + 1);
                begin = end;
            }
        };

        const bool handled = castTypeToEither<
            ColumnInt8, ColumnInt16, ColumnInt32, ColumnInt64,
            ColumnUInt8, ColumnUInt16, ColumnUInt32, ColumnUInt64,
            ColumnInt128, ColumnInt256, ColumnUInt128, ColumnUInt256,
            ColumnFloat32, ColumnFloat64,
            ColumnDecimal<Decimal32>, ColumnDecimal<Decimal64>,
            ColumnDecimal<Decimal128>, ColumnDecimal<Decimal256>,
            ColumnDecimal<DateTime64>>(mapped.get(), [&](const auto & column)
        {
            using ValueType = typename std::decay_t<decltype(column)>::ValueType;
            const auto * data = column.getData().data();

            if constexpr (has_find_extreme_index_implementation<ValueType>)
            {
                fill([&](size_t begin, size_t end)
                {
                    return is_min ? *findExtremeMinIndex(data, begin, end) : *findExtremeMaxIndex(data, begin, end);
                });
            }
            else
            {
                /// findExtreme*Index deliberately excludes 128/256-bit integers and decimals because its two-pass scan only pays off when vectorized.
                /// Scan these concrete columns once to avoid the virtual compareAt fallback.
                fill([&](size_t begin, size_t end)
                {
                    size_t best = begin;
                    for (size_t i = begin + 1; i < end; ++i)
                    {
                        if constexpr (is_decimal<ValueType>)
                        {
                            if constexpr (is_min)
                            {
                                if (data[i].value < data[best].value)
                                    best = i;
                            }
                            else
                            {
                                if (data[i].value > data[best].value)
                                    best = i;
                            }
                        }
                        else
                        {
                            if constexpr (is_min)
                            {
                                if (data[i] < data[best])
                                    best = i;
                            }
                            else
                            {
                                if (data[i] > data[best])
                                    best = i;
                            }
                        }
                    }
                    return best;
                });
            }

            return true;
        });

        if (!handled)
        {
            /// Plain floats took the fast path above; NaN only gets here inside `Nullable`.
            /// The direction hint sorts NULL and NaN last, so they only win if nothing else is in the array.
            fill([&](size_t begin, size_t end)
            {
                size_t best = begin;
                for (size_t i = begin + 1; i < end; ++i)
                {
                    const int comparison = mapped->compareAt(i, best, *mapped, is_min ? 1 : -1);
                    if (is_min ? comparison < 0 : comparison > 0)
                        best = i;
                }
                return best;
            });
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
