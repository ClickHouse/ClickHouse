#include <base/defines.h>
#include <base/types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include <Functions/array/FunctionArrayMapped.h>

#include <Common/NaNUtils.h>
#include <Common/findExtreme.h>

#include <optional>
#include <type_traits>
#include <utility>

namespace DB
{

enum class ArrayMinMaxIndexStrategy : uint8_t
{
    Min,
    Max,
};

namespace ArrayMinMaxIndexImpl
{

template <ArrayMinMaxIndexStrategy strategy, typename T>
static bool isBetter(const T & lhs, const T & rhs)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return lhs < rhs;
    else
        return lhs > rhs;
}

/// Keep the one-pass implementation for short arrays and types without a vectorized findExtreme implementation.
template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndexOnePass(const T * data, size_t size)
{
    size_t index = 0;

    if constexpr (std::is_floating_point_v<T>)
    {
        while (index < size && isNaN(data[index]))
            ++index;

        if (index == size)
            return 0;
    }

    T best = data[index];
    for (size_t i = index + 1; i < size; ++i)
    {
        if (isBetter<strategy>(data[i], best))
        {
            best = data[i];
            index = i;
        }
    }

    return index;
}

/// Use the existing vectorized value reduction and then find the first match.
template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
static std::optional<T> findExtremeValue(const T * data, size_t begin, size_t end)
{
    if constexpr (strategy == ArrayMinMaxIndexStrategy::Min)
        return findExtremeMin(data, begin, end);
    else
        return findExtremeMax(data, begin, end);
}

template <typename T>
static size_t findFirstExtreme(const T * data, size_t size, const T & extreme)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        if (isNaN(extreme))
        {
            for (size_t i = 0; i < size; ++i)
                if (isNaN(data[i]))
                    return i;
            return size;
        }
    }

    for (size_t i = 0; i < size; ++i)
        if (data[i] == extreme)
            return i;
    return size;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
requires(has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
static size_t findIndexUsingExtreme(const T * data, size_t size)
{
    const auto extreme = findExtremeValue<strategy>(data, 0, size);
    chassert(extreme.has_value());

    const size_t index = findFirstExtreme(data, size, *extreme);
    chassert(index < size);
    return index;
}

template <ArrayMinMaxIndexStrategy strategy, typename T>
static size_t findIndex(const T * data, size_t size)
{
    constexpr size_t min_size_for_extreme_reduction = 32;
    if constexpr (has_find_extreme_index_implementation<T>)
    {
        /// The existing reduction is vectorized; for short arrays a single indexed scan is cheaper.
        if (size >= min_size_for_extreme_reduction)
            return findIndexUsingExtreme<strategy>(data, size);
    }

    return findIndexOnePass<strategy>(data, size);
}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
static void executeNumericData(const Element * data, const ColumnArray::Offsets & offsets, ColumnUInt32::Container & result)
{
    size_t begin = 0;
    for (size_t row = 0; row < offsets.size(); ++row)
    {
        const size_t end = offsets[row];
        const size_t size = end - begin;
        result[row] = size == 0 ? 0 : static_cast<UInt32>(findIndex<strategy>(data + begin, size) + 1);
        begin = end;
    }
}

template <ArrayMinMaxIndexStrategy strategy, typename Element>
static bool executeNumeric(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);
    if (!column)
        return false;

    auto result_column = ColumnUInt32::create(offsets.size());
    executeNumericData<strategy, Element>(column->getData().data(), offsets, result_column->getData());
    result_ptr = std::move(result_column);
    return true;
}

template <ArrayMinMaxIndexStrategy strategy, typename Decimal>
requires(is_decimal<Decimal>)
static bool executeDecimal(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & result_ptr)
{
    const auto * column = checkAndGetColumn<ColumnDecimal<Decimal>>(&*mapped);
    if (!column)
        return false;

    using Element = typename Decimal::NativeType;
    static_assert(sizeof(Decimal) == sizeof(Element));
    static_assert(alignof(Decimal) == alignof(Element));

    auto result_column = ColumnUInt32::create(offsets.size());
    const auto * data = reinterpret_cast<const Element *>(column->getData().data());
    executeNumericData<strategy, Element>(data, offsets, result_column->getData());
    result_ptr = std::move(result_column);
    return true;
}

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

        if (checkAndGetColumn<ColumnConst>(&*mapped))
        {
            auto result = ColumnUInt32::create(offsets.size());
            auto & result_data = result->getData();
            size_t begin = 0;
            for (size_t row = 0; row < offsets.size(); ++row)
            {
                const size_t end = offsets[row];
                result_data[row] = begin == end ? 0 : 1;
                begin = end;
            }
            return result;
        }

        ColumnPtr numeric_result;
        if (executeNumeric<strategy, UInt8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt64>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int8>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int16>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int64>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt128>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, UInt256>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int128>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Int256>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float32>(mapped, offsets, numeric_result)
            || executeNumeric<strategy, Float64>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal32>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal64>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal128>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, Decimal256>(mapped, offsets, numeric_result)
            || executeDecimal<strategy, DateTime64>(mapped, offsets, numeric_result))
            return numeric_result;

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
                if (isBetter<strategy>(comparison, 0))
                    best = i;
            }

            result_data[row] = static_cast<UInt32>(best - begin + 1);
            begin = end;
        }

        return result;
    }
};

}

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
    FunctionDocumentation::IntroducedIn introduced_in_min = {26, 9};
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
    FunctionDocumentation::IntroducedIn introduced_in_max = {26, 9};
    FunctionDocumentation::Category category_max = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_max = {description_max, syntax_max, arguments_max, {}, returned_value_max, examples_max, introduced_in_max, category_max};
    factory.registerFunction<FunctionArrayMaxIndex>(documentation_max);
}

}
