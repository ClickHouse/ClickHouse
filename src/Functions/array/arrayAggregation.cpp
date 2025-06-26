#include <base/defines.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>

#include <Core/callOnTypeIndex.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionFactory.h>

#include "FunctionArrayMapped.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

enum class AggregateOperation : uint8_t
{
    min,
    max,
    sum,
    average,
    product
};

/**
 * During array aggregation we derive result type from operation.
 * For array min or array max we use array element as result type.
 * For array average we use Float64.
 * For array sum for big integers, we use same type representation, decimal numbers up to 128-bit will use Decimal128, then Decimal256.
 * for floating point numbers Float64, for numeric unsigned Int64, and for numeric signed UInt64.
 */

template <typename ArrayElement, AggregateOperation operation>
struct ArrayAggregateResultImpl;

template <typename ArrayElement>
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::min>
{
    using Result = ArrayElement;
};

template <typename ArrayElement>
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::max>
{
    using Result = ArrayElement;
};

template <typename ArrayElement>
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::average>
{
    using Result = Float64;
};

template <typename ArrayElement>
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::product>
{
    using Result = Float64;
};

template <typename ArrayElement>
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::sum>
{
    using Result =
        std::conditional_t<std::is_same_v<ArrayElement, Int128>, Int128,
        std::conditional_t<std::is_same_v<ArrayElement, UInt128>, UInt128,
        std::conditional_t<std::is_same_v<ArrayElement, Int256>, Int256,
        std::conditional_t<std::is_same_v<ArrayElement, UInt256>, UInt256,
        std::conditional_t<std::is_same_v<ArrayElement, Decimal32>, Decimal128,
        std::conditional_t<std::is_same_v<ArrayElement, Decimal64>, Decimal128,
        std::conditional_t<std::is_same_v<ArrayElement, Decimal128>, Decimal128,
        std::conditional_t<std::is_same_v<ArrayElement, Decimal256>, Decimal256,
        std::conditional_t<std::is_same_v<ArrayElement, DateTime64>, Decimal128,
        std::conditional_t<is_floating_point<ArrayElement>, Float64,
        std::conditional_t<std::is_signed_v<ArrayElement>, Int64,
            UInt64>>>>>>>>>>>;
};

template <typename ArrayElement, AggregateOperation operation>
using ArrayAggregateResult = typename ArrayAggregateResultImpl<ArrayElement, operation>::Result;

template<AggregateOperation aggregate_operation>
struct ArrayAggregateImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        if constexpr (aggregate_operation == AggregateOperation::max || aggregate_operation == AggregateOperation::min)
        {
            return expression_return;
        }

        DataTypePtr result;

        auto call = [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            if constexpr (!IsDataTypeDateOrDateTimeOrTime<DataType>)
            {
                if constexpr (aggregate_operation == AggregateOperation::average || aggregate_operation == AggregateOperation::product)
                {
                    result = std::make_shared<DataTypeFloat64>();

                    return true;
                }
                else if constexpr (IsDataTypeNumber<DataType>)
                {
                    using NumberReturnType = ArrayAggregateResult<typename DataType::FieldType, aggregate_operation>;
                    result = std::make_shared<DataTypeNumber<NumberReturnType>>();

                    return true;
                }
                else if constexpr (IsDataTypeDecimal<DataType>)
                {
                    using DecimalReturnType = ArrayAggregateResult<typename DataType::FieldType, aggregate_operation>;
                    UInt32 scale = getDecimalScale(*expression_return);
                    result = std::make_shared<DataTypeDecimal<DecimalReturnType>>(DecimalUtils::max_precision<DecimalReturnType>, scale);

                    return true;
                }
            }
            return false;
        };

        if (!callOnIndexAndDataType<void>(expression_return->getTypeId(), call))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "array aggregation function cannot be performed on type {}",
                expression_return->getName());
        }

        return result;
    }

    template <AggregateOperation op = aggregate_operation>
    requires(op == AggregateOperation::min || op == AggregateOperation::max)
    static void executeMinOrMax(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        const ColumnConst * const_column = checkAndGetColumn<ColumnConst>(&*mapped);
        if (const_column)
        {
            MutableColumnPtr res_column = const_column->getDataColumn().cloneEmpty();
            res_column->insertMany(const_column->getField(), offsets.size());
            res_ptr = std::move(res_column);
            return;
        }

        MutableColumnPtr res_column = mapped->cloneEmpty();
        static constexpr int nan_null_direction_hint = aggregate_operation == AggregateOperation::min ? 1 : -1;

        /// TODO: Introduce row_begin and row_end to getPermutation or an equivalent function to use that instead
        /// (same use case as SingleValueDataBase::getSmallestIndex)
        UInt64 start_of_array = 0;
        for (auto end_of_array : offsets)
        {
            /// Array is empty
            if (start_of_array == end_of_array)
            {
                res_column->insertDefault();
                continue;
            }

            UInt64 index = start_of_array;
            for (UInt64 i = index + 1; i < end_of_array; i++)
            {
                if constexpr (aggregate_operation == AggregateOperation::min)
                {
                    if ((mapped->compareAt(i, index, *mapped, nan_null_direction_hint) < 0))
                        index = i;
                }
                else
                {
                    if ((mapped->compareAt(i, index, *mapped, nan_null_direction_hint) > 0))
                        index = i;
                }
            }

            res_column->insertFrom(*mapped, index);
            start_of_array = end_of_array;
        }

        chassert(res_column->size() == offsets.size());
        res_ptr = std::move(res_column);
    }

    template <typename Element>
    static NO_SANITIZE_UNDEFINED bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        /// Min and Max are implemented in a different function
        static_assert(aggregate_operation != AggregateOperation::min && aggregate_operation != AggregateOperation::max);
        using ResultType = ArrayAggregateResult<Element, aggregate_operation>;
        using ColVecType = ColumnVectorOrDecimal<Element>;
        using ColVecResultType = ColumnVectorOrDecimal<ResultType>;

        /// For average and product of array we return Float64 as result, but we want to keep precision
        /// so we convert to Float64 as last step, but intermediate value is represented as result of sum operation
        static constexpr bool is_average_or_product_operation = aggregate_operation == AggregateOperation::average ||
            aggregate_operation == AggregateOperation::product;
        using SummAggregationType = ArrayAggregateResult<Element, AggregateOperation::sum>;

        using AggregationType = std::conditional_t<is_average_or_product_operation, SummAggregationType, ResultType>;

        const ColVecType * column = checkAndGetColumn<ColVecType>(&*mapped);

        /// Constant case.
        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColVecType>(&*mapped);

            if (!column_const)
                return false;

            const AggregationType x = column_const->template getValue<Element>(); // NOLINT
            const ColVecType * column_typed = checkAndGetColumn<ColVecType>(&column_const->getDataColumn());

            typename ColVecResultType::MutablePtr res_column;
            if constexpr (is_decimal<Element>)
                res_column = ColVecResultType::create(offsets.size(), column_typed->getScale());
            else
                res_column = ColVecResultType::create(offsets.size());

            auto & res = res_column->getData();

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                if constexpr (aggregate_operation == AggregateOperation::sum)
                {
                    size_t array_size = offsets[i] - pos;
                    /// Just multiply the value by array size.
                    res[i] = x * static_cast<ResultType>(array_size);
                }
                else if constexpr (aggregate_operation == AggregateOperation::average)
                {
                    if constexpr (is_decimal<Element>)
                    {
                        res[i] = DecimalUtils::convertTo<ResultType>(x, column_typed->getScale());
                    }
                    else
                    {
                        res[i] = x;
                    }
                }
                else if constexpr (aggregate_operation == AggregateOperation::product)
                {
                    size_t array_size = offsets[i] - pos;
                    AggregationType product = x;

                    if constexpr (is_decimal<Element>)
                    {
                        using T = decltype(x.value);
                        T x_val = x.value;

                        for (size_t array_index = 1; array_index < array_size; ++array_index)
                        {
                            T product_val = product.value;

                            if (common::mulOverflow(x_val, product_val, product.value))
                                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                        }

                        auto result_scale = column_typed->getScale() * array_size;
                        if (unlikely(result_scale > DecimalUtils::max_precision<AggregationType>))
                            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is out of bounds (max scale: {})",
                                            result_scale, DecimalUtils::max_precision<AggregationType>);

                        res[i] = DecimalUtils::convertTo<ResultType>(product, static_cast<UInt32>(result_scale));
                    }
                    else
                    {
                        for (size_t array_index = 1; array_index < array_size; ++array_index)
                            product = product * x;

                        res[i] = product;
                    }
                }

                pos = offsets[i];
            }

            res_ptr = std::move(res_column);
            return true;
        }

        const auto & data = column->getData();

        typename ColVecResultType::MutablePtr res_column;
        if constexpr (is_decimal<Element>)
            res_column = ColVecResultType::create(offsets.size(), column->getScale());
        else
            res_column = ColVecResultType::create(offsets.size());

        typename ColVecResultType::Container & res = res_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            AggregationType aggregate_value{};

            /// Array is empty
            if (offsets[i] == pos)
            {
                if constexpr (is_decimal<AggregationType>)
                    res[i] = aggregate_value.value;
                else
                    res[i] = aggregate_value;
                continue;
            }

            size_t count = 1;
            aggregate_value = data[pos]; // NOLINT
            ++pos;

            for (; pos < offsets[i]; ++pos)
            {
                auto element = data[pos];

                if constexpr (aggregate_operation == AggregateOperation::sum ||
                            aggregate_operation == AggregateOperation::average)
                {
                    aggregate_value += element;
                }
                else if constexpr (aggregate_operation == AggregateOperation::product)
                {
                    if constexpr (is_decimal<Element>)
                    {
                        using AggregateValueDecimalUnderlyingValue = decltype(aggregate_value.value);
                        AggregateValueDecimalUnderlyingValue current_aggregate_value = aggregate_value.value;
                        AggregateValueDecimalUnderlyingValue element_value = static_cast<AggregateValueDecimalUnderlyingValue>(element.value);

                        if (common::mulOverflow(current_aggregate_value, element_value, aggregate_value.value))
                            throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                    }
                    else
                    {
                        aggregate_value *= element;
                    }
                }

                ++count;
            }

            if constexpr (aggregate_operation == AggregateOperation::average)
            {
                if constexpr (is_decimal<Element>)
                {
                    aggregate_value = aggregate_value / AggregationType(count);
                    res[i] = DecimalUtils::convertTo<ResultType>(aggregate_value, column->getScale());
                }
                else
                {
                    res[i] = static_cast<ResultType>(aggregate_value) / count;
                }
            }
            else if constexpr (aggregate_operation == AggregateOperation::product && is_decimal<Element>)
            {
                auto result_scale = column->getScale() * count;

                if (unlikely(result_scale > DecimalUtils::max_precision<AggregationType>))
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is out of bounds (max scale: {})",
                                    result_scale, DecimalUtils::max_precision<AggregationType>);

                res[i] = DecimalUtils::convertTo<ResultType>(aggregate_value, static_cast<UInt32>(result_scale));
            }
            else
            {
                res[i] = aggregate_value;
            }
        }

        res_ptr = std::move(res_column);
        return true;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const IColumn::Offsets & offsets = array.getOffsets();
        ColumnPtr res;

        if constexpr (aggregate_operation == AggregateOperation::min || aggregate_operation == AggregateOperation::max)
        {
            executeMinOrMax(mapped, offsets, res);
            return res;
        }
        else
        {
            if (executeType<UInt8>(mapped, offsets, res) ||
                executeType<UInt16>(mapped, offsets, res) ||
                executeType<UInt32>(mapped, offsets, res) ||
                executeType<UInt64>(mapped, offsets, res) ||
                executeType<UInt128>(mapped, offsets, res) ||
                executeType<UInt256>(mapped, offsets, res) ||
                executeType<Int8>(mapped, offsets, res) ||
                executeType<Int16>(mapped, offsets, res) ||
                executeType<Int32>(mapped, offsets, res) ||
                executeType<Int64>(mapped, offsets, res) ||
                executeType<Int128>(mapped, offsets, res) ||
                executeType<Int256>(mapped, offsets, res) ||
                executeType<Float32>(mapped, offsets, res) ||
                executeType<Float64>(mapped, offsets, res) ||
                executeType<Decimal32>(mapped, offsets, res) ||
                executeType<Decimal64>(mapped, offsets, res) ||
                executeType<Decimal128>(mapped, offsets, res) ||
                executeType<Decimal256>(mapped, offsets, res) ||
                executeType<DateTime64>(mapped, offsets, res))
            {
                return res;
            }
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column for arraySum: {}", mapped->getName());
    }
};

struct NameArrayMin { static constexpr auto name = "arrayMin"; };
using FunctionArrayMin = FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::min>, NameArrayMin>;

struct NameArrayMax { static constexpr auto name = "arrayMax"; };
using FunctionArrayMax = FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::max>, NameArrayMax>;

struct NameArraySum { static constexpr auto name = "arraySum"; };
using FunctionArraySum = FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::sum>, NameArraySum>;

struct NameArrayAverage { static constexpr auto name = "arrayAvg"; };
using FunctionArrayAverage = FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::average>, NameArrayAverage>;

struct NameArrayProduct { static constexpr auto name = "arrayProduct"; };
using FunctionArrayProduct = FunctionArrayMapped<ArrayAggregateImpl<AggregateOperation::product>, NameArrayProduct>;

REGISTER_FUNCTION(ArrayAggregation)
{
    FunctionDocumentation::Description description_min = R"(
Returns the minimum element in the source array.

If a lambda function `func` is specified, returns the minimum element of the lambda results.
    )";
    FunctionDocumentation::Syntax syntax_min = "arrayMin([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_min = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"cond1_arr, ...", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_min = {"Returns the minimum element in the source array, or the minimum element of the lambda results if provided."};
    FunctionDocumentation::Examples examples_min = {
        {"Basic example", "SELECT arrayMin([5, 3, 2, 7]);", "2"},
        {"Usage with lambda function", "SELECT arrayMin(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "4"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_min = {21, 1};
    FunctionDocumentation::Category category_min = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_min = {description_min, syntax_min, arguments_min, returned_value_min, examples_min, introduced_in_min, category_min};

    factory.registerFunction<FunctionArrayMin>(documentation_min);

    FunctionDocumentation::Description description_max = R"(
Returns the maximum element in the source array.

If a lambda function `func` is specified, returns the maximum element of the lambda results.
    )";
    FunctionDocumentation::Syntax syntax_max = "arrayMax([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_max = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_max = {"Returns the maximum element in the source array, or the maximum element of the lambda results if provided."};
    FunctionDocumentation::Examples examples_max = {
        {"Basic example", "SELECT arrayMax([5, 3, 2, 7]);", "7"},
        {"Usage with lambda function", "SELECT arrayMax(x, y -> x/y, [4, 8, 12, 16], [1, 2, 1, 2]);", "12"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_max = {21, 1};
    FunctionDocumentation::Category category_max = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_max = {description_max, syntax_max, arguments_max, returned_value_max, examples_max, introduced_in_max, category_max};

    factory.registerFunction<FunctionArrayMax>(documentation_max);

    FunctionDocumentation::Description description_sum = R"(
Returns the sum of elements in the source array.

If a lambda function `func` is specified, returns the sum of elements of the lambda results.
    )";
    FunctionDocumentation::Syntax syntax_sum = "arrayMax([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_sum = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {", cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_sum = {"Returns the sum of elements in the source array, or the sum of elements of the lambda results if provided."};
    FunctionDocumentation::Examples examples_sum = {
        {"Basic example", "SELECT arraySum([1, 2, 3, 4]);", "10"},
        {"Usage with lambda function", "SELECT arraySum(x, y -> x+y, [1, 1, 1, 1], [1, 1, 1, 1]);", "8"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_sum = {21, 1};
    FunctionDocumentation::Category category_sum = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_sum = {description_sum, syntax_sum, arguments_sum, returned_value_sum, examples_sum, introduced_in_sum, category_sum};

    factory.registerFunction<FunctionArraySum>(documentation_sum);

    FunctionDocumentation::Description description_avg = R"(
Returns the average of elements in the source array.

If a lambda function `func` is specified, returns the average of elements of the lambda results.
    )";
    FunctionDocumentation::Syntax syntax_avg = "arrayAvg([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_avg = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_avg = {"Returns the average of elements in the source array, or the average of elements of the lambda results if provided.", {"Float64"}};
    FunctionDocumentation::Examples examples_avg = {
        {"Basic example", "SELECT arrayAvg([1, 2, 3, 4]);", "2.5"},
        {"Usage with lambda function", "SELECT arrayAvg(x, y -> x*y, [2, 3], [2, 3]) AS res;", "6.5"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_avg = {21, 1};
    FunctionDocumentation::Category category_avg = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_avg = {description_avg, syntax_avg, arguments_avg, returned_value_avg, examples_avg, introduced_in_avg, category_avg};

    factory.registerFunction<FunctionArrayAverage>(documentation_avg);

    FunctionDocumentation::Description description_prod = R"(
Returns the product of elements in the source array.

If a lambda function `func` is specified, returns the product of elements of the lambda results.
    )";
    FunctionDocumentation::Syntax syntax_prod = "arrayProduct([func(x[, y1, ..., yN])], source_arr[, cond1_arr, ... , condN_arr])";
    FunctionDocumentation::Arguments arguments_prod = {
        {"func(x[, y1, ..., yN])", "Optional. A lambda function which operates on elements of the source array (`x`) and condition arrays (`y`).", {"Lambda function"}},
        {"source_arr", "The source array to process.", {"Array(T)"}},
        {"[, cond1_arr, ... , condN_arr]", "Optional. N condition arrays providing additional arguments to the lambda function.", {"Array(T)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_prod = {"Returns the product of elements in the source array, or the product of elements of the lambda results if provided.", {"Float64"}};
    FunctionDocumentation::Examples examples_prod = {
        {"Basic example", "SELECT arrayProduct([1, 2, 3, 4]);", "24"},
        {"Usage with lambda function", "SELECT arrayProduct(x, y -> x+y, [2, 2], [2, 2]) AS res;", "16"},
    };
    FunctionDocumentation::IntroducedIn introduced_in_prod = {21, 1};
    FunctionDocumentation::Category category_prod = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_prod = {description_prod, syntax_prod, arguments_prod, returned_value_prod, examples_prod, introduced_in_prod, category_prod};

    factory.registerFunction<FunctionArrayProduct>(documentation_prod);
}

}
