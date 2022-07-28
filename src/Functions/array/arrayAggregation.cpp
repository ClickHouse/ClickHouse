#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>
#include <common/defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int DECIMAL_OVERFLOW;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

enum class AggregateOperation
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
 * For array sum for for big integers, we use same type representation, decimal numbers we use Decimal128,
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
        std::conditional_t<IsDecimalNumber<ArrayElement>, Decimal128,
        std::conditional_t<std::is_floating_point_v<ArrayElement>, Float64,
        std::conditional_t<std::is_signed_v<ArrayElement>, Int64,
            UInt64>>>>>>>;
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
        DataTypePtr result;

        auto call = [&](const auto & types)
        {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

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
            else if constexpr (IsDataTypeDecimal<DataType> && !IsDataTypeDateOrDateTime<DataType>)
            {
                using DecimalReturnType = ArrayAggregateResult<typename DataType::FieldType, aggregate_operation>;
                UInt32 scale = getDecimalScale(*expression_return);
                result = std::make_shared<DataTypeDecimal<DecimalReturnType>>(DecimalUtils::max_precision<DecimalReturnType>, scale);

                return true;
            }

            return false;
        };

        if (!callOnIndexAndDataType<void>(expression_return->getTypeId(), call))
        {
            throw Exception(
                "array aggregation function cannot be performed on type " + expression_return->getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return result;
    }

    template <typename Element>
    static NO_SANITIZE_UNDEFINED bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        using ResultType = ArrayAggregateResult<Element, aggregate_operation>;
        using ColVecType = std::conditional_t<IsDecimalNumber<Element>, ColumnDecimal<Element>, ColumnVector<Element>>;
        using ColVecResultType = std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>, ColumnVector<ResultType>>;

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
            const auto & data = checkAndGetColumn<ColVecType>(&column_const->getDataColumn())->getData();

            typename ColVecResultType::MutablePtr res_column;
            if constexpr (IsDecimalNumber<Element>)
                res_column = ColVecResultType::create(offsets.size(), data.getScale());
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
                    res[i] = x * ResultType(array_size);
                }
                else if constexpr (aggregate_operation == AggregateOperation::min ||
                                aggregate_operation == AggregateOperation::max)
                {
                    res[i] = x;
                }
                else if constexpr (aggregate_operation == AggregateOperation::average)
                {
                    if constexpr (IsDecimalNumber<Element>)
                    {
                        res[i] = DecimalUtils::convertTo<ResultType>(x, data.getScale());
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

                    if constexpr (IsDecimalNumber<Element>)
                    {
                        using T = decltype(x.value);
                        T x_val = x.value;

                        for (size_t array_index = 1; array_index < array_size; ++array_index)
                        {
                            T product_val = product.value;

                            if (common::mulOverflow(x_val, product_val, product.value))
                                throw Exception(ErrorCodes::DECIMAL_OVERFLOW, "Decimal math overflow");
                        }

                        auto result_scale = data.getScale() * array_size;
                        if (unlikely(result_scale > DecimalUtils::max_precision<AggregationType>))
                            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is out of bounds", result_scale);

                        res[i] = DecimalUtils::convertTo<ResultType>(product, data.getScale() * array_size);
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
        if constexpr (IsDecimalNumber<Element>)
            res_column = ColVecResultType::create(offsets.size(), data.getScale());
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
                if constexpr (IsDecimalNumber<AggregationType>)
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
                else if constexpr (aggregate_operation == AggregateOperation::min)
                {
                    if (element < aggregate_value)
                    {
                        aggregate_value = element;
                    }
                }
                else if constexpr (aggregate_operation == AggregateOperation::max)
                {
                    if (element > aggregate_value)
                    {
                        aggregate_value = element;
                    }
                }
                else if constexpr (aggregate_operation == AggregateOperation::product)
                {
                    if constexpr (IsDecimalNumber<Element>)
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
                if constexpr (IsDecimalNumber<Element>)
                {
                    aggregate_value = aggregate_value / AggregationType(count);
                    res[i] = DecimalUtils::convertTo<ResultType>(aggregate_value, data.getScale());
                }
                else
                {
                    res[i] = static_cast<ResultType>(aggregate_value) / count;
                }
            }
            else if constexpr (aggregate_operation == AggregateOperation::product && IsDecimalNumber<Element>)
            {
                auto result_scale = data.getScale() * count;

                if (unlikely(result_scale > DecimalUtils::max_precision<AggregationType>))
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Scale {} is out of bounds", result_scale);

                res[i] = DecimalUtils::convertTo<ResultType>(aggregate_value, result_scale);
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
            executeType<Decimal128>(mapped, offsets, res))
            return res;
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Unexpected column for arraySum: {}" + mapped->getName());
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

void registerFunctionArrayAggregation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayMin>();
    factory.registerFunction<FunctionArrayMax>();
    factory.registerFunction<FunctionArraySum>();
    factory.registerFunction<FunctionArrayAverage>();
    factory.registerFunction<FunctionArrayProduct>();
}

}

