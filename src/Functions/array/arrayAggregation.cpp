#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

enum class AggregateOperation
{
    min,
    max,
    sum,
    average
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
struct ArrayAggregateResultImpl<ArrayElement, AggregateOperation::sum>
{
    using Result =
        std::conditional_t<std::is_same_v<ArrayElement, Int128>, Int128,
            std::conditional_t<std::is_same_v<ArrayElement, Int256>, Int256,
                std::conditional_t<std::is_same_v<ArrayElement, UInt256>, UInt256,
                    std::conditional_t<IsDecimalNumber<ArrayElement>, Decimal128,
                        std::conditional_t<std::is_floating_point_v<ArrayElement>, Float64,
                            std::conditional_t<std::is_signed_v<ArrayElement>, Int64,
                                UInt64>>>>>>;
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

            if constexpr (aggregate_operation == AggregateOperation::average)
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
                result = std::make_shared<DataTypeDecimal<DecimalReturnType>>(DecimalUtils::maxPrecision<DecimalReturnType>(), scale);

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
    static bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        using Result = ArrayAggregateResult<Element, aggregate_operation>;
        using ColVecType = std::conditional_t<IsDecimalNumber<Element>, ColumnDecimal<Element>, ColumnVector<Element>>;
        using ColVecResult = std::conditional_t<IsDecimalNumber<Result>, ColumnDecimal<Result>, ColumnVector<Result>>;

        /// For average of array we return Float64 as result, but we want to keep precision
        /// so we convert to Float64 as last step, but intermediate sum is represented as result of sum operation
        static constexpr bool is_average_operation = aggregate_operation == AggregateOperation::average;
        using SummAggregationType = ArrayAggregateResult<Element, AggregateOperation::sum>;

        using AggregationType = std::conditional_t<is_average_operation, SummAggregationType, Result>;


        const ColVecType * column = checkAndGetColumn<ColVecType>(&*mapped);

        /// Constant case.
        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColVecType>(&*mapped);

            if (!column_const)
                return false;

            const AggregationType x = column_const->template getValue<Element>(); // NOLINT
            const typename ColVecType::Container & data
                = checkAndGetColumn<ColVecType>(&column_const->getDataColumn())->getData();

            typename ColVecResult::MutablePtr res_column;
            if constexpr (IsDecimalNumber<Element>)
            {
                res_column = ColVecResult::create(offsets.size(), data.getScale());
            }
            else
                res_column = ColVecResult::create(offsets.size());

            typename ColVecResult::Container & res = res_column->getData();

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                if constexpr (aggregate_operation == AggregateOperation::sum)
                {
                    size_t array_size = offsets[i] - pos;
                    /// Just multiply the value by array size.
                    res[i] = x * array_size;
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
                        res[i] = DecimalUtils::convertTo<Result>(x, data.getScale());
                    }
                    else
                    {
                        res[i] = x;
                    }
                }

                pos = offsets[i];
            }

            res_ptr = std::move(res_column);
            return true;
        }

        const typename ColVecType::Container & data = column->getData();

        typename ColVecResult::MutablePtr res_column;
        if constexpr (IsDecimalNumber<Element>)
            res_column = ColVecResult::create(offsets.size(), data.getScale());
        else
            res_column = ColVecResult::create(offsets.size());

        typename ColVecResult::Container & res = res_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            AggregationType s = 0;

            /// Array is empty
            if (offsets[i] == pos)
            {
                res[i] = s;
                continue;
            }

            size_t count = 1;
            s = data[pos]; // NOLINT
            ++pos;

            for (; pos < offsets[i]; ++pos)
            {
                auto element = data[pos];

                if constexpr (aggregate_operation == AggregateOperation::sum ||
                            aggregate_operation == AggregateOperation::average)
                {
                    s += element;
                }
                else if constexpr (aggregate_operation == AggregateOperation::min)
                {
                    if (element < s)
                    {
                        s = element;
                    }
                }
                else if constexpr (aggregate_operation == AggregateOperation::max)
                {
                    if (element > s)
                    {
                        s = element;
                    }
                }

                ++count;
            }

            if constexpr (aggregate_operation == AggregateOperation::average)
            {
                if constexpr (IsDecimalNumber<Element>)
                {
                    s = s / count;
                    res[i] = DecimalUtils::convertTo<Result>(s, data.getScale());
                }
                else
                {
                    res[i] = static_cast<Result>(s) / count;
                }
            }
            else
            {
                res[i] = s;
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
            throw Exception("Unexpected column for arraySum: " + mapped->getName(), ErrorCodes::ILLEGAL_COLUMN);
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

void registerFunctionArrayAggregation(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayMin>();
    factory.registerFunction<FunctionArrayMax>();
    factory.registerFunction<FunctionArraySum>();
    factory.registerFunction<FunctionArrayAverage>();
}

}

