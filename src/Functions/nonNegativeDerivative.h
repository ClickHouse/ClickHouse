#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeInterval.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Calculate derivative of given value column by specified timestamp. */
class FunctionNonNegativeDerivativeImpl : public IFunction
{
private:
    static NO_SANITIZE_UNDEFINED Int64 getResultScaling()
    {
        Int64 res = 1;

        return res;
    }

    template <typename Src, typename Dst>
    static NO_SANITIZE_UNDEFINED void process(const PaddedPODArray<Src> & metric, PaddedPODArray<Dst> & result,
                                              const PaddedPODArray<DataTypeDateTime64> & timestamp, const UInt32 ts_scale,
                                              const std::tuple<IntervalKind::Kind, Int64> interval, const NullMap * null_map)
    {
        size_t size = metric.size();
        result.resize(size);

        if (size == 0)
            return;

        Src prev_metric_value{};
        DateTime64 prev_ts_value{};

        bool first_row = false;

        for (size_t i = 0; i < size; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                result[i] = Dst{};
                continue;
            }

            if (!first_row)
            {
                auto cur = metric[i];
                /// Overflow is Ok.
                result[i] = static_cast<Dst>(cur) - prev_metric_value;
                prev_metric_value = cur;
            }
            else
            {
                result[i] = 0;
                prev_metric_value = metric[i];
                first_row = false;
            }
        }
    }

    /// Result type is same as result of subtraction of argument types.
    template <typename SrcFieldType>
    using DstFieldType = typename NumberTraits::ResultOfSubtraction<SrcFieldType, SrcFieldType>::Type;

    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        WhichDataType which(src_type);

        if (which.isUInt8())
            f(UInt8());
        else if (which.isUInt16())
            f(UInt16());
        else if (which.isUInt32())
            f(UInt32());
        else if (which.isUInt64())
            f(UInt64());
        else if (which.isInt8())
            f(Int8());
        else if (which.isInt16())
            f(Int16());
        else if (which.isInt32())
            f(Int32());
        else if (which.isInt64())
            f(Int64());
        else if (which.isFloat32())
            f(Float32());
        else if (which.isFloat64())
            f(Float64());
        else if (which.isDate())
            f(DataTypeDate::FieldType());
        else if (which.isDate32())
            f(DataTypeDate::FieldType());
        else if (which.isDateTime())
            f(DataTypeDateTime::FieldType());
        else
            throw Exception("First argument for function " + getName() + " must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    static constexpr auto name = "nonNegativeDerivative";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionNonNegativeDerivativeImpl>();
    }

    String getName() const override
    {
        return name;
    }

    bool isStateful() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        DataTypePtr res;
        dispatchForSourceType(*removeNullable(arguments[0]), [&](auto field_type_tag)
                              {
                                  res = std::make_shared<DataTypeNumber<DstFieldType<decltype(field_type_tag)>>>();
                              });

        if (arguments[0]->isNullable())
            res = makeNullable(res);

        return res;
    }

    static std::tuple<IntervalKind::Kind, Int64>
    dispatchForIntervalColumns(const ColumnWithTypeAndName & interval_column)
    {
        const auto * interval_type = checkAndGetDataType<DataTypeInterval>(interval_column.type.get());
        if (!interval_type)
            throw Exception("Illegal value" + interval_column.name + "for function nonNegativeDerivative, INTERVAL expected",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto * interval_column_const_int64 = checkAndGetColumnConst<ColumnInt64>(interval_column.column.get());
        if (!interval_column_const_int64)
            throw Exception("Illegal value " + interval_column.name + "for function nonNegativeDerivative, INTERVAL expected",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        Int64 num_units = interval_column_const_int64->getValue<Int64>();
        if (num_units <= 0)
            throw Exception("Value for column " + interval_column.name + "for function nonNegativeDerivative must be positive",
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        return {interval_type->getKind(), num_units};
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() < 2 || arguments.size() > 3)
            throw Exception("Invalid number of arguments, expected 2 or 3: nonNegativeDerivative(metric, timestamp[, INTERVAL x SECOND])",
                            DB::ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto & metric = arguments.at(0);
        const auto & timestamp = arguments.at(1);

        const auto timestamp_scale = assert_cast<const DataTypeDateTime64 &>(*arguments[0].type).getScale();

        // Default interval value: INTERVAL 1 SECOND
        const auto interval_params = arguments.size() == 3 ? dispatchForIntervalColumns(arguments.at(2)) : std::tuple<IntervalKind::Kind, Int64>(IntervalKind::Second, 1);

        /// When column is constant, its derivative is 0.
        if (isColumnConst(*metric.column))
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        auto res_column = removeNullable(result_type)->createColumn();

        const auto * metric_column = metric.column.get();
        const auto * timestamp_column = timestamp.column.get();

        ColumnPtr null_map_column = nullptr;
        const NullMap * null_map = nullptr;
        if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(metric_column))
        {
            metric_column = &nullable_column->getNestedColumn();
            null_map_column = nullable_column->getNullMapColumnPtr();
            null_map = &nullable_column->getNullMapData();
        }

        dispatchForSourceType(*removeNullable(metric.type), [&](auto field_type_tag)
                              {
                                  using MetricFieldType = decltype(field_type_tag);

                                  process(assert_cast<const ColumnVector<MetricFieldType> &>(*metric_column).getData(),
                                          assert_cast<ColumnVector<DstFieldType<MetricFieldType>> &>(*res_column).getData(),
                                          assert_cast<const ColumnVector<decltype(DataTypeDateTime64(timestamp_scale))> &>(*timestamp_column).getData(),
                                          timestamp_scale, interval_params, null_map);
                              });

        if (null_map_column)
            return ColumnNullable::create(std::move(res_column), null_map_column);
        else
            return res_column;
    }
};

}
