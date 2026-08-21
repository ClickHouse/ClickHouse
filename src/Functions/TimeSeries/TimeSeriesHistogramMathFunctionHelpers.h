#pragma once

/// Shared machinery for the native-histogram arithmetic scalar functions, mirroring the
/// histogram arms of `vectorElemBinop` in Prometheus promql/engine.go (see tmp/prom_engine.go).

#include <cstddef>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/TimeSeries/TimeSeriesHistogramFunctionHelpers.h>
#include <Functions/TimeSeries/TimeSeriesHistogramKernel.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Appends one kernel histogram as a payload-tuple row (the encode-side counterpart of
/// TimeSeriesFloatHistogram::fromPayloadTupleRow); the counter reset hint goes into the `flags` byte.
inline void appendTimeSeriesHistogramPayloadRow(const TimeSeriesFloatHistogram & histogram, ColumnTuple & tuple_to)
{
    namespace Idx = TimeSeriesHistogramPayloadTupleIndex;

    auto append_floats = [](const std::vector<Float64> & values, ColumnArray & array_to) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        auto & data_to = typeid_cast<ColumnFloat64 &>(array_to.getData()).getData();
        data_to.insert(values.begin(), values.end());
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? values.size() : offsets_to.back() + values.size());
    };

    auto append_spans = [](const std::vector<TimeSeriesHistogramSpan> & spans, ColumnArray & array_to) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        auto & tuple = typeid_cast<ColumnTuple &>(array_to.getData());
        auto & span_offsets_to = typeid_cast<ColumnInt32 &>(tuple.getColumn(0)).getData();
        auto & span_lengths_to = typeid_cast<ColumnUInt32 &>(tuple.getColumn(1)).getData();
        for (const auto & span : spans)
        {
            span_offsets_to.push_back(span.offset);
            span_lengths_to.push_back(span.length);
        }
        auto & offsets_to = array_to.getOffsets();
        offsets_to.push_back(offsets_to.empty() ? spans.size() : offsets_to.back() + spans.size());
    };

    const UInt8 flags = static_cast<UInt8>(histogram.counter_reset_hint << TimeSeriesHistogramFlags::CounterResetHintShift);
    typeid_cast<ColumnUInt8 &>(tuple_to.getColumn(Idx::Flags)).getData().push_back(flags);
    typeid_cast<ColumnInt8 &>(tuple_to.getColumn(Idx::Schema)).getData().push_back(static_cast<Int8>(histogram.schema));
    typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroThreshold)).getData().push_back(histogram.zero_threshold);
    typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Count)).getData().push_back(histogram.count);
    typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::Sum)).getData().push_back(histogram.sum);
    typeid_cast<ColumnFloat64 &>(tuple_to.getColumn(Idx::ZeroCount)).getData().push_back(histogram.zero_count);
    append_spans(histogram.positive_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveSpans)));
    append_floats(histogram.positive_buckets, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::PositiveValues)));
    append_spans(histogram.negative_spans, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeSpans)));
    append_floats(histogram.negative_buckets, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::NegativeValues)));
    append_floats(histogram.custom_values, typeid_cast<ColumnArray &>(tuple_to.getColumn(Idx::CustomValues)));
}

/// Base class for `timeSeriesHistogramAdd`/`timeSeriesHistogramSub`: NULL when either input is NULL
/// or the schemas are incompatible (upstream drops the sample with ErrHistogramsIncompatibleSchema).
class FunctionTimeSeriesHistogramBinaryMath : public IFunction
{
public:
    explicit FunctionTimeSeriesHistogramBinaryMath(String name_)
        : name(std::move(name_)) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Constant columns are materialized, so executeImpl always sees plain (possibly Nullable) columns.
    bool useDefaultImplementationForConstants() const override { return true; }

    /// Manual NULL handling: NULL is also produced for schema-incompatible inputs.
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(histogram, histogram)",
                            getName(), getName());

        resolvePositions(arguments, 0);
        resolvePositions(arguments, 1);
        return makeNullable(getTimeSeriesHistogramPayloadTupleType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const auto lhs_positions = resolvePositions(arguments, 0);
        const auto rhs_positions = resolvePositions(arguments, 1);

        /// Unwrap the Nullable layers (Nullable payload tuples from the converter, plain tuples in direct calls).
        const ColumnTuple * lhs_tuple = nullptr;
        const ColumnTuple * rhs_tuple = nullptr;
        const UInt8 * lhs_null_map = nullptr;
        const UInt8 * rhs_null_map = nullptr;

        auto unwrap = [this](const ColumnPtr & column, const ColumnTuple *& tuple_column, const UInt8 *& null_map)
        {
            const IColumn * nested = column.get();
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(nested))
            {
                null_map = nullable->getNullMapData().data();
                nested = &nullable->getNestedColumn();
            }
            tuple_column = checkAndGetColumn<ColumnTuple>(nested);
            if (!tuple_column)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Arguments of function {} must be histogram payload tuples, got column {}",
                                getName(), column->getName());
        };
        unwrap(arguments[0].column, lhs_tuple, lhs_null_map);
        unwrap(arguments[1].column, rhs_tuple, rhs_null_map);

        const size_t rows = lhs_tuple->size();
        auto result_tuple = getTimeSeriesHistogramPayloadTupleType()->createColumn();
        auto & result_tuple_concrete = typeid_cast<ColumnTuple &>(*result_tuple);
        auto result_null_map = ColumnUInt8::create();
        auto & result_nulls = result_null_map->getData();
        result_nulls.reserve(rows);

        for (size_t row = 0; row < rows; ++row)
        {
            TimeSeriesFloatHistogram lhs;
            bool is_null = (lhs_null_map && lhs_null_map[row]) || (rhs_null_map && rhs_null_map[row]);
            if (!is_null)
            {
                lhs = TimeSeriesFloatHistogram::fromPayloadTupleRow(*lhs_tuple, lhs_positions, row);
                const TimeSeriesFloatHistogram rhs = TimeSeriesFloatHistogram::fromPayloadTupleRow(*rhs_tuple, rhs_positions, row);

                /// The only upstream error of Add/Sub is ErrHistogramsIncompatibleSchema, which drops the sample.
                is_null = (lhs.usesCustomBuckets() != rhs.usesCustomBuckets());
                if (!is_null)
                {
                    apply(lhs, rhs);
                    lhs.compact(0);
                }
            }

            result_nulls.push_back(is_null);
            if (is_null)
                result_tuple_concrete.insertDefault();
            else
                appendTimeSeriesHistogramPayloadRow(lhs, result_tuple_concrete);
        }

        return ColumnNullable::create(std::move(result_tuple), std::move(result_null_map));
    }

protected:
    /// Applies the arithmetic operation to `lhs` (mirroring FloatHistogram.Add/Sub), WITHOUT the final Compact.
    virtual void apply(TimeSeriesFloatHistogram & lhs, const TimeSeriesFloatHistogram & rhs) const = 0;

private:
    /// Validates that arguments[arg_index] (after unwrapping Nullable) is a named payload tuple and returns the element positions.
    TimeSeriesHistogramPayloadPositions resolvePositions(const ColumnsWithTypeAndName & arguments, size_t arg_index) const
    {
        ColumnsWithTypeAndName unwrapped;
        unwrapped.emplace_back(arguments[arg_index].column, removeNullable(arguments[arg_index].type), arguments[arg_index].name);
        return resolveTimeSeriesHistogramPayloadPositions(unwrapped, getName());
    }

    const String name;
};

/// Base class for `timeSeriesHistogramMulByScalar`/`timeSeriesHistogramDivByScalar`: a payload tuple
/// and a scalar, result the scaled/divided tuple (FloatHistogram.Mul/Div, then Compact(0)).
class FunctionTimeSeriesHistogramScalarMath : public IFunction
{
public:
    explicit FunctionTimeSeriesHistogramScalarMath(String name_)
        : name(std::move(name_)) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Constant columns are materialized, so executeImpl always sees a plain ColumnTuple.
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with two arguments: {}(histogram, scalar)",
                            getName(), getName());

        resolveTimeSeriesHistogramPayloadPositions(arguments, getName());

        if (!isNativeNumber(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Second argument of function {} must be a number, got {}",
                            getName(), arguments[1].type->getName());

        return getTimeSeriesHistogramPayloadTupleType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const auto * tuple_column = checkAndGetColumn<ColumnTuple>(arguments[0].column.get());
        if (!tuple_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "First argument of function {} must be a histogram payload tuple, got column {}",
                            getName(), arguments[0].column->getName());

        const auto positions = resolveTimeSeriesHistogramPayloadPositions(arguments, getName());

        const size_t rows = tuple_column->size();
        auto result_tuple = getTimeSeriesHistogramPayloadTupleType()->createColumn();
        auto & result_tuple_concrete = typeid_cast<ColumnTuple &>(*result_tuple);

        for (size_t row = 0; row < rows; ++row)
        {
            TimeSeriesFloatHistogram histogram = TimeSeriesFloatHistogram::fromPayloadTupleRow(*tuple_column, positions, row);
            /// Field-based conversion accepts any native-number scalar column (the PromQL
            /// converter feeds Float64; direct calls may use integer literals).
            apply(histogram, applyVisitor(FieldVisitorConvertToNumber<Float64>(), (*arguments[1].column)[row]));
            histogram.compact(0);
            appendTimeSeriesHistogramPayloadRow(histogram, result_tuple_concrete);
        }

        return result_tuple;
    }

protected:
    /// Applies the scalar operation to `histogram` (mirroring FloatHistogram.Mul/Div), WITHOUT the final Compact.
    virtual void apply(TimeSeriesFloatHistogram & histogram, Float64 scalar) const = 0;

private:
    const String name;
};

}
