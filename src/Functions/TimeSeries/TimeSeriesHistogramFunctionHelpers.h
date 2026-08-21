#pragma once

#include <array>
#include <cmath>
#include <limits>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Base class for the scalar functions extracting one Float64 element of a native-histogram payload tuple
/// (`timeSeriesHistogramCount`/`timeSeriesHistogramSum`); the by-name lookup lets the tuple layout evolve (append-only).
class FunctionTimeSeriesHistogramElement : public IFunction
{
public:
    FunctionTimeSeriesHistogramElement(String name_, String element_name_)
        : name(std::move(name_)), element_name(std::move(element_name_)) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Constant columns are materialized, so executeImpl always sees a plain ColumnTuple.
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        getElementPosition(arguments);
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const auto * tuple_column = checkAndGetColumn<ColumnTuple>(arguments[0].column.get());
        if (!tuple_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Argument of function {} must be a histogram payload tuple, got column {}",
                            getName(), arguments[0].column->getName());

        /// Zero-copy: the result is the element column itself.
        return tuple_column->getColumnPtr(getElementPosition(arguments));
    }

private:
    /// Validates that the argument is a named tuple containing the element of type Float64 and returns
    /// the element's position. The lookup is by name; the position is never hardcoded.
    size_t getElementPosition(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with one argument: {}(histogram)",
                            getName(), getName());

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].type.get());
        if (!tuple_type || !tuple_type->hasExplicitNames())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument of function {} must be a named tuple (a histogram payload), got {}",
                            getName(), arguments[0].type->getName());

        auto position = tuple_type->tryGetPositionByName(element_name);
        if (!position)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument of function {} must be a histogram payload tuple containing the element `{}`, got {}",
                            getName(), element_name, arguments[0].type->getName());

        if (!WhichDataType(tuple_type->getElements()[*position]).isFloat64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Element `{}` of the histogram payload tuple must be Float64, got {} in {}",
                            element_name, tuple_type->getElements()[*position]->getName(), arguments[0].type->getName());

        return *position;
    }

    const String name;
    const String element_name;
};

/// Positions of the elements of a histogram payload tuple inside a concrete tuple column,
/// indexed by TimeSeriesHistogramPayloadTupleIndex.
using TimeSeriesHistogramPayloadPositions = std::array<size_t, TimeSeriesHistogramPayloadTupleIndex::Size>;

/// Validates that arguments[0] is a named tuple containing all 11 elements of getTimeSeriesHistogramPayloadTupleType
/// with the canonical types (at any positions) and returns the element positions. Shared by the function bases below.
inline TimeSeriesHistogramPayloadPositions resolveTimeSeriesHistogramPayloadPositions(
    const ColumnsWithTypeAndName & arguments, const String & function_name)
{
    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arguments[0].type.get());
    if (!tuple_type || !tuple_type->hasExplicitNames())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Argument of function {} must be a named tuple (a histogram payload), got {}",
                        function_name, arguments[0].type->getName());

    const auto payload_tuple = std::static_pointer_cast<const DataTypeTuple>(getTimeSeriesHistogramPayloadTupleType());

    TimeSeriesHistogramPayloadPositions positions;
    for (size_t i = 0; i < TimeSeriesHistogramPayloadTupleIndex::Size; ++i)
    {
        const auto & element_name = payload_tuple->getElementNames()[i];
        auto position = tuple_type->tryGetPositionByName(element_name);
        if (!position)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument of function {} must be a histogram payload tuple containing the element `{}`, got {}",
                            function_name, element_name, arguments[0].type->getName());

        if (!tuple_type->getElements()[*position]->equals(*payload_tuple->getElements()[i]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Element `{}` of the histogram payload tuple must be {}, got {} in {}",
                            element_name, payload_tuple->getElements()[i]->getName(),
                            tuple_type->getElements()[*position]->getName(), arguments[0].type->getName());

        positions[i] = *position;
    }
    return positions;
}

/// Base class for the scalar functions combining several elements of a native-histogram payload tuple
/// (`timeSeriesHistogramAvg`/`timeSeriesHistogramStddev`/`timeSeriesHistogramStdvar`); all elements are looked up BY NAME and validated.
class FunctionTimeSeriesHistogramStatistic : public IFunction
{
public:
    explicit FunctionTimeSeriesHistogramStatistic(String name_)
        : name(std::move(name_)) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Constant columns are materialized, so executeImpl always sees a plain ColumnTuple.
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        resolveElementPositions(arguments);
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const auto * tuple_column = checkAndGetColumn<ColumnTuple>(arguments[0].column.get());
        if (!tuple_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Argument of function {} must be a histogram payload tuple, got column {}",
                            getName(), arguments[0].column->getName());

        const auto element_positions = resolveElementPositions(arguments);

        const size_t rows = tuple_column->size();
        auto result_column = ColumnFloat64::create(rows);
        auto & result_data = result_column->getData();
        for (size_t row = 0; row < rows; ++row)
            result_data[row] = computeRow(*tuple_column, element_positions, row);
        return result_column;
    }

protected:
    /// Computes the statistic for one row of the payload tuple. `element_positions` maps a payload
    /// element index (TimeSeriesHistogramPayloadTupleIndex) to the element's position in `tuple_column`.
    virtual Float64 computeRow(
        const ColumnTuple & tuple_column,
        const TimeSeriesHistogramPayloadPositions & element_positions,
        size_t row) const = 0;

private:
    /// Validates that the argument is a named tuple containing all 11 payload elements with the canonical
    /// types (at any positions) and returns the element positions.
    TimeSeriesHistogramPayloadPositions resolveElementPositions(const ColumnsWithTypeAndName & arguments) const
    {
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with one argument: {}(histogram)",
                            getName(), getName());

        return resolveTimeSeriesHistogramPayloadPositions(arguments, getName());
    }

    const String name;
};

/// One bucket of a native histogram with its boundaries resolved (see `walkTimeSeriesHistogramBuckets`); the zero
/// value mirrors the zero Bucket{} of Prometheus promql/quantile.go (when the walk never advances the iterator).
struct TimeSeriesHistogramResolvedBucket
{
    Float64 lower = 0;
    Float64 upper = 0;
    Float64 count = 0;
};

/// The bucket walk over one native-histogram payload row, shared by `timeSeriesHistogramQuantile`
/// and `timeSeriesHistogramFraction`.
struct TimeSeriesHistogramBucketWalk
{
    bool custom_buckets = false;
    /// Whether the histogram has any negative / positive buckets at all (span-covered buckets are
    /// counted even when their count is 0, like `len(h.NegativeBuckets)` in Prometheus).
    bool has_negative_buckets = false;
    bool has_positive_buckets = false;
    /// The buckets in `AllBucketIterator` order (negative from the most negative one up, then the zero bucket
    /// when zero_count > 0, then positive ascending), mirrored as `AllReverseBucketIterator` when `reverse_order`.
    std::vector<TimeSeriesHistogramResolvedBucket> buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
};

/// Resolves the buckets of one payload row for the quantile/fraction walks, mirroring `allFloatBucketIterator`: bounds
/// overlapping the zero bucket clamp to the zero threshold, custom bounds come from `custom_values`; custom histograms have no negative side.
inline TimeSeriesHistogramBucketWalk walkTimeSeriesHistogramBuckets(
    const ColumnTuple & tuple_column,
    const TimeSeriesHistogramPayloadPositions & element_positions,
    size_t row,
    bool reverse_order)
{
    const Int32 schema = static_cast<Int32>(
        tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Schema]).getInt(row));
    const bool custom_buckets = (schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
    if (!custom_buckets && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an invalid bucket schema: {}", schema);

    const Float64 zero_threshold
        = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::ZeroThreshold]).getFloat64(row);
    const Float64 zero_count
        = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::ZeroCount]).getFloat64(row);

    const auto positive_buckets = expandHistogramSpans(
        tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::PositiveSpans]),
        tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::PositiveValues]),
        row);

    std::vector<HistogramBucket> negative_buckets; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    if (!custom_buckets)
    {
        negative_buckets = expandHistogramSpans(
            tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::NegativeSpans]),
            tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::NegativeValues]),
            row);
    }

    const auto & custom_values_array = typeid_cast<const ColumnArray &>(
        tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::CustomValues]));
    const auto & custom_values_offsets = custom_values_array.getOffsets();
    const size_t custom_values_begin = (row == 0) ? 0 : custom_values_offsets[row - 1];
    const size_t num_custom_values = custom_values_offsets[row] - custom_values_begin;
    const auto & custom_values_data = custom_values_array.getData();

    auto custom_bound = [&](Int64 idx) -> Float64
    {
        if (idx < 0)
            return -std::numeric_limits<Float64>::infinity();
        const auto uidx = static_cast<UInt64>(idx);
        if (uidx >= num_custom_values)
        {
            if (uidx == num_custom_values)
                return std::numeric_limits<Float64>::infinity();
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Native histogram bucket index {} is out of bounds for {} custom bucket bounds",
                idx, num_custom_values);
        }
        return custom_values_data.getFloat64(custom_values_begin + uidx);
    };

    auto resolve_positive = [&](const HistogramBucket & bucket) -> TimeSeriesHistogramResolvedBucket
    {
        Float64 lower = 0;
        Float64 upper = 0;
        if (custom_buckets)
        {
            lower = custom_bound(bucket.index - 1);
            upper = custom_bound(bucket.index);
        }
        else
        {
            lower = getHistogramBoundExponential(bucket.index - 1, schema);
            upper = getHistogramBoundExponential(bucket.index, schema);
        }
        if (lower > 0 && lower < zero_threshold)
            lower = zero_threshold;
        return TimeSeriesHistogramResolvedBucket{lower, upper, bucket.count};
    };

    auto resolve_negative = [&](const HistogramBucket & bucket) -> TimeSeriesHistogramResolvedBucket
    {
        Float64 lower = -getHistogramBoundExponential(bucket.index, schema);
        Float64 upper = -getHistogramBoundExponential(bucket.index - 1, schema);
        if (upper < 0 && upper > -zero_threshold)
            upper = -zero_threshold;
        return TimeSeriesHistogramResolvedBucket{lower, upper, bucket.count};
    };

    TimeSeriesHistogramBucketWalk walk;
    walk.custom_buckets = custom_buckets;
    walk.has_positive_buckets = !positive_buckets.empty();
    walk.has_negative_buckets = !negative_buckets.empty();
    walk.buckets.reserve(positive_buckets.size() + negative_buckets.size() + (zero_count > 0 ? 1 : 0));

    if (!reverse_order)
    {
        for (auto it = negative_buckets.rbegin(); it != negative_buckets.rend(); ++it)
            walk.buckets.push_back(resolve_negative(*it));
        if (zero_count > 0)
            walk.buckets.push_back(TimeSeriesHistogramResolvedBucket{-zero_threshold, zero_threshold, zero_count});
        for (const auto & bucket : positive_buckets)
            walk.buckets.push_back(resolve_positive(bucket));
    }
    else
    {
        for (auto it = positive_buckets.rbegin(); it != positive_buckets.rend(); ++it)
            walk.buckets.push_back(resolve_positive(*it));
        if (zero_count > 0)
            walk.buckets.push_back(TimeSeriesHistogramResolvedBucket{-zero_threshold, zero_threshold, zero_count});
        for (const auto & bucket : negative_buckets)
            walk.buckets.push_back(resolve_negative(bucket));
    }

    return walk;
}

/// Base class for `timeSeriesHistogramQuantile(histogram, phi)` and `timeSeriesHistogramFraction(histogram, lower, upper)`:
/// a validated payload tuple plus constant native-number scalar parameters (enforced by `getArgumentsThatAreAlwaysConstant`).
class FunctionTimeSeriesHistogramWithScalarParams : public IFunction
{
public:
    FunctionTimeSeriesHistogramWithScalarParams(String name_, size_t num_scalar_params_)
        : name(std::move(name_)), num_scalar_params(num_scalar_params_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1 + num_scalar_params; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        ColumnNumbers arguments(num_scalar_params);
        for (size_t i = 0; i < num_scalar_params; ++i)
            arguments[i] = 1 + i;
        return arguments;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Constant columns are materialized, so executeImpl always sees a plain ColumnTuple
    /// (the scalar parameters stay ColumnConst, see getArgumentsThatAreAlwaysConstant).
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 1 + num_scalar_params)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} must be called with {} arguments, got {}",
                            name, 1 + num_scalar_params, arguments.size());

        resolveTimeSeriesHistogramPayloadPositions(arguments, name);

        for (size_t i = 0; i < num_scalar_params; ++i)
        {
            const auto & scalar_argument = arguments[1 + i];
            if (!isNativeNumber(scalar_argument.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Argument {} of function {} must be a constant number, got {}",
                                i + 2, name, scalar_argument.type->getName());
            if (scalar_argument.column && !isColumnConst(*scalar_argument.column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Argument {} of function {} must be a constant, got column {}",
                                i + 2, name, scalar_argument.column->getName());
        }

        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        const auto * tuple_column = checkAndGetColumn<ColumnTuple>(arguments[0].column.get());
        if (!tuple_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Argument of function {} must be a histogram payload tuple, got column {}",
                            name, arguments[0].column->getName());

        const auto element_positions = resolveTimeSeriesHistogramPayloadPositions(arguments, name);

        const size_t rows = tuple_column->size();
        auto result_column = ColumnFloat64::create(rows);
        auto & result_data = result_column->getData();
        for (size_t row = 0; row < rows; ++row)
            result_data[row] = computeRow(*tuple_column, element_positions, arguments, row);
        return result_column;
    }

protected:
    /// Computes the function for one row of the payload tuple: `element_positions` maps a payload element index
    /// to the element's position in `tuple_column`; the constant scalar parameters are arguments[1..] read as Float64 per row.
    virtual Float64 computeRow(
        const ColumnTuple & tuple_column,
        const TimeSeriesHistogramPayloadPositions & element_positions,
        const ColumnsWithTypeAndName & arguments,
        size_t row) const = 0;

private:
    const String name;
    const size_t num_scalar_params;
};

/// Base class for `timeSeriesHistogramStddev`/`timeSeriesHistogramStdvar`, mirroring `histogramVariance` in promql/functions.go: each populated bucket
/// counts at a representative value, the squared deviations from `sum`/`count` are Kahan-summed; `transformResult` is sqrt for stddev, identity for stdvar.
class FunctionTimeSeriesHistogramVariance : public FunctionTimeSeriesHistogramStatistic
{
public:
    explicit FunctionTimeSeriesHistogramVariance(String name_)
        : FunctionTimeSeriesHistogramStatistic(std::move(name_)) {}

protected:
    virtual Float64 transformResult(Float64 variance) const = 0;

    Float64 computeRow(
        const ColumnTuple & tuple_column,
        const TimeSeriesHistogramPayloadPositions & element_positions,
        size_t row) const override
    {
        const Int32 schema = static_cast<Int32>(
            tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Schema]).getInt(row));
        const bool custom_buckets = (schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
        if (!custom_buckets && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an invalid bucket schema: {}", schema);

        const Float64 zero_threshold
            = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::ZeroThreshold]).getFloat64(row);
        const Float64 count = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Count]).getFloat64(row);
        const Float64 sum = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::Sum]).getFloat64(row);
        const Float64 zero_count
            = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::ZeroCount]).getFloat64(row);

        const Float64 mean = sum / count;
        Float64 variance = 0;
        Float64 c_variance = 0;

        auto process_bucket = [&](Float64 lower, Float64 upper, Float64 bucket_count)
        {
            if (bucket_count == 0)
                return;
            /// A boundary overlapping the zero bucket is clamped to the zero threshold
            /// (mirrors `allFloatBucketIterator` in Prometheus model/histogram/float_histogram.go).
            if (upper < 0 && upper > -zero_threshold)
                upper = -zero_threshold;
            if (lower > 0 && lower < zero_threshold)
                lower = zero_threshold;
            Float64 val = 0;
            if (custom_buckets)
                val = (upper + lower) / 2;
            else if (lower <= 0 && upper >= 0)
                val = 0;
            else
            {
                val = std::sqrt(upper * lower);
                if (upper < 0)
                    val = -val;
            }
            const Float64 delta = val - mean;
            kahanSumInc(bucket_count * delta * delta, variance, c_variance);
        };

        if (!custom_buckets)
        {
            /// Negative buckets go first, from the most negative one up towards the zero bucket (the
            /// reverse of the span expansion order); custom-bucket histograms have no negative side.
            const auto negative_buckets = expandHistogramSpans(
                tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::NegativeSpans]),
                tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::NegativeValues]),
                row);
            for (auto it = negative_buckets.rbegin(); it != negative_buckets.rend(); ++it)
                process_bucket(-getHistogramBoundExponential(it->index, schema), -getHistogramBoundExponential(it->index - 1, schema), it->count);
        }

        if (zero_count > 0)
            process_bucket(-zero_threshold, zero_threshold, zero_count);

        const auto & positive_spans_column = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::PositiveSpans]);
        const auto & positive_values_column = tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::PositiveValues]);
        const auto positive_buckets = expandHistogramSpans(positive_spans_column, positive_values_column, row);

        if (custom_buckets)
        {
            /// Custom bucket boundaries come from `custom_values` (mirrors `getBound`: -Inf below the first
            /// bound, +Inf above the last one; out-of-range bucket indices are rejected).
            const auto & custom_values_array = typeid_cast<const ColumnArray &>(
                tuple_column.getColumn(element_positions[TimeSeriesHistogramPayloadTupleIndex::CustomValues]));
            const auto & custom_values_offsets = custom_values_array.getOffsets();
            const size_t custom_values_begin = (row == 0) ? 0 : custom_values_offsets[row - 1];
            const size_t num_custom_values = custom_values_offsets[row] - custom_values_begin;
            const auto & custom_values_data = custom_values_array.getData();

            auto custom_bound = [&](Int64 idx) -> Float64
            {
                if (idx < 0)
                    return -std::numeric_limits<Float64>::infinity();
                const auto uidx = static_cast<UInt64>(idx);
                if (uidx >= num_custom_values)
                {
                    if (uidx == num_custom_values)
                        return std::numeric_limits<Float64>::infinity();
                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "Native histogram bucket index {} is out of bounds for {} custom bucket bounds",
                        idx, num_custom_values);
                }
                return custom_values_data.getFloat64(custom_values_begin + uidx);
            };

            for (const auto & bucket : positive_buckets)
                process_bucket(custom_bound(bucket.index - 1), custom_bound(bucket.index), bucket.count);
        }
        else
        {
            for (const auto & bucket : positive_buckets)
                process_bucket(getHistogramBoundExponential(bucket.index - 1, schema), getHistogramBoundExponential(bucket.index, schema), bucket.count);
        }

        variance += c_variance;
        variance /= count;
        return transformResult(variance);
    }

private:
    /// Compensated addition: sum += inc, with the lost low-order bits accumulated in c
    /// (mirrors `kahanSumInc` in Prometheus promql/functions.go).
    static void kahanSumInc(Float64 inc, Float64 & sum, Float64 & c)
    {
        const Float64 t = sum + inc;
        if (std::isinf(t))
            c = 0;
        else if (std::abs(sum) >= std::abs(inc))
            c += (sum - t) + inc;
        else
            c += (inc - t) + sum;
        sum = t;
    }
};

}
