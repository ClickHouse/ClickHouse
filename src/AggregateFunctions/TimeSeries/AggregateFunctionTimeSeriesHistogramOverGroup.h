#pragma once

/// Shared machinery for `timeSeriesHistogramSumOverGroup`/`timeSeriesHistogramAvgOverGroup`: the PromQL
/// `sum`/`avg` histogram branches of `aggregation` in Prometheus promql/engine.go (see tmp/prom_engine.go).

#include <cmath>
#include <cstddef>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/TimeSeries/TimeSeriesHistogramKernel.h>
#include <Functions/TimeSeries/TimeSeriesHistogramMathFunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// The port of `FloatHistogram.HasOverflow`: true if any field is +-Inf.
inline bool timeSeriesHistogramHasOverflow(const TimeSeriesFloatHistogram & histogram)
{
    if (std::isinf(histogram.zero_count) || std::isinf(histogram.count) || std::isinf(histogram.sum))
        return true;
    for (const Float64 value : histogram.positive_buckets)
        if (std::isinf(value))
            return true;
    for (const Float64 value : histogram.negative_buckets)
        if (std::isinf(value))
            return true;
    for (const Float64 value : histogram.custom_values)
        if (std::isinf(value))
            return true;
    return false;
}

/// The shared part of the over-group aggregation states: the accumulated histogram and its Kahan compensation.
struct TimeSeriesHistogramOverGroupState
{
    bool has_value = false;
    /// Set when two samples of the group had incompatible schemas: the group element is dropped (NULL).
    bool incompatible = false;
    TimeSeriesFloatHistogram value;
    std::optional<TimeSeriesFloatHistogram> kahan_c;
};

/// The `avg` state: like upstream's `groupedAggregation`, the direct Kahan sum is used until it
/// would overflow; then the aggregation switches to the incremental mean.
struct TimeSeriesHistogramAvgOverGroupState
{
    bool has_value = false;
    bool incompatible = false;
    bool incremental_mean = false;
    /// The number of histogram samples aggregated (upstream's float64 `groupCount`).
    Float64 count = 0;
    TimeSeriesFloatHistogram value;
    std::optional<TimeSeriesFloatHistogram> kahan_c;
    TimeSeriesFloatHistogram mean;  /// Valid iff incremental_mean.
};

/// Serialization of one kernel histogram: the scalars, then per array field the count and the raw elements.
inline void writeTimeSeriesFloatHistogram(WriteBuffer & buf, const TimeSeriesFloatHistogram & histogram)
{
    writeBinaryLittleEndian(histogram.counter_reset_hint, buf);
    writeBinaryLittleEndian(histogram.schema, buf);
    writeBinaryLittleEndian(histogram.zero_threshold, buf);
    writeBinaryLittleEndian(histogram.count, buf);
    writeBinaryLittleEndian(histogram.sum, buf);
    writeBinaryLittleEndian(histogram.zero_count, buf);

    auto write_spans = [&buf](const std::vector<TimeSeriesHistogramSpan> & spans) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        writeBinaryLittleEndian(static_cast<UInt64>(spans.size()), buf);
        for (const auto & span : spans)
        {
            writeBinaryLittleEndian(span.offset, buf);
            writeBinaryLittleEndian(span.length, buf);
        }
    };
    auto write_floats = [&buf](const std::vector<Float64> & values) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        writeBinaryLittleEndian(static_cast<UInt64>(values.size()), buf);
        for (const Float64 value : values)
            writeBinaryLittleEndian(value, buf);
    };

    write_spans(histogram.positive_spans);
    write_floats(histogram.positive_buckets);
    write_spans(histogram.negative_spans);
    write_floats(histogram.negative_buckets);
    write_floats(histogram.custom_values);
}

/// Deserialization of one kernel histogram (fail-close on corrupt data).
inline TimeSeriesFloatHistogram readTimeSeriesFloatHistogram(ReadBuffer & buf)
{
    TimeSeriesFloatHistogram histogram;
    readBinaryLittleEndian(histogram.counter_reset_hint, buf);
    readBinaryLittleEndian(histogram.schema, buf);
    readBinaryLittleEndian(histogram.zero_threshold, buf);
    readBinaryLittleEndian(histogram.count, buf);
    readBinaryLittleEndian(histogram.sum, buf);
    readBinaryLittleEndian(histogram.zero_count, buf);

    auto read_spans = [&buf](std::vector<TimeSeriesHistogramSpan> & spans) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        UInt64 size = 0;
        readBinaryLittleEndian(size, buf);
        spans.reserve(size);
        for (UInt64 i = 0; i < size; ++i)
        {
            TimeSeriesHistogramSpan span{};
            readBinaryLittleEndian(span.offset, buf);
            readBinaryLittleEndian(span.length, buf);
            spans.push_back(span);
        }
    };
    auto read_floats = [&buf](std::vector<Float64> & values) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        UInt64 size = 0;
        readBinaryLittleEndian(size, buf);
        values.reserve(size);
        for (UInt64 i = 0; i < size; ++i)
        {
            Float64 value = 0;
            readBinaryLittleEndian(value, buf);
            values.push_back(value);
        }
    };

    read_spans(histogram.positive_spans);
    read_floats(histogram.positive_buckets);
    read_spans(histogram.negative_spans);
    read_floats(histogram.negative_buckets);
    read_floats(histogram.custom_values);

    histogram.validateDecodedLayout();
    return histogram;
}

/// Serialization of the over-group states (with a format version prefix, like the rate-family states).
inline void writeTimeSeriesHistogramOverGroupState(WriteBuffer & buf, const TimeSeriesHistogramOverGroupState & state, UInt16 format_version)
{
    writeBinaryLittleEndian(format_version, buf);
    writeBinary(state.has_value, buf);
    writeBinary(state.incompatible, buf);
    if (!state.has_value)
        return;
    writeTimeSeriesFloatHistogram(buf, state.value);
    writeBinary(state.kahan_c.has_value(), buf);
    if (state.kahan_c)
        writeTimeSeriesFloatHistogram(buf, *state.kahan_c);
}

inline void writeTimeSeriesHistogramOverGroupState(WriteBuffer & buf, const TimeSeriesHistogramAvgOverGroupState & state, UInt16 format_version)
{
    writeBinaryLittleEndian(format_version, buf);
    writeBinary(state.has_value, buf);
    writeBinary(state.incompatible, buf);
    if (!state.has_value)
        return;
    writeBinary(state.incremental_mean, buf);
    writeBinaryLittleEndian(state.count, buf);
    writeTimeSeriesFloatHistogram(buf, state.value);
    writeBinary(state.kahan_c.has_value(), buf);
    if (state.kahan_c)
        writeTimeSeriesFloatHistogram(buf, *state.kahan_c);
    if (state.incremental_mean)
        writeTimeSeriesFloatHistogram(buf, state.mean);
}

inline void readTimeSeriesHistogramOverGroupState(ReadBuffer & buf, TimeSeriesHistogramOverGroupState & state, UInt16 format_version)
{
    UInt16 version = 0;
    readBinaryLittleEndian(version, buf);
    if (version != format_version)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

    readBinary(state.has_value, buf);
    readBinary(state.incompatible, buf);
    if (!state.has_value)
        return;
    state.value = readTimeSeriesFloatHistogram(buf);
    bool has_c = false;
    readBinary(has_c, buf);
    if (has_c)
        state.kahan_c = readTimeSeriesFloatHistogram(buf);
}

inline void readTimeSeriesHistogramOverGroupState(ReadBuffer & buf, TimeSeriesHistogramAvgOverGroupState & state, UInt16 format_version)
{
    UInt16 version = 0;
    readBinaryLittleEndian(version, buf);
    if (version != format_version)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize data with different format version");

    readBinary(state.has_value, buf);
    readBinary(state.incompatible, buf);
    if (!state.has_value)
        return;
    readBinary(state.incremental_mean, buf);
    readBinaryLittleEndian(state.count, buf);
    state.value = readTimeSeriesFloatHistogram(buf);
    bool has_c = false;
    readBinary(has_c, buf);
    if (has_c)
        state.kahan_c = readTimeSeriesFloatHistogram(buf);
    if (state.incremental_mean)
        state.mean = readTimeSeriesFloatHistogram(buf);
}

/// Merging two partial `sum` states (no upstream counterpart): the rhs histogram and its compensation
/// are folded into the lhs Kahan accumulation.
inline void mergeTimeSeriesHistogramOverGroupState(TimeSeriesHistogramOverGroupState & state, const TimeSeriesHistogramOverGroupState & rhs)
{
    state.incompatible = state.incompatible || rhs.incompatible;
    if (state.incompatible || !rhs.has_value)
        return;
    if (!state.has_value)
    {
        state.value = rhs.value;
        state.kahan_c = rhs.kahan_c;
        state.has_value = true;
        return;
    }
    if (state.value.usesCustomBuckets() != rhs.value.usesCustomBuckets())
    {
        state.incompatible = true;
        return;
    }
    state.kahan_c = state.value.kahanAdd(rhs.value, std::move(state.kahan_c)).updated_compensation;
    if (rhs.kahan_c)
        state.kahan_c = state.value.kahanAdd(*rhs.kahan_c, std::move(state.kahan_c)).updated_compensation;
}

/// Merging two partial `avg` states (no upstream counterpart): running-sum states merge like the
/// `sum` states; an incremental-mean state combines via the parallel-mean formula.
inline void mergeTimeSeriesHistogramOverGroupState(TimeSeriesHistogramAvgOverGroupState & state, const TimeSeriesHistogramAvgOverGroupState & rhs)
{
    state.incompatible = state.incompatible || rhs.incompatible;
    if (state.incompatible || !rhs.has_value)
        return;
    if (!state.has_value)
    {
        state.incremental_mean = rhs.incremental_mean;
        state.count = rhs.count;
        state.value = rhs.value;
        state.kahan_c = rhs.kahan_c;
        state.mean = rhs.mean;
        state.has_value = true;
        return;
    }

    const Float64 total_count = state.count + rhs.count;

    if (!state.incremental_mean && !rhs.incremental_mean)
    {
        if (state.value.usesCustomBuckets() != rhs.value.usesCustomBuckets())
        {
            state.incompatible = true;
            return;
        }
        state.kahan_c = state.value.kahanAdd(rhs.value, std::move(state.kahan_c)).updated_compensation;
        if (rhs.kahan_c)
            state.kahan_c = state.value.kahanAdd(*rhs.kahan_c, std::move(state.kahan_c)).updated_compensation;
        state.count = total_count;
        return;
    }

    TimeSeriesFloatHistogram mean_lhs = state.incremental_mean ? state.mean : state.value;
    if (!state.incremental_mean)
        mean_lhs.div(state.count);
    TimeSeriesFloatHistogram mean_rhs = rhs.incremental_mean ? rhs.mean : rhs.value;
    if (!rhs.incremental_mean)
        mean_rhs.div(rhs.count);

    if (mean_lhs.usesCustomBuckets() != mean_rhs.usesCustomBuckets())
    {
        state.incompatible = true;
        return;
    }
    mean_lhs.mul(state.count / total_count);
    mean_rhs.mul(rhs.count / total_count);
    mean_lhs.add(mean_rhs);

    state.incremental_mean = true;
    state.mean = mean_lhs;
    state.value = std::move(mean_lhs);
    state.kahan_c = std::nullopt;
    state.count = total_count;
}

/// Base class for `timeSeriesHistogramSumOverGroup`/`timeSeriesHistogramAvgOverGroup`: aggregates one
/// group's payload tuples (validated POSITIONALLY) into a Nullable payload tuple.
template <typename Derived, typename State>
class AggregateFunctionTimeSeriesHistogramOverGroupBase :
    public IAggregateFunctionDataHelper<State, Derived>
{
public:
    AggregateFunctionTimeSeriesHistogramOverGroupBase(const DataTypes & argument_types_, const Array & parameters_, std::string_view name_)
        : IAggregateFunctionDataHelper<State, Derived>(argument_types_, parameters_, createResultType())
    {
        if (argument_types_.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Aggregate function {} requires one argument (the histogram payload tuple)", name_);

        /// The histogram argument is decoded positionally: accept any tuple with the exact payload element types in order.
        if (!isTimeSeriesHistogramPayloadTupleType(argument_types_[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument for aggregate function {}, expected Tuple with the payload of {}",
                            argument_types_[0]->getName(), name_, getTimeSeriesHistogramPayloadTupleType()->getName());

        for (size_t i = 0; i < payload_positions.size(); ++i)
            payload_positions[i] = i;
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * /*arena*/) const override
    {
        State & state = this->data(place);
        /// Mirrors upstream's `if group.incompatibleHistograms { continue }`.
        if (state.incompatible)
            return;
        const auto & tuple_column = typeid_cast<const ColumnTuple &>(*columns[0]);
        derived().doAdd(state, TimeSeriesFloatHistogram::fromPayloadTupleRow(tuple_column, payload_positions, row_num));
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        mergeTimeSeriesHistogramOverGroupState(this->data(place), this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeTimeSeriesHistogramOverGroupState(buf, this->data(place), Derived::FORMAT_VERSION);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readTimeSeriesHistogramOverGroupState(buf, this->data(place), Derived::FORMAT_VERSION);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & to_nullable = typeid_cast<ColumnNullable &>(to);
        auto result = derived().computeResult(this->data(place));
        if (!result)
        {
            to_nullable.insertDefault();
            return;
        }
        appendTimeSeriesHistogramPayloadRow(*result, typeid_cast<ColumnTuple &>(to_nullable.getNestedColumn()));
        to_nullable.getNullMapData().push_back(false);
    }

private:
    static DataTypePtr createResultType()
    {
        return makeNullable(getTimeSeriesHistogramPayloadTupleType());
    }

    const Derived & derived() const { return *static_cast<const Derived *>(this); }

    /// The payload tuple is validated positionally, so the element positions are the identity.
    TimeSeriesHistogramPayloadPositions payload_positions{};
};

}
