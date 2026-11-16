#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

DataTypePtr getTimeSeriesHistogramSpansType()
{
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeUInt32>()},
        Names{"offset", "length"}));
}

NamesAndTypes getTimeSeriesHistogramPayloadColumns()
{
    auto float64 = std::make_shared<DataTypeFloat64>();
    auto float64_array = std::make_shared<DataTypeArray>(float64);
    auto uint64 = std::make_shared<DataTypeUInt64>();
    auto uint64_array = std::make_shared<DataTypeArray>(uint64);
    auto spans = getTimeSeriesHistogramSpansType();

    return NamesAndTypes{
        {TimeSeriesColumnNames::Flags, std::make_shared<DataTypeUInt8>()},
        {TimeSeriesColumnNames::Schema, std::make_shared<DataTypeInt8>()},
        {TimeSeriesColumnNames::ZeroThreshold, float64},
        {TimeSeriesColumnNames::Count, float64},
        {TimeSeriesColumnNames::Sum, float64},
        {TimeSeriesColumnNames::ZeroCount, float64},
        {TimeSeriesColumnNames::PositiveSpans, spans},
        {TimeSeriesColumnNames::PositiveValues, float64_array},
        {TimeSeriesColumnNames::NegativeSpans, spans},
        {TimeSeriesColumnNames::NegativeValues, float64_array},
        {TimeSeriesColumnNames::CustomValues, float64_array},
        {TimeSeriesColumnNames::CountInt, uint64},
        {TimeSeriesColumnNames::ZeroCountInt, uint64},
        {TimeSeriesColumnNames::PositiveValuesInt, uint64_array},
        {TimeSeriesColumnNames::NegativeValuesInt, uint64_array},
    };
}

DataTypePtr getTimeSeriesHistogramPayloadTupleType()
{
    DataTypes element_types;
    Strings element_names;
    for (const auto & [name, type] : getTimeSeriesHistogramPayloadColumns())
    {
        element_types.push_back(type);
        element_names.push_back(name);
    }
    return std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names));
}

DataTypePtr getTimeSeriesHistogramTupleType(const DataTypePtr & timestamp_type)
{
    const auto payload_tuple = std::static_pointer_cast<const DataTypeTuple>(getTimeSeriesHistogramPayloadTupleType());
    DataTypes element_types;
    Strings element_names;
    element_types.push_back(timestamp_type);
    element_names.push_back(TimeSeriesColumnNames::Timestamp);
    for (size_t i = 0; i < payload_tuple->getElements().size(); ++i)
    {
        element_types.push_back(payload_tuple->getElements()[i]);
        element_names.push_back(payload_tuple->getElementNames()[i]);
    }
    return std::make_shared<DataTypeTuple>(std::move(element_types), std::move(element_names));
}

DataTypePtr getTimeSeriesHistogramsOuterColumnType(const DataTypePtr & timestamp_type)
{
    return std::make_shared<DataTypeArray>(getTimeSeriesHistogramTupleType(timestamp_type));
}

bool isTimeSeriesHistogramTupleType(const DataTypePtr & type)
{
    const auto * tuple = typeid_cast<const DataTypeTuple *>(removeNullable(type).get());
    if (!tuple || !tuple->hasExplicitNames())
        return false;

    const auto payload_tuple = std::static_pointer_cast<const DataTypeTuple>(getTimeSeriesHistogramPayloadTupleType());
    for (size_t i = 0; i < TimeSeriesHistogramPayloadTupleIndex::Size; ++i)
    {
        const auto pos = tuple->tryGetPositionByName(payload_tuple->getElementNames()[i]);
        if (!pos || !tuple->getElement(*pos)->equals(*payload_tuple->getElement(i)))
            return false;
    }
    return true;
}

bool isTimeSeriesHistogramPayloadTupleType(const DataTypePtr & type)
{
    const auto * tuple = typeid_cast<const DataTypeTuple *>(removeNullable(type).get());
    if (!tuple)
        return false;

    const auto payload_tuple = std::static_pointer_cast<const DataTypeTuple>(getTimeSeriesHistogramPayloadTupleType());
    if (tuple->getElements().size() != TimeSeriesHistogramPayloadTupleIndex::Size)
        return false;
    for (size_t i = 0; i < TimeSeriesHistogramPayloadTupleIndex::Size; ++i)
    {
        if (!tuple->getElement(i)->equals(*payload_tuple->getElement(i)))
            return false;
    }
    return true;
}


std::vector<HistogramBucket> expandHistogramSpans(const IColumn & spans_column, const IColumn & values_column, size_t row)
{
    const auto & spans_array = typeid_cast<const ColumnArray &>(spans_column);
    const auto & spans_array_offsets = spans_array.getOffsets();
    const auto & span_tuple = typeid_cast<const ColumnTuple &>(spans_array.getData());
    const auto & span_offset_column = span_tuple.getColumn(0);
    const auto & span_length_column = span_tuple.getColumn(1);

    const auto & values_array = typeid_cast<const ColumnArray &>(values_column);
    const auto & values_array_offsets = values_array.getOffsets();
    const auto & values_data = values_array.getData();

    const size_t spans_begin = (row == 0) ? 0 : spans_array_offsets[row - 1];
    const size_t spans_end = spans_array_offsets[row];
    const size_t values_begin = (row == 0) ? 0 : values_array_offsets[row - 1];
    const size_t values_end = values_array_offsets[row];

    std::vector<HistogramBucket> buckets;
    buckets.reserve(values_end - values_begin);

    Int64 idx = 0;
    size_t value_pos = values_begin;
    for (size_t s = spans_begin; s < spans_end; ++s)
    {
        idx += span_offset_column.getInt(s);
        if (s != spans_begin)
            ++idx;
        const UInt64 length = span_length_column.getUInt(s);
        for (UInt64 k = 0; k < length; ++k)
        {
            if (value_pos >= values_end)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has fewer bucket values than its spans cover");
            buckets.push_back(HistogramBucket{idx, values_data.getFloat64(value_pos)});
            ++value_pos;
            ++idx;
        }
        --idx;
    }
    if (value_pos != values_end)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has more bucket values than its spans cover");

    return buckets;
}


namespace
{
    /// Returns the range of the elements of the row `row` of an array column.
    std::pair<size_t, size_t> getArrayRowRange(const IColumn & column, size_t row)
    {
        const auto & offsets = typeid_cast<const ColumnArray &>(column).getOffsets();
        return {(row == 0) ? 0 : offsets[row - 1], offsets[row]};
    }

    /// Checks one direction (positive or negative) of a histogram sample: its spans must cover exactly its bucket values,
    /// which must be non-negative and NaN only in a stale marker. Returns the range of the bucket indexes the spans reach
    /// (following the span walk of `floatBucketIterator` in Prometheus), or nullopt if they reach no bucket.
    std::optional<std::pair<Int64, Int64>> checkHistogramBuckets(
        const IColumn & spans_column, const IColumn & values_column, size_t row, bool is_stale_marker, std::string_view what)
    {
        const auto & span_tuple = typeid_cast<const ColumnTuple &>(typeid_cast<const ColumnArray &>(spans_column).getData());
        const auto & span_offsets = span_tuple.getColumn(0);
        const auto & span_lengths = span_tuple.getColumn(1);
        const auto [spans_begin, spans_end] = getArrayRowRange(spans_column, row);
        const auto [values_begin, values_end] = getArrayRowRange(values_column, row);
        const size_t num_values = values_end - values_begin;

        std::optional<std::pair<Int64, Int64>> index_range;
        UInt64 total_length = 0;
        Int64 last_index = 0;
        for (size_t i = spans_begin; i != spans_end; ++i)
        {
            /// The first span's offset is the index of its first bucket, a later span's offset is the gap after the previous span.
            const Int64 first_index = last_index + span_offsets.getInt(i) + ((i == spans_begin) ? 0 : 1);
            const UInt64 length = span_lengths.getUInt(i);
            total_length += length;
            last_index = first_index + static_cast<Int64>(length) - 1;
            if (length != 0)
                index_range = index_range ? std::pair{std::min(index_range->first, first_index), std::max(index_range->second, last_index)}
                                          : std::pair{first_index, last_index};
        }
        if (total_length != num_values)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram has {} {} bucket values but its spans cover {} buckets", num_values, what, total_length);

        const auto & values = typeid_cast<const ColumnArray &>(values_column).getData();
        for (size_t i = values_begin; i != values_end; ++i)
        {
            const Float64 value = values.getFloat64(i);
            if (value < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has a negative {} bucket count: {}", what, value);
            if (std::isnan(value) && !is_stale_marker)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has a NaN {} bucket count but is not a stale marker", what);
        }

        return index_range;
    }

    /// Checks that the exact integer carriers of an integer histogram match its Float64 counts, which are their rounded copies.
    bool exactCountsMatch(const IColumn & values_column, const IColumn & int_values_column, size_t row)
    {
        const auto [values_begin, values_end] = getArrayRowRange(values_column, row);
        const auto [int_values_begin, int_values_end] = getArrayRowRange(int_values_column, row);
        if (values_end - values_begin != int_values_end - int_values_begin)
            return false;

        const auto & values = typeid_cast<const ColumnArray &>(values_column).getData();
        const auto & int_values = typeid_cast<const ColumnArray &>(int_values_column).getData();
        for (size_t i = 0; i != values_end - values_begin; ++i)
            if (values.getFloat64(values_begin + i) != static_cast<Float64>(int_values.getUInt(int_values_begin + i)))
                return false;
        return true;
    }
}

void validateTimeSeriesHistogramSample(const ColumnTuple & tuple, size_t row)
{
    namespace Idx = TimeSeriesHistogramsTupleIndex;

    const UInt8 flags = static_cast<UInt8>(tuple.getColumn(Idx::Flags).getUInt(row));
    constexpr UInt8 known_flags
        = TimeSeriesHistogramFlags::IsFloat | TimeSeriesHistogramFlags::CounterResetHintMask | TimeSeriesHistogramFlags::StaleMarker;
    if (flags & ~known_flags)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has unknown flag bits set: {}", static_cast<UInt32>(flags));
    const bool is_float = flags & TimeSeriesHistogramFlags::IsFloat;
    const bool is_stale_marker = flags & TimeSeriesHistogramFlags::StaleMarker;

    const Int64 schema = tuple.getColumn(Idx::Schema).getInt(row);
    const bool custom_buckets = (schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
    if (!custom_buckets && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an out-of-range bucket schema: {}", schema);

    /// NaN counts are allowed only in a stale marker (whose sum carries the stale NaN).
    const Float64 count = tuple.getColumn(Idx::Count).getFloat64(row);
    const Float64 zero_count = tuple.getColumn(Idx::ZeroCount).getFloat64(row);
    const Float64 zero_threshold = tuple.getColumn(Idx::ZeroThreshold).getFloat64(row);
    if (count < 0 || zero_count < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Native histogram has a negative {}: {}", (count < 0) ? "count" : "zero count", (count < 0) ? count : zero_count);
    if (!is_stale_marker && (std::isnan(count) || std::isnan(zero_count)))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Native histogram has a NaN {} but is not a stale marker", std::isnan(count) ? "count" : "zero count");
    if (zero_threshold < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has a negative zero threshold: {}", zero_threshold);

    const auto positive_index_range = checkHistogramBuckets(
        tuple.getColumn(Idx::PositiveSpans), tuple.getColumn(Idx::PositiveValues), row, is_stale_marker, "positive");
    checkHistogramBuckets(
        tuple.getColumn(Idx::NegativeSpans), tuple.getColumn(Idx::NegativeValues), row, is_stale_marker, "negative");

    const auto [custom_values_begin, custom_values_end] = getArrayRowRange(tuple.getColumn(Idx::CustomValues), row);
    const size_t num_custom_values = custom_values_end - custom_values_begin;
    if (!custom_buckets)
    {
        if (num_custom_values != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram has an exponential bucket schema ({}) but carries {} custom bucket bounds", schema, num_custom_values);
    }
    else
    {
        /// The custom-bucket rules of `Histogram.Validate` in Prometheus model/histogram/histogram.go.
        const auto [negative_spans_begin, negative_spans_end] = getArrayRowRange(tuple.getColumn(Idx::NegativeSpans), row);
        const auto [negative_values_begin, negative_values_end] = getArrayRowRange(tuple.getColumn(Idx::NegativeValues), row);
        if ((negative_spans_begin != negative_spans_end) || (negative_values_begin != negative_values_end))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram with custom buckets must not have negative buckets");

        if (zero_count != 0 || zero_threshold != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram with custom buckets must not use the zero bucket, but has zero count {} and zero threshold {}",
                zero_count, zero_threshold);

        const auto & custom_values = typeid_cast<const ColumnArray &>(tuple.getColumn(Idx::CustomValues)).getData();
        Float64 previous_bound = -std::numeric_limits<Float64>::infinity();
        for (size_t i = custom_values_begin; i != custom_values_end; ++i)
        {
            const Float64 bound = custom_values.getFloat64(i);
            if (!std::isfinite(bound) || !(bound > previous_bound))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Native histogram has custom bucket bounds which are not finite and strictly increasing: {} follows {}",
                    bound, previous_bound);
            previous_bound = bound;
        }

        /// `custom_values` holds the upper bounds of the custom buckets, so a bucket index may reach one past
        /// the last bound (that bucket's upper bound is +Inf). Anything beyond has no bound to render when read back.
        if (positive_index_range
            && (positive_index_range->first < 0 || positive_index_range->second > static_cast<Int64>(num_custom_values)))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram with custom buckets reaches bucket indexes {}..{}, which are not covered by its {} custom bucket bounds",
                positive_index_range->first, positive_index_range->second, num_custom_values);
    }

    /// A tuple of an older, shorter layout has no exact integer carriers (see `TimeSeriesSink::consumeTagsAndSamples`).
    if (tuple.tupleSize() < Idx::Size)
        return;

    const UInt64 count_int = tuple.getColumn(Idx::CountInt).getUInt(row);
    const UInt64 zero_count_int = tuple.getColumn(Idx::ZeroCountInt).getUInt(row);
    if (is_float)
    {
        const auto [positive_int_begin, positive_int_end] = getArrayRowRange(tuple.getColumn(Idx::PositiveValuesInt), row);
        const auto [negative_int_begin, negative_int_end] = getArrayRowRange(tuple.getColumn(Idx::NegativeValuesInt), row);
        if (count_int != 0 || zero_count_int != 0 || positive_int_begin != positive_int_end || negative_int_begin != negative_int_end)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram is a float histogram but has exact integer counts, which only an integer histogram can have");
    }
    else if (count != static_cast<Float64>(count_int) || zero_count != static_cast<Float64>(zero_count_int)
        || !exactCountsMatch(tuple.getColumn(Idx::PositiveValues), tuple.getColumn(Idx::PositiveValuesInt), row)
        || !exactCountsMatch(tuple.getColumn(Idx::NegativeValues), tuple.getColumn(Idx::NegativeValuesInt), row))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Native histogram is an integer histogram but its exact integer counts do not match its counts");
    }
}

}
