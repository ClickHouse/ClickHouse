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


void validateTimeSeriesHistogramSample(const ColumnTuple & tuple, size_t row)
{
    const UInt8 flags = static_cast<UInt8>(tuple.getColumn(TimeSeriesHistogramsTupleIndex::Flags).getUInt(row));
    constexpr UInt8 known_flags
        = TimeSeriesHistogramFlags::IsFloat | TimeSeriesHistogramFlags::CounterResetHintMask | TimeSeriesHistogramFlags::StaleMarker;
    if (flags & ~known_flags)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has unknown flag bits set: {}", static_cast<UInt32>(flags));

    const Int64 schema = tuple.getColumn(TimeSeriesHistogramsTupleIndex::Schema).getInt(row);
    const bool custom_buckets = (schema == HISTOGRAM_CUSTOM_BUCKETS_SCHEMA);
    if (!custom_buckets && (schema < HISTOGRAM_EXPONENTIAL_SCHEMA_MIN || schema > HISTOGRAM_EXPONENTIAL_SCHEMA_MAX))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has an invalid bucket schema: {}", schema);

    /// NaN is a valid count only as part of a stale marker, and it compares false here either way.
    const Float64 count = tuple.getColumn(TimeSeriesHistogramsTupleIndex::Count).getFloat64(row);
    const Float64 zero_count = tuple.getColumn(TimeSeriesHistogramsTupleIndex::ZeroCount).getFloat64(row);
    const Float64 zero_threshold = tuple.getColumn(TimeSeriesHistogramsTupleIndex::ZeroThreshold).getFloat64(row);
    if (count < 0 || zero_count < 0 || zero_threshold < 0)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Native histogram has a negative count ({}), zero count ({}) or zero threshold ({})",
            count, zero_count, zero_threshold);

    const auto positive_buckets = expandHistogramSpans(
        tuple.getColumn(TimeSeriesHistogramsTupleIndex::PositiveSpans),
        tuple.getColumn(TimeSeriesHistogramsTupleIndex::PositiveValues), row);
    const auto negative_buckets = expandHistogramSpans(
        tuple.getColumn(TimeSeriesHistogramsTupleIndex::NegativeSpans),
        tuple.getColumn(TimeSeriesHistogramsTupleIndex::NegativeValues), row);
    for (const auto * buckets : {&positive_buckets, &negative_buckets})
        for (const auto & bucket : *buckets)
            if (bucket.count < 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram has a negative bucket count: {}", bucket.count);

    const auto & custom_values_offsets = typeid_cast<const ColumnArray &>(
        tuple.getColumn(TimeSeriesHistogramsTupleIndex::CustomValues)).getOffsets();
    const size_t num_custom_values = custom_values_offsets[row] - ((row == 0) ? 0 : custom_values_offsets[row - 1]);
    if (!custom_buckets)
    {
        if (num_custom_values != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram has an exponential bucket schema ({}) but carries {} custom bucket bounds",
                schema, num_custom_values);
        return;
    }

    if (!negative_buckets.empty())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Native histogram with custom buckets must not have negative buckets");

    /// `custom_values` holds upper bounds, so an index may reach one past the last one: that
    /// bucket's upper bound is +Inf. Anything beyond has no bound to render when read back.
    for (const auto & bucket : positive_buckets)
        if (bucket.index < 0 || bucket.index > static_cast<Int64>(num_custom_values))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Native histogram with custom buckets reaches bucket index {}, "
                "which is not covered by its {} custom bucket bounds",
                bucket.index, num_custom_values);
}

}
