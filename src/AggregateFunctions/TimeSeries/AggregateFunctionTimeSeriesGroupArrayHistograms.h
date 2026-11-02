#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Common/ProfileEvents.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/TimeSeries/TimeSeriesDuplicateTimestampRule.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <Storages/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/sort.h>

#include <numeric>
#include <optional>
#include <vector>


namespace ProfileEvents
{
    extern const Event TimeSeriesGroupArrayHistogramsAppendOnlyMerges;
    extern const Event TimeSeriesGroupArrayHistogramsSortedMerges;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_DATA;
}

/// The state of `timeSeriesGroupArrayHistograms`: the added arrays as references to ranges of their source tuple columns, so a merge
/// of float-only rows allocates nothing and no array is copied before the result is built. Empty arrays are not recorded. With the
/// first and last timestamp of the arrays the state tracks whether every array starts after the previous one ended (`appends_only`):
/// then the result is their concatenation. The state lives on the heap. A deserialized state owns the column its ranges refer to.
template <typename TimestampType>
struct AggregateFunctionTimeSeriesGroupArrayHistogramsData
{
    struct Range
    {
        ColumnPtr source;
        size_t offset = 0;
        size_t size = 0;
    };

    VectorWithMemoryTracking<Range> ranges;
    size_t num_elements = 0;
    TimestampType first_timestamp{};
    TimestampType last_timestamp{};
    bool appends_only = true;

    void addRange(ColumnPtr source, size_t offset, size_t size, TimestampType first, TimestampType last)
    {
        if (ranges.empty())
            first_timestamp = first;
        else if (!(last_timestamp < first))
            appends_only = false;
        last_timestamp = last;
        num_elements += size;
        ranges.push_back(Range{std::move(source), offset, size});
    }

    void merge(const AggregateFunctionTimeSeriesGroupArrayHistogramsData & rhs)
    {
        if (rhs.ranges.empty())
            return;
        if (ranges.empty())
            first_timestamp = rhs.first_timestamp;
        else if (!rhs.appends_only || !(last_timestamp < rhs.first_timestamp))
            appends_only = false;
        last_timestamp = rhs.last_timestamp;
        num_elements += rhs.num_elements;
        ranges.insert(ranges.end(), rhs.ranges.begin(), rhs.ranges.end());
    }
};

/// Aggregate function merging arrays of histogram samples of the shape of the `histograms` column of a TimeSeries table (see
/// TimeSeriesHistogramsColumns), each sorted by timestamp: the result is sorted and deduplicated by timestamp. Of the samples with
/// the same timestamp the greatest one under the order of `TimeSeriesDuplicateTimestampRule` wins (the greatest total count,
/// then the greatest `sum`, then the other elements), so the result does not depend on the order of merging. The elements are stored
/// as they are, so arrays in timestamp order (ingestion and the merges of its parts) are appended as they are, and overlapping
/// arrays are sorted and deduplicated. The result type is the argument type, so the function fits `SimpleAggregateFunction`.
template <typename TimestampType>
class AggregateFunctionTimeSeriesGroupArrayHistograms final
    : public IAggregateFunctionDataHelper<AggregateFunctionTimeSeriesGroupArrayHistogramsData<TimestampType>, AggregateFunctionTimeSeriesGroupArrayHistograms<TimestampType>>
{
public:
    using Data = AggregateFunctionTimeSeriesGroupArrayHistogramsData<TimestampType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionTimeSeriesGroupArrayHistograms<TimestampType>>;
    using TimestampColumnType = ColumnVectorOrDecimal<TimestampType>;

    AggregateFunctionTimeSeriesGroupArrayHistograms(const String & name_, const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , name(name_)
        , tuple_type(typeid_cast<const DataTypeArray &>(*argument_types_[0]).getNestedType())
        , tuple_serialization(tuple_type->getDefaultSerialization())
    {
    }

    String getName() const override { return name; }

    /// The result type is the argument type without the custom name of `SimpleAggregateFunction`.
    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        const auto & array_type = typeid_cast<const DataTypeArray &>(*argument_types_[0]);
        const auto & tuple_type = typeid_cast<const DataTypeTuple &>(*array_type.getNestedType());
        if (tuple_type.hasExplicitNames())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements(), tuple_type.getElementNames()));
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements()));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto * array = checkAndGetColumn<ColumnArray>(columns[0]);
        if (!array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of aggregate function {} must be an array, got {}", name, columns[0]->getName());

        const size_t size = array->getSize(row_num);
        if (!size)
            return;
        const size_t offset = array->getOffsets()[static_cast<ssize_t>(row_num) - 1];
        const auto & timestamps = getTimestamps(array->getData());
        this->data(place).addRange(array->getDataPtr(), offset, size, timestamps[offset], timestamps[offset + size - 1]);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const Data & data = this->data(place);
        const FormatSettings format_settings;

        /// The format version, the end of each array in the concatenation, then the elements.
        writeBinaryLittleEndian(FORMAT_VERSION, buf);
        writeVarUInt(data.ranges.size(), buf);
        size_t end = 0;
        for (const auto & range : data.ranges)
        {
            end += range.size;
            writeVarUInt(end, buf);
        }

        writeVarUInt(data.num_elements, buf);
        for (const auto & range : data.ranges)
            for (size_t i = 0; i != range.size; ++i)
                tuple_serialization->serializeBinary(*range.source, range.offset + i, buf, format_settings);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        UInt16 format_version = 0;
        readBinaryLittleEndian(format_version, buf);
        if (format_version != FORMAT_VERSION)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot deserialize data with different format version, expected {}, got {}",
                FORMAT_VERSION, format_version);

        Data & data = this->data(place);
        data = Data{};

        size_t num_arrays = 0;
        readVarUInt(num_arrays, buf);
        PODArray<UInt64> array_ends(num_arrays);
        for (auto & end : array_ends)
            readVarUInt(end, buf);

        size_t num_elements = 0;
        readVarUInt(num_elements, buf);
        MutableColumnPtr elements = tuple_type->createColumn();
        const FormatSettings format_settings;
        for (size_t i = 0; i != num_elements; ++i)
            tuple_serialization->deserializeBinary(*elements, buf, format_settings);

        const ColumnPtr owned = std::move(elements);
        const auto & timestamps = getTimestamps(*owned);
        size_t begin = 0;
        for (const UInt64 end : array_ends)
        {
            if (end > begin)
                data.addRange(owned, begin, end - begin, timestamps[begin], timestamps[end - 1]);
            begin = end;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & array_to = assert_cast<ColumnArray &>(to);
        auto & offsets_to = array_to.getOffsets();
        auto & tuple_to = assert_cast<ColumnTuple &>(array_to.getData());

        const Data & data = this->data(place);
        if (data.ranges.empty())
        {
            /// The usual case of a merge of float-only rows: every array is empty.
            offsets_to.push_back(tuple_to.size());
            return;
        }

        /// A single non-empty input array is sorted and deduplicated already (the sink writes it so): it is copied once,
        /// from its source column into the result.
        if (data.ranges.size() == 1)
        {
            const auto & range = data.ranges.front();
            tuple_to.insertRangeFrom(*range.source, range.offset, range.size);
            offsets_to.push_back(tuple_to.size());
            return;
        }

        if (data.appends_only)
            appendRanges(data, tuple_to);
        else
            sortRanges(data, tuple_to);
        offsets_to.push_back(tuple_to.size());
    }

private:
    /// The version of the serialized state, checked on deserialization: the state embeds the tuple's binary serialization,
    /// so a server with another element list must refuse it instead of misreading it.
    static constexpr UInt16 FORMAT_VERSION = 1;

    static const typename TimestampColumnType::Container & getTimestamps(const IColumn & tuple)
    {
        return assert_cast<const TimestampColumnType &>(assert_cast<const ColumnTuple &>(tuple).getColumn(TimeSeriesHistogramsColumns::getTimestampPosition())).getData();
    }

    /// Every array starts after the previous one ended: the result is the concatenation, copied straight into the result column.
    static void appendRanges(const Data & data, ColumnTuple & tuple_to)
    {
        ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsAppendOnlyMerges);
        for (const auto & range : data.ranges)
            tuple_to.insertRangeFrom(*range.source, range.offset, range.size);
    }

    /// The arrays overlap in time: they are concatenated into a column of their own, sorted by timestamp, deduplicated
    /// and copied into the result.
    void sortRanges(const Data & data, ColumnTuple & tuple_to) const
    {
        ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsSortedMerges);
        const size_t num_elements = data.num_elements;
        MutableColumnPtr elements = tuple_type->createColumn();
        elements->reserve(num_elements);
        for (const auto & range : data.ranges)
            elements->insertRangeFrom(*range.source, range.offset, range.size);
        const auto & source = assert_cast<const ColumnTuple &>(*elements);

        const auto & timestamps = assert_cast<const TimestampColumnType &>(source.getColumn(TimeSeriesHistogramsColumns::getTimestampPosition())).getData();
        IColumn::Permutation permutation(num_elements);
        std::iota(permutation.begin(), permutation.end(), 0);
        ::sort(permutation.begin(), permutation.end(), [&](size_t lhs, size_t rhs) { return timestamps[lhs] < timestamps[rhs]; });

        /// Collapses each run of equal timestamps into the winning element, in place at the beginning of the permutation. The rule
        /// orders the whole payload, so the result is the same for any order of the elements with the same timestamp (the sort is not stable).
        const TimeSeriesHistogramsColumnsView view{source};
        size_t num_unique = 1;
        for (size_t i = 1; i < num_elements; ++i)
        {
            if (timestamps[permutation[i]] == timestamps[permutation[num_unique - 1]])
            {
                if (TimeSeriesDuplicateTimestampRule::histogramSampleWins(view, permutation[num_unique - 1], permutation[i]))
                    permutation[num_unique - 1] = permutation[i];
            }
            else
            {
                permutation[num_unique++] = permutation[i];
            }
        }

        const ColumnPtr merged = source.permute(permutation, num_unique);
        tuple_to.insertRangeFrom(*merged, 0, num_unique);
    }

    const String name;
    const DataTypePtr tuple_type;
    const SerializationPtr tuple_serialization;
};

}
