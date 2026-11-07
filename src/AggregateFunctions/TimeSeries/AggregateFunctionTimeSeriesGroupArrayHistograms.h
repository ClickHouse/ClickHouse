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
#include <DataTypes/TimeSeries/TimeSeriesDuplicateTimestampRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramCounterResetRule.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumns.h>
#include <DataTypes/TimeSeries/TimeSeriesHistogramsColumnsView.h>
#include <base/sort.h>

#include <algorithm>
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
/// first and last timestamp of the arrays the state tracks whether every array starts after the previous one ended and its own
/// timestamps strictly increase (`appends_only`): then the result is their concatenation. The state lives on the heap.
/// A deserialized state owns the column its ranges refer to.
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

    void addRange(ColumnPtr source, size_t offset, size_t size, const typename ColumnVectorOrDecimal<TimestampType>::Container & timestamps)
    {
        const auto first = timestamps[offset];
        if (ranges.empty())
            first_timestamp = first;
        else if (!(last_timestamp < first))
            appends_only = false;

        /// Direct aggregate inputs can contain duplicate timestamps within one array, unlike the arrays written by the sink.
        if (appends_only)
        {
            const auto begin = timestamps.begin() + offset;
            const auto end = begin + size;
            appends_only = std::adjacent_find(begin, end, [](auto lhs, auto rhs)
            {
                return lhs >= rhs;
            }) == end;
        }

        last_timestamp = timestamps[offset + size - 1];
        num_elements += size;
        ranges.push_back(Range{std::move(source), offset, size});
    }

    void merge(const AggregateFunctionTimeSeriesGroupArrayHistogramsData & rhs)
    {
        if (rhs.ranges.empty())
            return;
        if (ranges.empty())
        {
            *this = rhs;
            return;
        }

        appends_only &= rhs.appends_only && (last_timestamp < rhs.first_timestamp);
        last_timestamp = rhs.last_timestamp;
        num_elements += rhs.num_elements;
        ranges.insert(ranges.end(), rhs.ranges.begin(), rhs.ranges.end());
    }
};

/// Aggregate function merging arrays of histogram samples of the shape of the `histograms` column of a TimeSeries table (see
/// TimeSeriesHistogramsColumns), each sorted by timestamp: the result is sorted and deduplicated by timestamp. Of the samples with
/// the same timestamp the greatest one under the order of `TimeSeriesDuplicateTimestampRule` wins (the greatest total count,
/// then the greatest `sum`, then the other elements). Arrays in timestamp order are appended, and overlapping arrays are sorted
/// and deduplicated. Where the predecessor of a kept sample changes (the first sample of every array but the first, a sample whose
/// neighbour changed by sorting or deduplication), its counter reset hint is computed again against the new predecessor by
/// TimeSeriesHistogramCounterResetRule, as on insert; explicit resets and gauge markers are kept. The result type is the argument
/// type, so the function fits `SimpleAggregateFunction`.
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
        this->data(place).addRange(array->getDataPtr(), offset, size, timestamps);
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
        /// Reserve smaller states once, bounding the initial allocation from the untrusted count.
        /// Larger states grow as their offsets are read, like the samples in `timeSeriesGroupArray`.
        PODArray<UInt64> array_ends;
        array_ends.reserve(std::min(num_arrays, MAX_ARRAYS_TO_RESERVE));
        UInt64 previous_end = 0;
        for (size_t i = 0; i != num_arrays; ++i)
        {
            UInt64 end = 0;
            readVarUInt(end, buf);
            if (end <= previous_end)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Invalid array end {} after {} in state of aggregate function {}: array ends must strictly increase",
                    end, previous_end, name);
            array_ends.push_back(end);
            previous_end = end;
        }

        size_t num_elements = 0;
        readVarUInt(num_elements, buf);
        if (previous_end != num_elements)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Last array end {} does not match the number of elements {} in state of aggregate function {}",
                previous_end, num_elements, name);

        MutableColumnPtr elements = tuple_type->createColumn();
        const FormatSettings format_settings;
        for (size_t i = 0; i != num_elements; ++i)
            tuple_serialization->deserializeBinary(*elements, buf, format_settings);

        const ColumnPtr owned = std::move(elements);
        const auto & timestamps = getTimestamps(*owned);
        size_t begin = 0;
        for (const UInt64 end : array_ends)
        {
            data.addRange(owned, begin, end - begin, timestamps);
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

        /// A single array with strictly increasing timestamps is copied once, from its source column into the result.
        if (data.appends_only && data.ranges.size() == 1)
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

    static constexpr size_t MAX_ARRAYS_TO_RESERVE = 4096;

    static const typename TimestampColumnType::Container & getTimestamps(const IColumn & tuple)
    {
        return assert_cast<const TimestampColumnType &>(assert_cast<const ColumnTuple &>(tuple).getColumn(TimeSeriesHistogramsColumns::getTimestampPosition())).getData();
    }

    /// Every array starts after the previous one ended: the result is the concatenation, copied straight into the result column.
    /// The first sample of every array but the first gets its hint against its new predecessor, the last sample of the previous array.
    static void appendRanges(const Data & data, ColumnTuple & tuple_to)
    {
        ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsAppendOnlyMerges);
        const TimeSeriesHistogramsColumnsView view{tuple_to};
        auto & hints = getCounterResetHints(tuple_to);
        for (const auto & range : data.ranges)
        {
            const size_t begin = tuple_to.size();
            tuple_to.insertRangeFrom(*range.source, range.offset, range.size);
            if (&range != &data.ranges.front())
                hints[begin] = TimeSeriesHistogramCounterResetRule::hintAfterPredecessor(view, begin - 1, begin);
        }
    }

    static ColumnUInt8::Container & getCounterResetHints(ColumnTuple & tuple)
    {
        return assert_cast<ColumnUInt8 &>(tuple.getColumn(
            TimeSeriesHistogramsColumns::getTuplePosition(TimeSeriesHistogramsColumn::CounterResetHint))).getData();
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
        auto & source = assert_cast<ColumnTuple &>(*elements);

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

        /// The winners are decided first; then every kept sample whose predecessor in the result isn't its predecessor in its input
        /// array gets its hint against the new one: the first sample of every range (the ranges are adjacent in the concatenated
        /// column, so adjacency alone doesn't tell) and the samples moved next to another one by the sort or the deduplication.
        /// The first sample of the result keeps its hint, as the first sample of an insert does.
        PaddedPODArray<UInt8> is_range_start(num_elements, 0);
        size_t range_begin = 0;
        for (const auto & range : data.ranges)
        {
            is_range_start[range_begin] = 1;
            range_begin += range.size;
        }
        auto & hints = getCounterResetHints(source);
        for (size_t i = 1; i < num_unique; ++i)
        {
            const size_t current = permutation[i];
            const size_t previous = permutation[i - 1];
            if ((current != previous + 1) || is_range_start[current])
                hints[current] = TimeSeriesHistogramCounterResetRule::hintAfterPredecessor(view, previous, current);
        }

        /// Copy consecutive source rows together, directly into the result without materializing a permuted column.
        for (size_t begin = 0; begin < num_unique;)
        {
            size_t end = begin + 1;
            while (end < num_unique && permutation[end] == permutation[end - 1] + 1)
                ++end;
            tuple_to.insertRangeFrom(source, permutation[begin], end - begin);
            begin = end;
        }
    }

    const String name;
    const DataTypePtr tuple_type;
    const SerializationPtr tuple_serialization;
};

}
