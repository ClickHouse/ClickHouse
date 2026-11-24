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
#include <optional>
#include <span>
#include <utility>


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

/// The state of `timeSeriesGroupArray` over histogram samples: the added arrays as references to ranges of their source tuple
/// columns, so a merge of float-only rows allocates nothing and no sample is copied before the result is built. Empty arrays are not
/// recorded; `sort` replaces the ranges by those of the result. The state lives on the heap and holds the columns its ranges refer to;
/// a deserialized state holds the column it read.
template <typename TimestampType>
struct AggregateFunctionTimeSeriesGroupArrayHistogramsData
{
    using TimestampColumnType = ColumnVectorOrDecimal<TimestampType>;
    using Timestamps = typename TimestampColumnType::Container;

    /// Rows `offset` to `offset + size` of `source`, a tuple column held by `sources`, with the timestamps of the first and the last row.
    struct Range
    {
        const ColumnTuple * source;
        size_t offset;
        size_t size;
        TimestampType first;
        TimestampType last;
    };

    /// How far the timestamps of the ranges are known to strictly increase. The combination of two states is the lesser one.
    enum class RangesOrder : UInt8
    {
        None,
        /// Within every range, so its `first` and `last` bound it.
        LocallySorted,
        /// Also across the ranges in their stored order: the result is the concatenation of the ranges.
        GloballySorted,
    };

    VectorWithMemoryTracking<Range> ranges;
    /// The columns the ranges refer to.
    VectorWithMemoryTracking<ColumnTuple::Ptr> sources;
    RangesOrder ranges_order = RangesOrder::GloballySorted;

    static const Timestamps & getTimestamps(const ColumnTuple & tuple)
    {
        return assert_cast<const TimestampColumnType &>(tuple.getColumn(TimeSeriesHistogramsColumns::getTimestampPosition())).getData();
    }

    /// Adds rows `offset` to `offset + size` of `source`, a tuple column of histogram samples.
    void addRange(const ColumnPtr & source, size_t offset, size_t size)
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*source);
        const auto & timestamps = getTimestamps(tuple);
        const auto begin = timestamps.begin() + offset;
        const auto end = begin + size;
        const TimestampType first = *begin;
        if (ranges_order != RangesOrder::None)
        {
            /// Direct aggregate inputs can contain duplicate timestamps within one array, unlike the arrays written by the sink.
            const bool locally_sorted = std::adjacent_find(begin, end, [](auto lhs, auto rhs)
            {
                return lhs >= rhs;
            }) == end;
            const bool follows_previous = ranges.empty() || ranges.back().last < first;
            const RangesOrder range_order = !locally_sorted
                ? RangesOrder::None
                : (follows_previous ? RangesOrder::GloballySorted : RangesOrder::LocallySorted);
            ranges_order = std::min(ranges_order, range_order);
        }
        /// The rows of one block reach a state one after another, so a single reference usually holds all of their ranges.
        if (sources.empty() || sources.back().get() != &tuple)
            sources.push_back(tuple.getPtr());
        ranges.push_back(Range{&tuple, offset, size, first, *(end - 1)});
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

        const RangesOrder joint_order = (ranges.back().last < rhs.ranges.front().first)
            ? RangesOrder::GloballySorted
            : RangesOrder::LocallySorted;
        ranges_order = std::min({ranges_order, rhs.ranges_order, joint_order});
        ranges.insert(ranges.end(), rhs.ranges.begin(), rhs.ranges.end());
        sources.insert(sources.end(), rhs.sources.begin(), rhs.sources.end());
    }

    /// Makes the state `GloballySorted`, the ranges those of the result, deduplicated by `TimeSeriesDuplicateTimestampRule`: orders
    /// the ranges by their first timestamps in place and replaces the overlapping ones by the runs of their rows. Counts the result in
    /// `TimeSeriesGroupArrayHistogramsAppendOnlyMerges` or `TimeSeriesGroupArrayHistogramsSortedMerges`.
    void sort()
    {
        if (ranges.empty())
            return;

        if (ranges_order == RangesOrder::GloballySorted)
        {
            /// A single array is not a merge.
            if (ranges.size() > 1)
                ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsAppendOnlyMerges);
            return;
        }

        VectorWithMemoryTracking<Range> merged;
        /// The overlaps of a range whose timestamps don't strictly increase are unknown: `first` and `last` don't bound it.
        if (ranges_order == RangesOrder::None)
        {
            ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsSortedMerges);
            mergeOverlaps(ranges, merged);
        }
        else
        {
            ::sort(ranges.begin(), ranges.end(), [](const Range & lhs, const Range & rhs) { return lhs.first < rhs.first; });
            /// An equal timestamp is an overlap too: its duplicates must be dropped.
            const auto overlap = [](const Range & lhs, const Range & rhs) { return !(lhs.last < rhs.first); };
            if (std::adjacent_find(ranges.begin(), ranges.end(), overlap) == ranges.end())
            {
                ranges_order = RangesOrder::GloballySorted;
                ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsAppendOnlyMerges);
                return;
            }

            ProfileEvents::increment(ProfileEvents::TimeSeriesGroupArrayHistogramsSortedMerges);
            for (size_t group_begin = 0; group_begin != ranges.size();)
            {
                TimestampType group_last = ranges[group_begin].last;
                size_t group_end = group_begin + 1;
                while (group_end != ranges.size() && !(group_last < ranges[group_end].first))
                {
                    group_last = std::max(group_last, ranges[group_end].last);
                    ++group_end;
                }

                if (group_end == group_begin + 1)
                    merged.push_back(ranges[group_begin]);
                else
                    mergeOverlaps(std::span<const Range>(ranges).subspan(group_begin, group_end - group_begin), merged);
                group_begin = group_end;
            }
        }

        ranges.swap(merged);
        ranges_order = RangesOrder::GloballySorted;
    }

private:
    /// Sample `row` of the source of the range `group[member]`.
    struct Sample
    {
        TimestampType timestamp;
        size_t member;
        size_t row;
    };

    /// Appends to `merged` the runs of the samples of the ranges `group`, which overlap in time, sorted by timestamp and one per timestamp.
    void mergeOverlaps(std::span<const Range> group, VectorWithMemoryTracking<Range> & merged) const
    {
        size_t num_samples = 0;
        for (const Range & range : group)
            num_samples += range.size;

        VectorWithMemoryTracking<TimeSeriesHistogramPayloadColumnsView> views;
        views.reserve(group.size());
        PODArray<Sample> samples;
        samples.reserve(num_samples);
        for (size_t member = 0; member != group.size(); ++member)
        {
            const Range & range = group[member];
            views.emplace_back(*range.source);
            const auto & timestamps = getTimestamps(*range.source);
            for (size_t row = range.offset; row != range.offset + range.size; ++row)
                samples.push_back(Sample{timestamps[row], member, row});
        }
        /// Equal timestamps are ordered by range, so of equal samples the one of the first range is kept: duplicates from a retried
        /// insert become consecutive rows of that range, one run.
        ::sort(samples.begin(), samples.end(), [](const Sample & lhs, const Sample & rhs)
        {
            if (lhs.timestamp != rhs.timestamp)
                return lhs.timestamp < rhs.timestamp;
            return std::pair(lhs.member, lhs.row) < std::pair(rhs.member, rhs.row);
        });

        /// Collapses each run of equal timestamps into the winning sample, in place at the beginning of `samples`. The rule orders
        /// the whole payload but the counter hints, so the payload of the winner doesn't depend on the order of the ranges. Neither
        /// does its hint, for normalized arrays: a winner continuing its run has the hint detection would give, any other one starts
        /// a range of the result, whose first hint `appendRanges` sets.
        size_t num_unique = 0;
        for (const Sample & candidate : samples)
        {
            if (num_unique == 0 || candidate.timestamp != samples[num_unique - 1].timestamp)
            {
                samples[num_unique++] = candidate;
                continue;
            }

            Sample & kept = samples[num_unique - 1];
            if (TimeSeriesDuplicateTimestampRule::compareHistogramSamples(views[candidate.member], candidate.row, views[kept.member], kept.row) > 0)
                kept = candidate;
        }
        samples.resize(num_unique);

        /// The first sample of a run gets its hint detected, the others keep theirs, which describe the same predecessor.
        for (size_t i = 0; i != samples.size(); ++i)
        {
            const Sample & sample = samples[i];
            const bool continues_run = (i != 0) && (samples[i - 1].member == sample.member) && (samples[i - 1].row + 1 == sample.row);
            if (continues_run)
            {
                Range & run = merged.back();
                ++run.size;
                run.last = sample.timestamp;
            }
            else
            {
                merged.push_back(Range{group[sample.member].source, sample.row, 1, sample.timestamp, sample.timestamp});
            }
        }
    }
};

/// Aggregate function merging arrays of histogram samples of the shape of the `histograms` column of a TimeSeries table (see
/// TimeSeriesHistogramsColumns), each sorted by timestamp, into one sorted and deduplicated by `TimeSeriesDuplicateTimestampRule`.
/// A sample whose predecessor may have changed gets the hint of TimeSeriesHistogramCounterResetRule against its predecessor in the
/// result, the first sample of the result an unknown hint unless it is a gauge; the other hints are kept, so every sample but the
/// first of an array must have the detected hint, as the sink stores them. Then the result doesn't depend on the order of the merges,
/// as `SimpleAggregateFunction` requires. The result type is the argument type.
template <typename TimestampType>
class AggregateFunctionTimeSeriesGroupArrayHistograms final
    : public IAggregateFunctionDataHelper<AggregateFunctionTimeSeriesGroupArrayHistogramsData<TimestampType>, AggregateFunctionTimeSeriesGroupArrayHistograms<TimestampType>>
{
public:
    using Data = AggregateFunctionTimeSeriesGroupArrayHistogramsData<TimestampType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionTimeSeriesGroupArrayHistograms<TimestampType>>;

    AggregateFunctionTimeSeriesGroupArrayHistograms(const String & name_, const DataTypes & argument_types_)
        : Base(argument_types_, {}, createResultType(argument_types_))
        , name(name_)
        , tuple_type(typeid_cast<const DataTypeArray &>(*argument_types_[0]).getNestedType())
        , tuple_serialization(tuple_type->getDefaultSerialization())
    {
    }

    [[nodiscard]] String getName() const override { return name; }

    /// The result type is the argument type without the custom name of `SimpleAggregateFunction`.
    static DataTypePtr createResultType(const DataTypes & argument_types_)
    {
        const auto & array_type = typeid_cast<const DataTypeArray &>(*argument_types_[0]);
        const auto & tuple_type = typeid_cast<const DataTypeTuple &>(*array_type.getNestedType());
        if (tuple_type.hasExplicitNames())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements(), tuple_type.getElementNames()));
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(tuple_type.getElements()));
    }

    [[nodiscard]] bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto * array = checkAndGetColumn<ColumnArray>(columns[0]);
        if (!array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Argument of aggregate function {} must be an array, got {}", name, columns[0]->getName());

        const size_t size = array->getSize(row_num);
        if (!size)
            return;
        const size_t offset = array->getOffsets()[static_cast<ssize_t>(row_num) - 1];
        this->data(place).addRange(array->getDataPtr(), offset, size);
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

        writeVarUInt(end, buf);
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
        /// Larger states grow as their offsets are read, like the float samples in `timeSeriesGroupArray`.
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
        size_t begin = 0;
        for (const UInt64 end : array_ends)
        {
            data.addRange(owned, begin, end - begin);
            begin = end;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & array_to = assert_cast<ColumnArray &>(to);
        auto & offsets_to = array_to.getOffsets();
        auto & tuple_to = assert_cast<ColumnTuple &>(array_to.getData());

        Data & data = this->data(place);
        data.sort();
        appendRanges(data.ranges, tuple_to);
        offsets_to.push_back(tuple_to.size());
    }

private:
    /// The version of the serialized state, checked on deserialization: the state embeds the tuple's binary serialization,
    /// so a server with another element list must refuse it instead of misreading it.
    static constexpr UInt16 FORMAT_VERSION = 1;

    static constexpr size_t MAX_ARRAYS_TO_RESERVE = 4096;

    /// Appends the rows of `ranges`, sorted and disjoint in time, to `tuple_to`, with new hints for the first samples of the ranges.
    static void appendRanges(std::span<const typename Data::Range> ranges, ColumnTuple & tuple_to)
    {
        if (ranges.empty())
            return;

        auto it = ranges.begin();
        const auto end = ranges.end();
        const size_t first = tuple_to.size();
        tuple_to.insertRangeFrom(*it->source, it->offset, it->size);
        /// The hint of the first sample describes its predecessor in its array, which isn't in the result, e.g. cut off by
        /// `timeSeriesSliceSortedArray`, and can differ between equal duplicates. A gauge marker describes the sample itself.
        auto & hints = assert_cast<ColumnInt8 &>(tuple_to.getColumn(
            TimeSeriesHistogramsColumns::getTuplePosition(TimeSeriesHistogramsColumn::CounterResetHint))).getData();
        if (TimeSeriesHistogramCounterResetHint{hints[first]} != TimeSeriesHistogramCounterResetHint::Gauge)
            hints[first] = static_cast<Int8>(TimeSeriesHistogramCounterResetHint::Unknown);
        ++it;
        /// Skip unused views creation.
        if (it == end)
            return;

        const TimeSeriesHistogramsColumnsMutableView view{tuple_to};
        const TimeSeriesHistogramPayloadColumnsView payload{view};
        /// Of the samples of a range only the first has a new predecessor, the last sample of the previous range.
        for (; it != end; ++it)
        {
            const size_t begin = tuple_to.size();
            tuple_to.insertRangeFrom(*it->source, it->offset, it->size);
            view.setCounterResetHintAt(begin, TimeSeriesHistogramCounterResetRule::detectReset(payload, begin - 1, begin));
        }
    }

    const String name;
    const DataTypePtr tuple_type;
    const SerializationPtr tuple_serialization;
};

}
