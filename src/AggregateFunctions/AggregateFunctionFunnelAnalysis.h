#pragma once

#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_set>
#include <map>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Common/ArenaAllocator.h>
#include <Common/ComparePair.h>
#include <Common/assert_cast.h>
#include <common/logger_useful.h>
#include <Poco/Logger.h>

#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

static constexpr auto max_events = 32;
template <typename T>
struct AggregateFunctionFunnelAnalysisData
{
    using TimestampEvent = std::pair<T, UInt8>;
    using TimestampEvents = PODArrayWithStackMemory<TimestampEvent, 64>;
    using Comparator = ComparePair;

    size_t _size = 0;
    std::vector<TimestampEvents> events_lists_to_merge;

    size_t size() const
    {
        return _size;
    }

    void add(UInt8 event_idx, const ColumnVector<T> * column, size_t begin, size_t end)
    {
        TimestampEvents events;
        for (size_t i=begin; i<end; i++)
            events.emplace_back(column->getData()[i], event_idx);
        events_lists_to_merge.emplace_back(std::move(events));
        _size += end-begin;
    }

    void merge(const AggregateFunctionFunnelAnalysisData & other)
    {
        if (other.events_lists_to_merge.empty())
            return;
        for (auto & events : other.events_lists_to_merge)
            events_lists_to_merge.emplace_back(std::begin(events), std::end(events));
        _size += other.size();
    }

    TimestampEvents & finalMerge()
    {
        TimestampEvents & list = doFinalMerge(events_lists_to_merge, 0, events_lists_to_merge.size() - 1);
        return list;
    }

    TimestampEvents & doFinalMerge(std::vector<TimestampEvents> & events, size_t begin, size_t end)
    {
        if (begin == end)
            return events[begin];
        int mid = (begin + end) / 2;
        TimestampEvents & left = doFinalMerge(events, begin, mid);
        TimestampEvents & right = doFinalMerge(events, mid + 1, end);
        const auto size = left.size();
        left.insert(std::begin(right), std::end(right));
        const auto b = std::begin(left);
        const auto m = std::next(b, size);
        const auto e = std::end(left);

        std::inplace_merge(b, m, e, Comparator{});
        return left;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(events_lists_to_merge.size(), buf);
        for (const auto & events : events_lists_to_merge)
        {
            writeBinary(events.size(), buf);
            for (const auto & event : events)
            {
                writeBinary(event.first, buf);
                writeBinary(event.second, buf);
            }
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size = 0;
        size_t vector_size;
        readBinary(vector_size, buf);

        /// TODO Protection against huge size
        T timestamp;
        UInt8 event;

        for (size_t i = 0; i < vector_size; ++i)
        {
            size_t array_size;
            readBinary(array_size, buf);
            TimestampEvents events(array_size);
            for (size_t j = 0; j < array_size; ++j)
            {
                readBinary(timestamp, buf);
                readBinary(event, buf);
                events.emplace_back(timestamp, event);
                size++;
            }
            events_lists_to_merge.emplace_back(std::begin(events), std::end(events));
        }
        _size = size;
    }
};

/** Calculates the max event level in a sliding window.
  * The max size of events is 32, that's enough for funnel analytics
  * T : the type of timestamp.
  * Data : the type of the aggregate data.
  *
  * Usage:
  * - funnelAnalysis(window, strict, strict_order, event1, event2, event3, ....)(event, timestamps)
  */
template <typename T, typename Data>
class AggregateFunctionFunnelAnalysis final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionFunnelAnalysis<T, Data>>
{
private:
    using IndexByName = std::unordered_map<String, size_t>;

    UInt64 window;
    UInt8 events_size;
    UInt8 strict;   // When the 'strict' is set, it applies conditions only for the not repeating values.
    UInt8 strict_order; // When the 'strict_order' is set, it doesn't allow interventions of other events.
                        // In the case of 'A->B->D->C', it stops finding 'A->B->C' at the 'D' and the max event level is 2.
    IndexByName events_index;

    // Loop through the entire events_list, update the event timestamp value
    // The level path must be 1---2---3---...---check_events_size, find the max event level that satisfied the path in the sliding window.
    // If found, returns the max event level, else return 0.
    // The Algorithm complexity is O(nlogk), in which n is the number of events, and k is the number of event level.
    UInt8 getEventLevel(Data & data) const
    {
        if (data.size() == 0)
            return 0;
        if (!strict_order && events_size == 1)
            return 1;

        const auto & events_list = data.finalMerge();
        /// events_timestamp stores the timestamp that latest i-th level event happen within time window after previous level event.
        /// timestamp defaults to -1, which unsigned timestamp value never meet
        /// there may be some bugs when UInt64 type timstamp overflows Int64, but it works on most cases.
        std::vector<Int64> events_timestamp(events_size, -1);
        bool first_event = false;
        for (const auto & pair : events_list)
        {
            const T & timestamp = pair.first;
            const auto & event_idx = pair.second - 1;

            if (strict_order && event_idx == -1)
            {
                if (first_event)
                    break;
                else
                    continue;
            }
            else if (event_idx == 0)
            {
                events_timestamp[0] = timestamp;
                first_event = true;
            }
            else if (strict && events_timestamp[event_idx] >= 0)
            {
                return event_idx + 1;
            }
            else if (strict_order && first_event && events_timestamp[event_idx - 1] == -1)
            {
                for (size_t event = 0; event < events_timestamp.size(); ++event)
                {
                    if (events_timestamp[event] == -1)
                        return event;
                }
            }
            else if (events_timestamp[event_idx - 1] >= 0 && timestamp <= events_timestamp[event_idx - 1] + window)
            {
                events_timestamp[event_idx] = events_timestamp[event_idx - 1];
                if (event_idx + 1 == events_size)
                    return events_size;
            }
        }
        for (size_t event = events_timestamp.size(); event > 0; --event)
        {
            if (events_timestamp[event - 1] >= 0)
                return event;
        }
        return 0;
    }

public:
    String getName() const override
    {
        return "funnelAnalysis";
    }

    AggregateFunctionFunnelAnalysis(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionFunnelAnalysis<T, Data>>(arguments, params)
    {
        window = params.at(0).safeGet<UInt64>();
        strict = params.at(1).get<UInt8>();
        strict_order = params.at(2).get<UInt8>();
        events_size = params.size() - 3;

        for (size_t i = 3; i < params.size(); ++i)
        {
            events_index[params.at(i).safeGet<String>()] = i-2;
        }
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        return std::make_shared<AggregateFunctionNullVariadic<false, false, false>>(nested_function, arguments, params);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto event = checkAndGetColumn<ColumnString>(columns[0])->getDataAt(row_num).toString();
        const auto col_array = checkAndGetColumn<ColumnArray>(columns[1]);
        const auto col_nested = checkAndGetColumn<ColumnVector<T>>(&col_array->getData());
        const auto index_it = events_index.find(event);
        const ColumnArray::Offsets & offsets = col_array->getOffsets();
        const ColumnArray::Offset begin = row_num == 0 ? 0 : offsets[row_num-1];
        const ColumnArray::Offset & end = offsets[row_num];

        // TODO : whether to allow other events except the events in events_index to come to calculation ?
        if (index_it != events_index.end())
            this->data(place).add(index_it->second, col_nested, begin, end);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto level = getEventLevel(this->data(place));
        assert_cast<ColumnUInt8 &>(to).getData().push_back(level);
    }
};

}
