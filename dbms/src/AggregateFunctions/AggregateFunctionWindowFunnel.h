#pragma once

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return lhs.first < rhs.first;
    }
};

struct AggregateFunctionWindowFunnelData
{
    static constexpr auto max_events = 32;
    using TimestampEvent = std::pair<UInt32, UInt8>;

    static constexpr size_t bytes_on_stack = 64;
    using TimestampEvents = PODArray<TimestampEvent, bytes_on_stack, AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;

    using Comparator = ComparePairFirst;

    bool sorted = true;
    TimestampEvents events_list;

    size_t size() const
    {
        return events_list.size();
    }

    void add(UInt32 timestamp, UInt8 event)
    {
        // Since most events should have already been sorted by timestamp.
        if (sorted && events_list.size() > 0 && events_list.back().first > timestamp)
            sorted = false;
        events_list.emplace_back(timestamp, event);
    }

    void merge(const AggregateFunctionWindowFunnelData & other)
    {
        const auto size = events_list.size();

        events_list.insert(std::begin(other.events_list), std::end(other.events_list));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(events_list), std::end(events_list), Comparator{});
        else
        {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted)
                std::stable_sort(begin, middle, Comparator{});

            if (!other.sorted)
                std::stable_sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(events_list), std::end(events_list), Comparator{});
            sorted = true;
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(events_list.size(), buf);

        for (const auto & events : events_list)
        {
            writeBinary(events.first, buf);
            writeBinary(events.second, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        /// TODO Protection against huge size

        events_list.clear();
        events_list.reserve(size);

        UInt32 timestamp;
        UInt8 event;

        for (size_t i = 0; i < size; ++i)
        {
            readBinary(timestamp, buf);
            readBinary(event, buf);
            events_list.emplace_back(timestamp, event);
        }
    }
};

/** Calculates the max event level in a sliding window.
  * The max size of events is 32, that's enough for funnel analytics
  *
  * Usage:
  * - windowFunnel(window)(timestamp, cond1, cond2, cond3, ....)
  */
class AggregateFunctionWindowFunnel final
    : public IAggregateFunctionDataHelper<AggregateFunctionWindowFunnelData, AggregateFunctionWindowFunnel>
{
private:
    UInt32 window;
    UInt8 events_size;


    // Loop through the entire events_list, update the event timestamp value
    // The level path must be 1---2---3---...---check_events_size, find the max event level that statisfied the path in the sliding window.
    // If found, returns the max event level, else return 0.
    // The Algorithm complexity is O(n).
    UInt8 getEventLevel(const AggregateFunctionWindowFunnelData & data) const
    {
        if (data.size() == 0)
            return 0;
        if (events_size == 1)
            return 1;

        const_cast<AggregateFunctionWindowFunnelData &>(data).sort();

        // events_timestamp stores the timestamp that latest i-th level event happen withing time window after previous level event.
        // timestamp defaults to -1, which unsigned timestamp value never meet
        std::vector<Int32> events_timestamp(events_size, -1);
        for (const auto & pair : data.events_list)
        {
            const auto & timestamp = pair.first;
            const auto & event_idx = pair.second - 1;
            if (event_idx == 0)
                events_timestamp[0] = timestamp;
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
        return "windowFunnel";
    }

    AggregateFunctionWindowFunnel(const DataTypes & arguments, const Array & params)
    {
        const auto time_arg = arguments.front().get();
        if (!typeid_cast<const DataTypeDateTime *>(time_arg) && !typeid_cast<const DataTypeUInt32 *>(time_arg))
            throw Exception{"Illegal type " + time_arg->getName() + " of first argument of aggregate function " + getName()
                + ", must be DateTime or UInt32"};

        for (const auto i : ext::range(1, arguments.size()))
        {
            auto cond_arg = arguments[i].get();
            if (!typeid_cast<const DataTypeUInt8 *>(cond_arg))
                throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) + " of aggregate function "
                        + getName() + ", must be UInt8",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        events_size = arguments.size() - 1;
        window = params.at(0).safeGet<UInt64>();
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto timestamp = static_cast<const ColumnVector<UInt32> *>(columns[0])->getData()[row_num];
        // reverse iteration and stable sorting are needed for events that are qualified by more than one condition.
        for (auto i = events_size; i > 0; --i)
        {
            auto event = static_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
                this->data(place).add(timestamp, i);
        }
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

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt8 &>(to).getData().push_back(getEventLevel(this->data(place)));
    }

    const char * getHeaderFilePath() const override
    {
        return __FILE__;
    }
};

}
