#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <unordered_set>
#include <sstream>
#include <iostream>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ArenaAllocator.h>
#include <ext/range.h>
#include <Common/typeid_cast.h>

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
    using TimestampEvent = std::pair<UInt64, UInt8>;

    static constexpr size_t bytes_on_stack = 64;
    using TimestampEvents = PODArray<TimestampEvent, bytes_on_stack,  AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;
    
    using Comparator = ComparePairFirst;

    bool sorted = true;
    TimestampEvents events_list;

    size_t size() const
    {
        return events_list.size();
    }

    void add(UInt64 timestamp, UInt8 event)
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
            std::sort(std::begin(events_list), std::end(events_list), Comparator{});
        else
        {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted)
                std::sort(begin, middle, Comparator{});

            if (!other.sorted)
                std::sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
       {
            std::sort(std::begin(events_list), std::end(events_list), Comparator{});
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

        events_list.clear();
        events_list.resize(size);

        UInt64 timestamp;
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
  * - windowFunnel(window_size)(window_column, event_condition1, event_condition2, event_condition3, ....)
  */

class AggregateFunctionWindowFunnel final : public IAggregateFunctionDataHelper<AggregateFunctionWindowFunnelData, AggregateFunctionWindowFunnel>
{
private:
    UInt64 window;
    Logger * log = &Logger::get("AggregateFunctionWindowFunnel");
    UInt8  check_events_size;


    // Loop through the entire events_list
    // If the timestamp window size between current event and pre event( that's event-1) is less than the window value, then update current event's timestamp.
    // Returns the max event level.
    // The Algorithm complexity is O(n).

    UInt8 match(const AggregateFunctionWindowFunnelData & data) const
    {
        if(data.events_list.empty()) return 0;
        if (check_events_size == 1)
            return 1;

        const_cast<AggregateFunctionWindowFunnelData &>(data).sort();

        std::vector<UInt64> events_timestamp(check_events_size,0);
        for(const auto i : ext::range(0, data.size()))
        {
            const auto & event = (data.events_list)[i - 1].second - 1;
            const auto & timestamp = (data.events_list)[i - 1].first;
            if(event == 0)
                events_timestamp[0] = timestamp;
            else if(timestamp <= events_timestamp[event - 1] + window)
            {
                events_timestamp[event] = timestamp;
                if(event == check_events_size) return check_events_size;
            }
        }

        for(const auto i : ext::range(data.size() - 1, 0))
        {
            if(events_timestamp[i]) return i + 1;
        }
        return 0;
    }

public:

    String getName() const override { return "windowFunnel"; }

    AggregateFunctionWindowFunnel(const DataTypes & arguments, const Array & params)
    {
        DataTypePtr timestampType = arguments[0];

        if (!(timestampType->isUnsignedInteger()))
            throw Exception("Illegal type " + timestampType->getName() + " of argument for aggregate function " + getName() + " (1 arg, timestamp: UIntXX)",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

       check_events_size = arguments.size() - 1;
       if(check_events_size > AggregateFunctionWindowFunnelData::max_events)
           throw Exception{"Aggregate function " + getName() + " supports up to " +
                   toString(AggregateFunctionWindowFunnelData::max_events) + " event arguments.",
               ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION};

       for(const auto i : ext::range(1, arguments.size()))
       {
          auto cond_arg = arguments[i].get();
          if (!typeid_cast<const DataTypeUInt8 *>(cond_arg))
               throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) +
                       " of aggregate function " + getName() + ", must be UInt8",
                   ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
       }

        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly 1 args(window_num).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        window = params[0].safeGet<UInt64>();
        LOG_TRACE(log, std::fixed << std::setprecision(3) << "setParameters, window: " << window << " check events:" << check_events_size);
    }


    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        UInt8 event_level = 0;
        for(const auto i : ext::range(1,check_events_size))
        {
           auto event = static_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
           if(event){
               event_level = i;
               break;
           }
        }
        this->data(place).add( //
          static_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num],
          event_level
        );
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
        static_cast<ColumnUInt8 &>(to).getData().push_back(match(this->data(place)));
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
