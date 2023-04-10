#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <unordered_set>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>

#include <sstream>
#include <iostream>

#include <Common/FieldVisitors.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return lhs.first < rhs.first;
    }
};

//以一个元素为基本，计算其他元素与它之间的时长，之后将这些差相加 uasge : [A, B, C, A, D] result is (B-A)+(D-A)
struct AggregateFunctionSessionDurationData
{

	static constexpr auto max_events = 32;
    using TimestampEvent = std::pair<UInt64, UInt8>;

    static constexpr size_t bytes_on_stack = 64;
    using TimestampEvents = PODArray<TimestampEvent, bytes_on_stack, AllocatorWithStackMemory<Allocator<false>, bytes_on_stack>>;

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

    void merge(const AggregateFunctionSessionDurationData & other)
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

class AggregateFunctionSessionDuration final : public IAggregateFunctionDataHelper<AggregateFunctionSessionDurationData, AggregateFunctionSessionDuration>
{
private:
	using Event = String;

    Event targetEvent;

	
public:
	
	String getName() const override { return "sessionDuration"; }
	
	AggregateFunctionSessionDuration(const Array & params)
	    : IAggregateFunctionDataHelper<AggregateFunctionSessionDurationData, AggregateFunctionSessionDuration>({}, params)
        {						
        if (params.size() != 1)
			throw Exception("Aggregate function requires at least two arguments of Array type.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		targetEvent = params[0].safeGet<String>();
	}
	
	
	String toString(Event event){
        std::ostringstream oss;
		
		oss << event;
        oss << ",";
        oss << "END";
		
        return oss.str();
    }
	
	DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }
	
	void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
	{
		const auto timestamp = static_cast<const ColumnVector<UInt64> *>(columns[0])->getData()[row_num];
        String event = String(columns[1]->getDataAt(row_num));
        if(event != targetEvent){
            this->data(place).add(timestamp, 0);
        }else{
            this->data(place).add(timestamp, 1);
        }
	}
		
	void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }
	
	void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }
		
	void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        const_cast<AggregateFunctionSessionDurationData &>(data).sort();
        
        auto total_len = data.events_list.size();
        bool a = false;
        size_t sum = 0;
        size_t tmp_timestamp = 0;
        for(size_t i = 0; i < total_len; i++)
        {
            const auto event = (data.events_list)[i].second;
            
            if(!a && 1 == event)
            {
                a = true;
                tmp_timestamp = (data.events_list)[i].first;
                continue;
            }
            if(a && 1 != event)
            {
                a = false;
                size_t timestamp =  (data.events_list)[i].first;
                sum += (timestamp - tmp_timestamp);
            }
        }
        const auto end_event = (data.events_list)[total_len -1].second;
        if(a && 1 == end_event)
        {
            sum += ((data.events_list)[total_len -1].first - tmp_timestamp);
        }
        static_cast<ColumnUInt64 &>(to).getData().push_back(sum);
    }

};

}
		
		
		
		
		
		
		
