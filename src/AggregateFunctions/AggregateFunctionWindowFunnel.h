#pragma once

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr auto max_events = 32;

template <typename T>
struct AggregateFunctionWindowFunnelData
{
    using TimestampEvent = std::pair<T, UInt8>;
    using TimestampEvents = PODArrayWithStackMemory<TimestampEvent, 64>;

    bool sorted = true;
    TimestampEvents events_list;

    size_t size() const
    {
        return events_list.size();
    }

    void add(T timestamp, UInt8 event)
    {
        /// Since most events should have already been sorted by timestamp.
        if (sorted && events_list.size() > 0)
        {
            if (events_list.back().first == timestamp)
                sorted = events_list.back().second <= event;
            else
                sorted = events_list.back().first <= timestamp;
        }
        events_list.emplace_back(timestamp, event);
    }

    void merge(const AggregateFunctionWindowFunnelData & other)
    {
        if (other.events_list.empty())
            return;

        const auto size = events_list.size();

        events_list.insert(std::begin(other.events_list), std::end(other.events_list));

        /// either sort whole container or do so partially merging ranges afterwards
        if (!sorted && !other.sorted)
            std::stable_sort(std::begin(events_list), std::end(events_list));
        else
        {
            const auto begin = std::begin(events_list);
            const auto middle = std::next(begin, size);
            const auto end = std::end(events_list);

            if (!sorted)
                std::stable_sort(begin, middle);

            if (!other.sorted)
                std::stable_sort(middle, end);

            std::inplace_merge(begin, middle, end);
        }

        sorted = true;
    }

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(events_list), std::end(events_list));
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

        T timestamp;
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
template <typename T, typename Data>
class AggregateFunctionWindowFunnel final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnel<T, Data>>
{
private:
    UInt64 window;
    UInt8 events_size;
    /// When the 'strict_deduplication' is set, it applies conditions only for the not repeating values.
    bool strict_deduplication;

    /// When the 'strict_order' is set, it doesn't allow interventions of other events.
    /// In the case of 'A->B->D->C', it stops finding 'A->B->C' at the 'D' and the max event level is 2.
    bool strict_order;

    /// Applies conditions only to events with strictly increasing timestamps
    bool strict_increase;

    /// Loop through the entire events_list, update the event timestamp value
    /// The level path must be 1---2---3---...---check_events_size, find the max event level that satisfied the path in the sliding window.
    /// If found, returns the max event level, else return 0.
    /// The algorithm works in O(n) time, but the overall function works in O(n * log(n)) due to sorting.
    UInt8 getEventLevel(Data & data) const
    {
        if (data.size() == 0)
            return 0;
        if (!strict_order && events_size == 1)
            return 1;

        data.sort();

        /// events_timestamp stores the timestamp of the first and previous i-th level event happen within time window
        std::vector<std::optional<std::pair<UInt64, UInt64>>> events_timestamp(events_size);
        bool first_event = false;
        for (size_t i = 0; i < data.events_list.size(); ++i)
        {
            const T & timestamp = data.events_list[i].first;
            const auto & event_idx = data.events_list[i].second - 1;
            if (strict_order && event_idx == -1)
            {
                if (first_event)
                    break;
                else
                    continue;
            }
            else if (event_idx == 0)
            {
                events_timestamp[0] = std::make_pair(timestamp, timestamp);
                first_event = true;
            }
            else if (strict_deduplication && events_timestamp[event_idx].has_value())
            {
                return data.events_list[i - 1].second;
            }
            else if (strict_order && first_event && !events_timestamp[event_idx - 1].has_value())
            {
                for (size_t event = 0; event < events_timestamp.size(); ++event)
                {
                    if (!events_timestamp[event].has_value())
                        return event;
                }
            }
            else if (events_timestamp[event_idx - 1].has_value())
            {
                auto first_timestamp = events_timestamp[event_idx - 1]->first;
                bool time_matched = timestamp <= first_timestamp + window;
                if (strict_increase)
                    time_matched = time_matched && events_timestamp[event_idx - 1]->second < timestamp;
                if (time_matched)
                {
                    events_timestamp[event_idx] = std::make_pair(first_timestamp, timestamp);
                    if (event_idx + 1 == events_size)
                        return events_size;
                }
            }
        }

        for (size_t event = events_timestamp.size(); event > 0; --event)
        {
            if (events_timestamp[event - 1].has_value())
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
        : IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnel<T, Data>>(arguments, params)
    {
        events_size = arguments.size() - 1;
        window = params.at(0).safeGet<UInt64>();

        strict_deduplication = false;
        strict_order = false;
        strict_increase = false;
        for (size_t i = 1; i < params.size(); ++i)
        {
            String option = params.at(i).safeGet<String>();
            if (option == "strict_deduplication")
                strict_deduplication = true;
            else if (option == "strict_order")
                strict_order = true;
            else if (option == "strict_increase")
                strict_increase = true;
            else if (option == "strict")
                throw Exception{"strict is replaced with strict_deduplication in Aggregate function " + getName(), ErrorCodes::BAD_ARGUMENTS};
            else
                throw Exception{"Aggregate function " + getName() + " doesn't support a parameter: " + option, ErrorCodes::BAD_ARGUMENTS};
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

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        bool has_event = false;
        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];
        /// reverse iteration and stable sorting are needed for events that are qualified by more than one condition.
        for (auto i = events_size; i > 0; --i)
        {
            auto event = assert_cast<const ColumnVector<UInt8> *>(columns[i])->getData()[row_num];
            if (event)
            {
                this->data(place).add(timestamp, i);
                has_event = true;
            }
        }

        if (strict_order && !has_event)
            this->data(place).add(timestamp, 0);
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
        assert_cast<ColumnUInt8 &>(to).getData().push_back(getEventLevel(this->data(place)));
    }
};

}
