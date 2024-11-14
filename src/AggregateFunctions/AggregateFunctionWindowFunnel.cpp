#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <base/range.h>

#include <unordered_set>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr size_t MAX_EVENTS = 32;


template <typename T>
void mergeEventsList(T & events_list, size_t prefix_size, bool prefix_sorted, bool suffix_sorted)
{
    /// either sort whole container or do so partially merging ranges afterwards
    if (!prefix_sorted && !suffix_sorted)
        std::stable_sort(std::begin(events_list), std::end(events_list));
    else
    {
        const auto begin = std::begin(events_list);
        const auto middle = std::next(begin, prefix_size);
        const auto end = std::end(events_list);

        if (!prefix_sorted)
            std::stable_sort(begin, middle);

        if (!suffix_sorted)
            std::stable_sort(middle, end);

        std::inplace_merge(begin, middle, end);
    }
}

template <typename T>
struct AggregateFunctionWindowFunnelData
{
    static constexpr bool strict_once_enabled = false;

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

        mergeEventsList(events_list, size, sorted, other.sorted);
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

        if (size > 100'000'000) /// The constant is arbitrary
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large size of the state of windowFunnel");

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

template <typename T>
struct AggregateFunctionWindowFunnelStrictOnceData
{
    static constexpr bool strict_once_enabled = true;
    struct TimestampEvent
    {
        T timestamp;
        UInt8 event_type;
        UInt64 unique_id;

        TimestampEvent(T timestamp_, UInt8 event_type_, UInt64 unique_id_)
            : timestamp(timestamp_), event_type(event_type_), unique_id(unique_id_) {}

        bool operator<(const TimestampEvent & other) const
        {
            return std::tie(timestamp, event_type, unique_id) < std::tie(other.timestamp, other.event_type, other.unique_id);
        }

        bool operator<=(const TimestampEvent & other) const
        {
            return std::tie(timestamp, event_type, unique_id) <= std::tie(other.timestamp, other.event_type, other.unique_id);
        }
    };


    using TimestampEvents = PODArrayWithStackMemory<TimestampEvent, 64>;
    TimestampEvents events_list;

    /// Next unique identifier for events
    /// Used to distinguish events with the same timestamp that matches several conditions.
    UInt64 next_unique_id = 1;
    bool sorted = true;

    size_t size() const
    {
        return events_list.size();
    }

    void advanceId()
    {
        ++next_unique_id;
    }

    void add(T timestamp, UInt8 event_type)
    {
        TimestampEvent new_event(timestamp, event_type, next_unique_id);
        /// Check if the new event maintains the sorted order
        if (sorted && !events_list.empty())
            sorted = events_list.back() <= new_event;
        events_list.push_back(new_event);
    }

    void merge(const AggregateFunctionWindowFunnelStrictOnceData & other)
    {
        if (other.events_list.empty())
            return;

        const auto current_size = events_list.size();

        UInt64 new_next_unique_id = next_unique_id;
        events_list.reserve(current_size + other.events_list.size());
        for (auto other_event : other.events_list)
        {
            /// Assign unique IDs to the new events to prevent conflicts
            other_event.unique_id += next_unique_id;
            new_next_unique_id = std::max(new_next_unique_id, other_event.unique_id + 1);
            events_list.push_back(other_event);
        }
        next_unique_id = new_next_unique_id;

        mergeEventsList(events_list, current_size, sorted, other.sorted);

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

        for (const auto & event : events_list)
        {
            writeBinary(event.timestamp, buf);
            writeBinary(event.event_type, buf);
            writeBinary(event.unique_id, buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t events_size;
        readBinary(events_size, buf);

        if (events_size > 100'000'000) /// Arbitrary limit to prevent excessive memory allocation
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large size of the state of windowFunnel");

        events_list.clear();
        events_list.reserve(events_size);

        T timestamp;
        UInt8 event_type;
        UInt64 unique_id = 0;

        for (size_t i = 0; i < events_size; ++i)
        {
            readBinary(timestamp, buf);
            readBinary(event_type, buf);
            readBinary(unique_id, buf);
            next_unique_id = std::max(next_unique_id, unique_id + 1);
            events_list.emplace_back(timestamp, event_type, unique_id);
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
    UInt8 getEventLevelNonStrictOnce(const AggregateFunctionWindowFunnelData<T>::TimestampEvents & events_list) const
    {
        /// events_timestamp stores the timestamp of the first and previous i-th level event happen within time window
        std::vector<std::optional<std::pair<UInt64, UInt64>>> events_timestamp(events_size);
        bool first_event = false;
        for (size_t i = 0; i < events_list.size(); ++i)
        {
            const T & timestamp = events_list[i].first;
            const auto & event_idx = events_list[i].second - 1;
            if (strict_order && event_idx == -1)
            {
                if (first_event)
                    break;
                continue;
            }
            if (event_idx == 0)
            {
                events_timestamp[0] = std::make_pair(timestamp, timestamp);
                first_event = true;
            }
            else if (strict_deduplication && events_timestamp[event_idx].has_value())
            {
                return events_list[i - 1].second;
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

    UInt8 getEventLevelStrictOnce(const AggregateFunctionWindowFunnelStrictOnceData<T>::TimestampEvents & events_list) const
    {
        /// Stores the timestamp of the first and last i-th level event happen within time window
        struct EventMatchTimeWindow
        {
            UInt64 first_timestamp;
            UInt64 last_timestamp;
            std::array<UInt64, MAX_EVENTS> event_path;

            EventMatchTimeWindow() = default;
            EventMatchTimeWindow(UInt64 first_ts, UInt64 last_ts)
                : first_timestamp(first_ts), last_timestamp(last_ts) {}
        };

        /// We track all possible event sequences up to the current event.
        /// It's required because one event can meet several conditions.
        /// For example: for events 'start', 'a', 'b', 'a', 'end'.
        /// The second occurrence of 'a' should be counted only once in one sequence.
        /// However, we do not know in advance if the next event will be 'b' or 'end', so we try to keep both paths.
        std::vector<std::list<EventMatchTimeWindow>> event_sequences(events_size);

        bool has_first_event = false;
        for (size_t i = 0; i < events_list.size(); ++i)
        {
            const auto & current_event = events_list[i];
            auto timestamp = current_event.timestamp;
            Int64 event_idx = current_event.event_type - 1;
            UInt64 unique_id = current_event.unique_id;

            if (strict_order && event_idx == -1)
            {
                if (has_first_event)
                    break;
                else
                    continue;
            }
            else if (event_idx == 0)
            {
                auto & event_seq = event_sequences[0].emplace_back(timestamp, timestamp);
                event_seq.event_path[0] = unique_id;
                has_first_event = true;
            }
            else if (strict_deduplication && !event_sequences[event_idx].empty())
            {
                return events_list[i - 1].event_type;
            }
            else if (strict_order && has_first_event && event_sequences[event_idx - 1].empty())
            {
                for (size_t event = 0; event < event_sequences.size(); ++event)
                {
                    if (event_sequences[event].empty())
                        return event;
                }
            }
            else if (!event_sequences[event_idx - 1].empty())
            {
                auto & prev_level = event_sequences[event_idx - 1];
                for (auto it = prev_level.begin(); it != prev_level.end();)
                {
                    auto first_ts = it->first_timestamp;
                    bool time_matched = timestamp <= first_ts + window;
                    if (!time_matched && prev_level.size() > 1)
                    {
                        // Remove old events that are out of the window, but keep at least one
                        it = prev_level.erase(it);
                        continue;
                    }

                    auto prev_path = it->event_path;
                    chassert(event_idx > 0);

                    /// Ensure the unique_id hasn't been used in the path already
                    for (size_t j = 0; j < static_cast<size_t>(event_idx); ++j)
                    {
                        if (!time_matched)
                            break;
                        time_matched = prev_path[j] != unique_id;
                    }

                    if (time_matched && strict_increase)
                        time_matched = it->last_timestamp < timestamp;

                    if (time_matched)
                    {
                        prev_path[event_idx] = unique_id;

                        auto & new_seq = event_sequences[event_idx].emplace_back(first_ts, timestamp);
                        new_seq.event_path = std::move(prev_path);
                        if (event_idx + 1 == events_size)
                            return events_size;
                    }
                    ++it;
                }
            }
        }

        for (size_t event = event_sequences.size(); event > 0; --event)
        {
            if (!event_sequences[event - 1].empty())
                return event;
        }
        return 0;
    }


    UInt8 getEventLevel(Data & data) const
    {
        if (data.size() == 0)
            return 0;
        if (!strict_order && events_size == 1)
            return 1;

        data.sort();

        if constexpr (Data::strict_once_enabled)
            return getEventLevelStrictOnce(data.events_list);
        else
            return getEventLevelNonStrictOnce(data.events_list);
    }

public:
    String getName() const override
    {
        return "windowFunnel";
    }

    AggregateFunctionWindowFunnel(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnel<T, Data>>(arguments, params, std::make_shared<DataTypeUInt8>())
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
            else if (option == "strict_once")
                /// Checked in factory
                chassert(Data::strict_once_enabled);
            else if (option == "strict")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "strict is replaced with strict_deduplication in Aggregate function {}", getName());
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", getName(), option);
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

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

        if constexpr (Data::strict_once_enabled)
            this->data(place).advanceId();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt8 &>(to).getData().push_back(getEventLevel(this->data(place)));
    }
};


AggregateFunctionPtr
createAggregateFunctionWindowFunnel(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (params.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function {} requires at least one parameter: <window>, [option, [option, ...]]",
                        name);

    if (arguments.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function {} requires one timestamp argument and at least one event condition.", name);

    if (arguments.size() > MAX_EVENTS + 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many event arguments for aggregate function {}", name);

    for (const auto i : collections::range(1, arguments.size()))
    {
        const auto * cond_arg = arguments[i].get();
        if (!isUInt8(cond_arg))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument {} of aggregate function {}, must be UInt8",
                            cond_arg->getName(), toString(i + 1), name);
    }

    bool strict_once = params.size() > 1 && std::any_of(params.begin() + 1, params.end(), [](const auto & f) { return f.template safeGet<String>() == "strict_once"; });
    if (strict_once)
    {
        AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionWindowFunnel, AggregateFunctionWindowFunnelStrictOnceData>(*arguments[0], arguments, params));
        WhichDataType which(arguments.front().get());
        if (res)
            return res;
        if (which.isDate())
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDate::FieldType, AggregateFunctionWindowFunnelStrictOnceData<DataTypeDate::FieldType>>>(arguments, params);
        if (which.isDateTime())
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDateTime::FieldType, AggregateFunctionWindowFunnelStrictOnceData<DataTypeDateTime::FieldType>>>(arguments, params);
    }
    else
    {
        AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionWindowFunnel, AggregateFunctionWindowFunnelData>(*arguments[0], arguments, params));
        WhichDataType which(arguments.front().get());
        if (res)
            return res;
        if (which.isDate())
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDate::FieldType, AggregateFunctionWindowFunnelData<DataTypeDate::FieldType>>>(arguments, params);
        if (which.isDateTime())
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDateTime::FieldType, AggregateFunctionWindowFunnelData<DataTypeDateTime::FieldType>>>(arguments, params);
    }
    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of aggregate function {}, must "
                    "be Unsigned Number, Date, DateTime", arguments.front().get()->getName(), name);
}

}

void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("windowFunnel", createAggregateFunctionWindowFunnel);
}

}
