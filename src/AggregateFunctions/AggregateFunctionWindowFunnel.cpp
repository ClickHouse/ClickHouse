#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <base/arithmeticOverflow.h>
#include <base/sort.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <base/range.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Common/ListWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <limits>
#include <list>
#include <optional>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Whether `timestamp` is at most `window` after `first_timestamp`, as `timestamp <= first_timestamp + window`, but
/// without wrapping around for a window close to the maximum of `UInt64`. The timestamps keep their types, which can
/// be wider than `UInt64`.
template <typename First, typename Timestamp>
bool isInWindow(First first_timestamp, Timestamp timestamp, UInt64 window)
{
    return timestamp <= first_timestamp || timestamp - first_timestamp <= window;
}

constexpr size_t MAX_EVENTS = 32;


template <typename T>
void mergeEventsList(T & events_list, size_t prefix_size, bool prefix_sorted, bool suffix_sorted)
{
    /// either sort whole container or do so partially merging ranges afterwards
    if (!prefix_sorted && !suffix_sorted)
        ::stableSort(std::begin(events_list), std::end(events_list));
    else
    {
        const auto begin = std::begin(events_list);
        const auto middle = std::next(begin, prefix_size);
        const auto end = std::end(events_list);

        if (!prefix_sorted)
            ::stableSort(begin, middle);

        if (!suffix_sorted)
            ::stableSort(middle, end);

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

    [[nodiscard]] size_t size() const
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
            ::stableSort(std::begin(events_list), std::end(events_list));
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

        size_t size = 0;
        readBinary(size, buf);

        if (size > 100'000'000) /// The constant is arbitrary
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large size of the state of windowFunnel");

        events_list.clear();
        /// Reserving is only an optimization here, so it is derived from payload that already arrived.
        events_list.reserve(std::min(size, buf.available() / (sizeof(T) + sizeof(UInt8))));

        T timestamp{};
        UInt8 event = 0;

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

    /// The identifiers count the rows, so only a malformed state can come close to this bound. Keeping the identifiers
    /// of the deserialized and merged states below it leaves `advanceId` room to never wrap around.
    static constexpr UInt64 max_unique_id = UInt64(1) << 62;
    bool sorted = true;

    [[nodiscard]] size_t size() const
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
            UInt64 next_after_event = 0;
            if (common::addOverflow(other_event.unique_id, next_unique_id, other_event.unique_id)
                || common::addOverflow(other_event.unique_id, UInt64(1), next_after_event)
                || next_after_event > max_unique_id)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too large event identifier in the state of windowFunnel");
            new_next_unique_id = std::max(new_next_unique_id, next_after_event);
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
            ::stableSort(std::begin(events_list), std::end(events_list));
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

        size_t events_size = 0;
        readBinary(events_size, buf);

        if (events_size > 100'000'000) /// Arbitrary limit to prevent excessive memory allocation
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large size of the state of windowFunnel");

        events_list.clear();
        /// Reserving is only an optimization here, so it is derived from payload that already arrived.
        events_list.reserve(std::min(events_size, buf.available() / (sizeof(T) + sizeof(UInt8) + sizeof(UInt64))));

        T timestamp{};
        UInt8 event_type = 0;
        UInt64 unique_id = 0;

        for (size_t i = 0; i < events_size; ++i)
        {
            readBinary(timestamp, buf);
            readBinary(event_type, buf);
            readBinary(unique_id, buf);
            if (unique_id >= max_unique_id)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Too large event identifier in the state of windowFunnel");
            next_unique_id = std::max(next_unique_id, unique_id + 1);
            events_list.emplace_back(timestamp, event_type, unique_id);
        }
    }
};

/** The fastest path to every funnel level, used by the `conversion_time` mode.
  *
  * For level i the fastest path is the sequence of events 1..i inside the window that minimizes the time between the
  * first and the i-th event. If several paths take the same time, the one that finishes first is kept, and among
  * those the one whose (i-1)-th event is the earliest possible.
  * The conversion time of a step is the time between the (i-1)-th and the i-th event of that path.
  */
struct FastestPaths
{
    static constexpr UInt64 NOT_REACHED = std::numeric_limits<UInt64>::max();

    /// Indexed by the 0-based level.
    VectorWithMemoryTracking<UInt64> duration;
    VectorWithMemoryTracking<UInt64> last_step_time;

    explicit FastestPaths(size_t levels) : duration(levels, NOT_REACHED), last_step_time(levels, 0) {}

    /// Paths are offered in the order of the timestamp of their last event, so on a tie the first offered path is kept.
    void offer(size_t level, UInt64 path_duration, UInt64 step_time)
    {
        if (path_duration < duration[level])
        {
            duration[level] = path_duration;
            last_step_time[level] = step_time;
        }
    }

    /// Conversion times between consecutive steps, only for the steps up to `max_level`.
    VectorWithMemoryTracking<UInt64> conversionTimes(size_t max_level) const
    {
        VectorWithMemoryTracking<UInt64> times(duration.size() - 1, 0);
        for (size_t level = 1; level < max_level; ++level)
            if (duration[level] != NOT_REACHED)
                times[level - 1] = last_step_time[level];
        return times;
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

    // Allow re-entry/skipping of future steps
    bool allow_reentry;

    /// When the 'conversion_time' is set, the function returns the tuple (max_level, time_1_to_2, time_2_to_3, ...)
    /// where time_i_to_(i+1) is the time between the steps i and i+1 on the fastest path to the step i+1.
    /// The times are 0 for the steps above the max level.
    bool need_conversion_time;

    /// Finds the fastest path to every level without strict_once. Does not change the max level.
    ///
    /// For every level it is enough to remember one chain: the one that starts the latest. Events are processed in
    /// the order of timestamps, so a chain that starts later and completes the level now is always at least as fast
    /// as any other chain completing it now. The chain that completed the previous level first among those with the
    /// same start is kept, which is also the earliest possible event of the previous level.
    void collectFastestPathsNonStrictOnce(const AggregateFunctionWindowFunnelData<T>::TimestampEvents & events_list, FastestPaths & paths) const
    {
        /// The timestamps keep their type: `UInt128` and `UInt256` ones must not be narrowed.
        struct Chain
        {
            T start;
            T last;
        };

        VectorWithMemoryTracking<std::optional<Chain>> latest(events_size);
        /// With strict_increase a chain completed at the current timestamp can't be extended by the events with
        /// the same timestamp, so such chains are kept aside until the timestamp changes.
        VectorWithMemoryTracking<std::optional<Chain>> pending(events_size);

        auto flush_pending = [&]
        {
            for (size_t level = 0; level < events_size; ++level)
            {
                if (pending[level] && (!latest[level] || pending[level]->start > latest[level]->start))
                    latest[level] = pending[level];
                pending[level].reset();
            }
        };

        auto offer = [&](size_t level, Chain chain)
        {
            auto & target = strict_increase ? pending[level] : latest[level];
            if (!target || chain.start > target->start)
                target = chain;
        };

        bool first_event = false;
        T group_timestamp{};
        for (size_t i = 0; i < events_list.size(); ++i)
        {
            const T timestamp = events_list[i].first;
            const Int64 event_idx = static_cast<Int64>(events_list[i].second) - 1;

            if (strict_increase && timestamp != group_timestamp)
            {
                flush_pending();
                group_timestamp = timestamp;
            }

            if (strict_order && event_idx == -1)
            {
                if (first_event)
                    break;
                continue;
            }

            if (event_idx == 0)
            {
                offer(0, {timestamp, timestamp});
                first_event = true;
                continue;
            }

            const size_t level = static_cast<size_t>(event_idx);
            if (strict_order && first_event && !latest[level - 1] && !pending[level - 1])
                break;

            const auto & previous = latest[level - 1];
            if (!previous || !isInWindow(previous->start, timestamp, window))
                continue;

            /// The events are sorted by timestamp, so both differences are in `[0, window]`.
            paths.offer(level, static_cast<UInt64>(timestamp - previous->start), static_cast<UInt64>(timestamp - previous->last));
            offer(level, {previous->start, timestamp});
        }
    }

    /// Loop through the entire events_list, update the event timestamp value
    /// The level path must be 1---2---3---...---check_events_size, find the max event level that satisfied the path in the sliding window.
    /// If found, returns the max event level, else return 0.
    /// The algorithm works in O(n) time, but the overall function works in O(n * log(n)) due to sorting.
    UInt8 getEventLevelNonStrictOnce(const AggregateFunctionWindowFunnelData<T>::TimestampEvents & events_list) const
    {
        /// events_timestamp stores the timestamp of the first and previous i-th level event happen within time window
        /// The timestamps keep their type: `UInt128` and `UInt256` ones must not be narrowed.
        VectorWithMemoryTracking<std::optional<std::pair<T, T>>> events_timestamp(events_size);
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
                /// A duplicate event was found — return the current max level reached.
                /// The old code incorrectly returned events_list[i-1].second which is just
                /// the condition index of the previous event in the sorted list, not the
                /// actual max funnel level. See #37177.
                for (size_t event = events_timestamp.size(); event > 0; --event)
                {
                    if (events_timestamp[event - 1].has_value())
                        return static_cast<UInt8>(event);
                }
                return 0;
            }
            else if (strict_order && first_event && !events_timestamp[event_idx - 1].has_value())
            {
                // If allow_reentry is set, we ignore "future steps" that appear too early,
                if (allow_reentry)
                    continue;

                for (size_t event = 0; event < events_timestamp.size(); ++event)
                {
                    if (!events_timestamp[event].has_value())
                        return static_cast<UInt8>(event);
                }
            }
            else if (events_timestamp[event_idx - 1].has_value())
            {
                auto first_timestamp = events_timestamp[event_idx - 1]->first;
                bool time_matched = isInWindow(first_timestamp, timestamp, window);
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
                return static_cast<UInt8>(event);
        }
        return 0;
    }

    /// If `fastest_paths` is set, the fastest path to every level is collected too. In that case the scan doesn't stop
    /// at the first complete funnel, because a faster one may follow.
    UInt8 getEventLevelStrictOnce(
        const AggregateFunctionWindowFunnelStrictOnceData<T>::TimestampEvents & events_list, FastestPaths * fastest_paths = nullptr) const
    {
        /// Stores the timestamp of the first and last i-th level event happen within time window.
        /// `event_path` must be zero-initialized: the full array is copied by `auto prev_path = it->event_path`
        /// even though only the prefix [0..event_idx] is logically used, so leaving the tail uninitialized
        /// would read indeterminate values under MSan/ASan.
        struct EventMatchTimeWindow
        {
            T first_timestamp{};
            T last_timestamp{};
            std::array<UInt64, MAX_EVENTS> event_path{};

            EventMatchTimeWindow() = default;
            EventMatchTimeWindow(T first_ts, T last_ts)
                : first_timestamp(first_ts), last_timestamp(last_ts) {}
        };

        /// We track all possible event sequences up to the current event.
        /// It's required because one event can meet several conditions.
        /// For example: for events 'start', 'a', 'b', 'a', 'end'.
        /// The second occurrence of 'a' should be counted only once in one sequence.
        /// However, we do not know in advance if the next event will be 'b' or 'end', so we try to keep both paths.
        VectorWithMemoryTracking<ListWithMemoryTracking<EventMatchTimeWindow>> event_sequences(events_size);

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
                /// Same fix as in getEventLevelNonStrictOnce — return actual max level,
                /// not the previous event's type. See #37177.
                for (size_t event = event_sequences.size(); event > 0; --event)
                {
                    if (!event_sequences[event - 1].empty())
                        return static_cast<UInt8>(event);
                }
                return 0;
            }
            else if (strict_order && has_first_event && event_sequences[event_idx - 1].empty())
            {
                // If allow_reentry is set, ignore strict order violation for this specific match.
                if (allow_reentry)
                    continue;

                for (size_t event = 0; event < event_sequences.size(); ++event)
                {
                    if (event_sequences[event].empty())
                        return static_cast<UInt8>(event);
                }
            }
            else if (!event_sequences[event_idx - 1].empty())
            {
                auto & prev_level = event_sequences[event_idx - 1];
                for (auto it = prev_level.begin(); it != prev_level.end();)
                {
                    auto first_ts = it->first_timestamp;
                    bool time_matched = isInWindow(first_ts, timestamp, window);
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
                        /// Paths that fall out of the window are erased above, so every path has to be offered when it is created.
                        if (fastest_paths)
                            fastest_paths->offer(
                                static_cast<size_t>(event_idx),
                                static_cast<UInt64>(timestamp - first_ts),
                                static_cast<UInt64>(timestamp - it->last_timestamp));

                        /// Paths of the last level are never extended, so with `fastest_paths` one of them is enough to mark the level as reached.
                        const bool is_last_level = event_idx + 1 == events_size;
                        if (!fastest_paths || !is_last_level || event_sequences[event_idx].empty())
                        {
                            prev_path[event_idx] = unique_id;

                            auto & new_seq = event_sequences[event_idx].emplace_back(first_ts, timestamp);
                            new_seq.event_path = std::move(prev_path);
                            if (is_last_level && !fastest_paths)
                                return events_size;
                        }
                    }
                    ++it;
                }
            }
        }

        for (size_t event = event_sequences.size(); event > 0; --event)
        {
            if (!event_sequences[event - 1].empty())
                return static_cast<UInt8>(event);
        }
        return 0;
    }


    /// If `fastest_paths` is set, it is filled with the fastest path to every level.
    UInt8 getEventLevel(Data & data, FastestPaths * fastest_paths = nullptr) const
    {
        if (data.size() == 0)
            return 0;
        if (!strict_order && events_size == 1)
            return 1;

        data.sort();

        if constexpr (Data::strict_once_enabled)
        {
            return getEventLevelStrictOnce(data.events_list, fastest_paths);
        }
        else
        {
            if (fastest_paths)
                collectFastestPathsNonStrictOnce(data.events_list, *fastest_paths);
            return getEventLevelNonStrictOnce(data.events_list);
        }
    }

public:
    [[nodiscard]] String getName() const override
    {
        return "windowFunnel";
    }

    AggregateFunctionWindowFunnel(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionWindowFunnel<T, Data>>(arguments, params, getReturnType(arguments, params))
    {
        events_size = static_cast<UInt8>(arguments.size() - 1);
        window = params.at(0).safeGet<UInt64>();

        strict_deduplication = false;
        strict_order = false;
        strict_increase = false;
        allow_reentry = false;
        need_conversion_time = false;
        for (size_t i = 1; i < params.size(); ++i)
        {
            String option = params.at(i).safeGet<String>();
            if (option == "strict_deduplication")
                strict_deduplication = true;
            else if (option == "strict_order")
                strict_order = true;
            else if (option == "strict_increase")
                strict_increase = true;
            else if (option == "allow_reentry")
                allow_reentry = true;
            else if (option == "strict_once")
                /// Checked in factory
                chassert(Data::strict_once_enabled);
            else if (option == "conversion_time")
                need_conversion_time = true;
            else if (option == "strict")
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "strict is replaced with strict_deduplication in Aggregate function {}", getName());
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", getName(), option);
        }

        /// Validate option combinations
        if (allow_reentry && !strict_order)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "allow_reentry option requires strict_order to be enabled in Aggregate function {}", getName());
        }

        if (need_conversion_time && (strict_deduplication || allow_reentry))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "conversion_time option can't be combined with strict_deduplication or allow_reentry in Aggregate function {}",
                getName());
        }
    }

    /// With 'conversion_time' the result is Tuple(max_level UInt8, time_1_to_2 UInt64, ..., time_N-1_to_N UInt64), otherwise UInt8.
    static DataTypePtr getReturnType(const DataTypes & arguments, const Array & params)
    {
        bool has_conversion_time = false;
        for (size_t i = 1; i < params.size(); ++i)
            if (params.at(i).getType() == Field::Types::String && params.at(i).safeGet<String>() == "conversion_time")
                has_conversion_time = true;

        if (!has_conversion_time)
            return std::make_shared<DataTypeUInt8>();

        DataTypes types{std::make_shared<DataTypeUInt8>()};
        Strings names{"max_level"};

        const size_t events = arguments.size() - 1;
        for (size_t step = 1; step < events; ++step)
        {
            types.push_back(std::make_shared<DataTypeUInt64>());
            names.push_back("time_" + toString(step) + "_to_" + toString(step + 1));
        }

        return std::make_shared<DataTypeTuple>(types, names);
    }

    [[nodiscard]] bool allocatesMemoryInArena() const override { return false; }

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

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        auto & data = this->data(place);
        data.deserialize(buf);

        /// Event types come from untrusted serialized state. getEventLevel* uses (event - 1) to index
        /// events_timestamp / event_sequences, both sized events_size, so an out-of-range event would
        /// read and write out of bounds. Valid event types are [1, events_size]; 0 is the no-event
        /// sentinel produced only with strict_order, so it is accepted only in that mode.
        const UInt8 min_event = strict_order ? 0 : 1;
        for (const auto & event : data.events_list)
        {
            UInt8 event_type = 0;
            if constexpr (Data::strict_once_enabled)
                event_type = event.event_type;
            else
                event_type = event.second;

            if (event_type < min_event || event_type > events_size)
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Invalid event type {} in the state of function {}, must be in range [{}, {}]",
                    static_cast<UInt16>(event_type), getName(), static_cast<UInt16>(min_event), static_cast<UInt16>(events_size));
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if (!need_conversion_time)
        {
            assert_cast<ColumnUInt8 &>(to).getData().push_back(getEventLevel(this->data(place)));
            return;
        }

        auto & tuple_column = assert_cast<ColumnTuple &>(to);

        FastestPaths fastest_paths(events_size);
        const UInt8 max_level = getEventLevel(this->data(place), &fastest_paths);
        assert_cast<ColumnUInt8 &>(tuple_column.getColumn(0)).getData().push_back(max_level);

        const auto conversion_times = fastest_paths.conversionTimes(max_level);
        for (size_t step = 0; step + 1 < events_size; ++step)
            assert_cast<ColumnUInt64 &>(tuple_column.getColumn(step + 1)).getData().push_back(conversion_times[step]);
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
                    "be Unsigned Number, Date, DateTime", arguments.front()->getName(), name);
}

}

void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory & factory);
void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("windowFunnel", {createAggregateFunctionWindowFunnel, {.description = R"DOC(Searches for a chain of events in a sliding time window and returns the maximum number of events from the chain that occurred in order (the length of the longest matching prefix).)DOC", .category = FunctionDocumentation::Category::AggregateFunction}});
}

}
