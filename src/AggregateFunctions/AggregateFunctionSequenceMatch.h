#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <base/range.h>
#include <base/sort.h>
#include <Common/PODArray.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <bitset>
#include <stack>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_SLOW;
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

/// helper type for comparing `std::pair`s using solely the .first member
template <template <typename> class Comparator>
struct ComparePairFirst final
{
    template <typename T1, typename T2>
    bool operator()(const std::pair<T1, T2> & lhs, const std::pair<T1, T2> & rhs) const
    {
        return Comparator<T1>{}(lhs.first, rhs.first);
    }
};

static constexpr size_t max_events = 32;

template <typename T>
struct AggregateFunctionSequenceMatchData final
{
    using Timestamp = T;
    using Events = std::bitset<max_events>;
    using TimestampEvents = std::pair<Timestamp, Events>;
    using Comparator = ComparePairFirst<std::less>;

    bool sorted = true;
    PODArrayWithStackMemory<TimestampEvents, 64> events_list;
    /// sequenceMatch conditions met at least once in events_list
    std::bitset<max_events> conditions_met;

    void add(const Timestamp timestamp, const Events & events)
    {
        /// store information exclusively for rows with at least one event
        if (events.any())
        {
            events_list.emplace_back(timestamp, events);
            sorted = false;
            conditions_met |= events;
        }
    }

    void merge(const AggregateFunctionSequenceMatchData & other)
    {
        if (other.events_list.empty())
            return;

        events_list.insert(std::begin(other.events_list), std::end(other.events_list));
        sorted = false;
        conditions_met |= other.conditions_met;
    }

    void sort()
    {
        if (sorted)
            return;

        ::sort(std::begin(events_list), std::end(events_list), Comparator{});
        sorted = true;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(sorted, buf);
        writeBinary(events_list.size(), buf);

        for (const auto & events : events_list)
        {
            writeBinary(events.first, buf);
            writeBinary(events.second.to_ulong(), buf);
        }
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(sorted, buf);

        size_t size;
        readBinary(size, buf);

        events_list.clear();
        events_list.reserve(size);

        for (size_t i = 0; i < size; ++i)
        {
            Timestamp timestamp;
            readBinary(timestamp, buf);

            UInt64 events;
            readBinary(events, buf);

            events_list.emplace_back(timestamp, Events{events});
        }
    }
};


/// Max number of iterations to match the pattern against a sequence, exception thrown when exceeded
constexpr auto sequence_match_max_iterations = 1000000;


template <typename T, typename Data, typename Derived>
class AggregateFunctionSequenceBase : public IAggregateFunctionDataHelper<Data, Derived>
{
public:
    AggregateFunctionSequenceBase(const DataTypes & arguments, const Array & params, const String & pattern_)
        : IAggregateFunctionDataHelper<Data, Derived>(arguments, params)
        , pattern(pattern_)
    {
        arg_count = arguments.size();
        parsePattern();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        typename Data::Events events;
        for (const auto i : collections::range(1, arg_count))
        {
            const auto event = assert_cast<const ColumnUInt8 *>(columns[i])->getData()[row_num];
            events.set(i - 1, event);
        }

        this->data(place).add(timestamp, events);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return this->getName() == rhs.getName() && this->haveEqualArgumentTypes(rhs);
    }

private:
    enum class PatternActionType
    {
        SpecificEvent,
        AnyEvent,
        KleeneStar,
        TimeLessOrEqual,
        TimeLess,
        TimeGreaterOrEqual,
        TimeGreater,
        TimeEqual
    };

    struct PatternAction final
    {
        PatternActionType type;
        std::uint64_t extra;

        PatternAction() = default;
        explicit PatternAction(const PatternActionType type_, const std::uint64_t extra_ = 0) : type{type_}, extra{extra_} {}
    };

    using PatternActions = PODArrayWithStackMemory<PatternAction, 64>;

    Derived & derived() { return static_cast<Derived &>(*this); }

    void parsePattern()
    {
        actions.clear();
        actions.emplace_back(PatternActionType::KleeneStar);

        dfa_states.clear();
        dfa_states.emplace_back(true);

        pattern_has_time = false;

        const char * pos = pattern.data();
        const char * begin = pos;
        const char * end = pos + pattern.size();

        auto throw_exception = [&](const std::string & msg)
        {
            throw Exception{msg + " '" + std::string(pos, end) + "' at position " + toString(pos - begin), ErrorCodes::SYNTAX_ERROR};
        };

        auto match = [&pos, end](const char * str) mutable
        {
            size_t length = strlen(str);
            if (pos + length <= end && 0 == memcmp(pos, str, length))
            {
                pos += length;
                return true;
            }
            return false;
        };

        while (pos < end)
        {
            if (match("(?"))
            {
                if (match("t"))
                {
                    PatternActionType type;

                    if (match("<="))
                        type = PatternActionType::TimeLessOrEqual;
                    else if (match("<"))
                        type = PatternActionType::TimeLess;
                    else if (match(">="))
                        type = PatternActionType::TimeGreaterOrEqual;
                    else if (match(">"))
                        type = PatternActionType::TimeGreater;
                    else if (match("=="))
                        type = PatternActionType::TimeEqual;
                    else
                        throw_exception("Unknown time condition");

                    UInt64 duration = 0;
                    const auto * prev_pos = pos;
                    pos = tryReadIntText(duration, pos, end);
                    if (pos == prev_pos)
                        throw_exception("Could not parse number");

                    if (actions.back().type != PatternActionType::SpecificEvent &&
                        actions.back().type != PatternActionType::AnyEvent &&
                        actions.back().type != PatternActionType::KleeneStar)
                        throw Exception{"Temporal condition should be preceded by an event condition", ErrorCodes::BAD_ARGUMENTS};

                    pattern_has_time = true;
                    actions.emplace_back(type, duration);
                }
                else
                {
                    UInt64 event_number = 0;
                    const auto * prev_pos = pos;
                    pos = tryReadIntText(event_number, pos, end);
                    if (pos == prev_pos)
                        throw_exception("Could not parse number");

                    if (event_number > arg_count - 1)
                        throw Exception{"Event number " + toString(event_number) + " is out of range", ErrorCodes::BAD_ARGUMENTS};

                    actions.emplace_back(PatternActionType::SpecificEvent, event_number - 1);
                    dfa_states.back().transition = DFATransition::SpecificEvent;
                    dfa_states.back().event = event_number - 1;
                    dfa_states.emplace_back();
                    conditions_in_pattern.set(event_number - 1);
                }

                if (!match(")"))
                    throw_exception("Expected closing parenthesis, found");

            }
            else if (match(".*"))
            {
                actions.emplace_back(PatternActionType::KleeneStar);
                dfa_states.back().has_kleene = true;
            }
            else if (match("."))
            {
                actions.emplace_back(PatternActionType::AnyEvent);
                dfa_states.back().transition = DFATransition::AnyEvent;
                dfa_states.emplace_back();
            }
            else
                throw_exception("Could not parse pattern, unexpected starting symbol");
        }
    }

protected:
    /// Uses a DFA based approach in order to better handle patterns without
    /// time assertions.
    ///
    /// NOTE: This implementation relies on the assumption that the pattern is *small*.
    ///
    /// This algorithm performs in O(mn) (with m the number of DFA states and N the number
    /// of events) with a memory consumption and memory allocations in O(m). It means that
    /// if n >>> m (which is expected to be the case), this algorithm can be considered linear.
    template <typename EventEntry>
    bool dfaMatch(EventEntry & events_it, const EventEntry events_end) const
    {
        using ActiveStates = std::vector<bool>;

        /// Those two vectors keep track of which states should be considered for the current
        /// event as well as the states which should be considered for the next event.
        ActiveStates active_states(dfa_states.size(), false);
        ActiveStates next_active_states(dfa_states.size(), false);
        active_states[0] = true;

        /// Keeps track of dead-ends in order not to iterate over all the events to realize that
        /// the match failed.
        size_t n_active = 1;

        for (/* empty */; events_it != events_end && n_active > 0 && !active_states.back(); ++events_it)
        {
            n_active = 0;
            next_active_states.assign(dfa_states.size(), false);

            for (size_t state = 0; state < dfa_states.size(); ++state)
            {
                if (!active_states[state])
                {
                    continue;
                }

                switch (dfa_states[state].transition)
                {
                    case DFATransition::None:
                        break;
                    case DFATransition::AnyEvent:
                        next_active_states[state + 1] = true;
                        ++n_active;
                        break;
                    case DFATransition::SpecificEvent:
                        if (events_it->second.test(dfa_states[state].event))
                        {
                            next_active_states[state + 1] = true;
                            ++n_active;
                        }
                        break;
                }

                if (dfa_states[state].has_kleene)
                {
                    next_active_states[state] = true;
                    ++n_active;
                }
            }
            swap(active_states, next_active_states);
        }

        return active_states.back();
    }

    template <typename EventEntry>
    bool backtrackingMatch(EventEntry & events_it, const EventEntry events_end) const
    {
        const auto action_begin = std::begin(actions);
        const auto action_end = std::end(actions);
        auto action_it = action_begin;

        const auto events_begin = events_it;
        auto base_it = events_it;

        /// an iterator to action plus an iterator to row in events list plus timestamp at the start of sequence
        using backtrack_info = std::tuple<decltype(action_it), EventEntry, EventEntry>;
        std::stack<backtrack_info> back_stack;

        /// backtrack if possible
        const auto do_backtrack = [&]
        {
            while (!back_stack.empty())
            {
                auto & top = back_stack.top();

                action_it = std::get<0>(top);
                events_it = std::next(std::get<1>(top));
                base_it = std::get<2>(top);

                back_stack.pop();

                if (events_it != events_end)
                    return true;
            }

            return false;
        };

        size_t i = 0;
        while (action_it != action_end && events_it != events_end)
        {
            if (action_it->type == PatternActionType::SpecificEvent)
            {
                if (events_it->second.test(action_it->extra))
                {
                    /// move to the next action and events
                    base_it = events_it;
                    ++action_it, ++events_it;
                }
                else if (!do_backtrack())
                    /// backtracking failed, bail out
                    break;
            }
            else if (action_it->type == PatternActionType::AnyEvent)
            {
                base_it = events_it;
                ++action_it, ++events_it;
            }
            else if (action_it->type == PatternActionType::KleeneStar)
            {
                back_stack.emplace(action_it, events_it, base_it);
                base_it = events_it;
                ++action_it;
            }
            else if (action_it->type == PatternActionType::TimeLessOrEqual)
            {
                if (events_it->first <= base_it->first + action_it->extra)
                {
                    /// condition satisfied, move onto next action
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                }
                else if (!do_backtrack())
                    break;
            }
            else if (action_it->type == PatternActionType::TimeLess)
            {
                if (events_it->first < base_it->first + action_it->extra)
                {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                }
                else if (!do_backtrack())
                    break;
            }
            else if (action_it->type == PatternActionType::TimeGreaterOrEqual)
            {
                if (events_it->first >= base_it->first + action_it->extra)
                {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                }
                else if (++events_it == events_end && !do_backtrack())
                    break;
            }
            else if (action_it->type == PatternActionType::TimeGreater)
            {
                if (events_it->first > base_it->first + action_it->extra)
                {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                }
                else if (++events_it == events_end && !do_backtrack())
                    break;
            }
            else if (action_it->type == PatternActionType::TimeEqual)
            {
                if (events_it->first == base_it->first + action_it->extra)
                {
                    back_stack.emplace(action_it, events_it, base_it);
                    base_it = events_it;
                    ++action_it;
                }
                else if (++events_it == events_end && !do_backtrack())
                    break;
            }
            else
                throw Exception{"Unknown PatternActionType", ErrorCodes::LOGICAL_ERROR};

            if (++i > sequence_match_max_iterations)
                throw Exception{"Pattern application proves too difficult, exceeding max iterations (" + toString(sequence_match_max_iterations) + ")",
                    ErrorCodes::TOO_SLOW};
        }

        /// if there are some actions remaining
        if (action_it != action_end)
        {
            /// match multiple empty strings at end
            while (action_it->type == PatternActionType::KleeneStar ||
                   action_it->type == PatternActionType::TimeLessOrEqual ||
                   action_it->type == PatternActionType::TimeLess ||
                   (action_it->type == PatternActionType::TimeGreaterOrEqual && action_it->extra == 0))
                ++action_it;
        }

        if (events_it == events_begin)
            ++events_it;

        return action_it == action_end;
    }

    /// Splits the pattern into deterministic parts separated by non-deterministic fragments
    /// (time constraints and Kleene stars), and tries to match the deterministic parts in their specified order,
    /// ignoring the non-deterministic fragments.
    /// This function can quickly check that a full match is not possible if some deterministic fragment is missing.
    template <typename EventEntry>
    bool couldMatchDeterministicParts(const EventEntry events_begin, const EventEntry events_end, bool limit_iterations = true) const
    {
        size_t events_processed = 0;
        auto events_it = events_begin;

        const auto actions_end = std::end(actions);
        auto actions_it = std::begin(actions);
        auto det_part_begin = actions_it;

        auto match_deterministic_part = [&events_it, events_end, &events_processed, det_part_begin, actions_it, limit_iterations]()
        {
            auto events_it_init = events_it;
            auto det_part_it = det_part_begin;

            while (det_part_it != actions_it && events_it != events_end)
            {
                /// matching any event
                if (det_part_it->type == PatternActionType::AnyEvent)
                    ++events_it, ++det_part_it;

                /// matching specific event
                else
                {
                    if (events_it->second.test(det_part_it->extra))
                        ++events_it, ++det_part_it;

                    /// abandon current matching, try to match the deterministic fragment further in the list
                    else
                    {
                        events_it = ++events_it_init;
                        det_part_it = det_part_begin;
                    }
                }

                if (limit_iterations && ++events_processed > sequence_match_max_iterations)
                    throw Exception{"Pattern application proves too difficult, exceeding max iterations (" + toString(sequence_match_max_iterations) + ")",
                        ErrorCodes::TOO_SLOW};
            }

            return det_part_it == actions_it;
        };

        for (; actions_it != actions_end; ++actions_it)
            if (actions_it->type != PatternActionType::SpecificEvent && actions_it->type != PatternActionType::AnyEvent)
            {
                if (!match_deterministic_part())
                    return false;
                det_part_begin = std::next(actions_it);
            }

        return match_deterministic_part();
    }

private:
    enum class DFATransition : char
    {
        ///   .-------.
        ///   |       |
        ///   `-------'
        None,
        ///   .-------.  (?[0-9])
        ///   |       | ----------
        ///   `-------'
        SpecificEvent,
        ///   .-------.      .
        ///   |       | ----------
        ///   `-------'
        AnyEvent,
    };

    struct DFAState
    {
        explicit DFAState(bool has_kleene_ = false)
            : has_kleene{has_kleene_}, event{0}, transition{DFATransition::None}
        {}

        ///   .-------.
        ///   |       | - - -
        ///   `-------'
        ///     |_^
        bool has_kleene;
        /// In the case of a state transitions with a `SpecificEvent`,
        /// `event` contains the value of the event.
        uint32_t event;
        /// The kind of transition out of this state.
        DFATransition transition;
    };

    using DFAStates = std::vector<DFAState>;

protected:
    /// `True` if the parsed pattern contains time assertions (?t...), `false` otherwise.
    bool pattern_has_time;
    /// sequenceMatch conditions met at least once in the pattern
    std::bitset<max_events> conditions_in_pattern;

private:
    std::string pattern;
    size_t arg_count;
    PatternActions actions;

    DFAStates dfa_states;
};

template <typename T, typename Data>
class AggregateFunctionSequenceMatch final : public AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceMatch<T, Data>>
{
public:
    AggregateFunctionSequenceMatch(const DataTypes & arguments, const Array & params, const String & pattern_)
        : AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceMatch<T, Data>>(arguments, params, pattern_) {}

    using AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceMatch<T, Data>>::AggregateFunctionSequenceBase;

    String getName() const override { return "sequenceMatch"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt8>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & output = assert_cast<ColumnUInt8 &>(to).getData();
        if ((this->conditions_in_pattern & this->data(place).conditions_met) != this->conditions_in_pattern)
        {
            output.push_back(false);
            return;
        }
        this->data(place).sort();

        const auto & data_ref = this->data(place);

        const auto events_begin = std::begin(data_ref.events_list);
        const auto events_end = std::end(data_ref.events_list);
        auto events_it = events_begin;

        bool match = (this->pattern_has_time ?
            (this->couldMatchDeterministicParts(events_begin, events_end) && this->backtrackingMatch(events_it, events_end)) :
            this->dfaMatch(events_it, events_end));
        output.push_back(match);
    }
};

template <typename T, typename Data>
class AggregateFunctionSequenceCount final : public AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceCount<T, Data>>
{
public:
    AggregateFunctionSequenceCount(const DataTypes & arguments, const Array & params, const String & pattern_)
        : AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceCount<T, Data>>(arguments, params, pattern_) {}

    using AggregateFunctionSequenceBase<T, Data, AggregateFunctionSequenceCount<T, Data>>::AggregateFunctionSequenceBase;

    String getName() const override { return "sequenceCount"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & output = assert_cast<ColumnUInt64 &>(to).getData();
        if ((this->conditions_in_pattern & this->data(place).conditions_met) != this->conditions_in_pattern)
        {
            output.push_back(0);
            return;
        }
        this->data(place).sort();
        output.push_back(count(place));
    }

private:
    UInt64 count(ConstAggregateDataPtr __restrict place) const
    {
        const auto & data_ref = this->data(place);

        const auto events_begin = std::begin(data_ref.events_list);
        const auto events_end = std::end(data_ref.events_list);
        auto events_it = events_begin;

        size_t count = 0;
        // check if there is a chance of matching the sequence at least once
        if (this->couldMatchDeterministicParts(events_begin, events_end))
        {
            while (events_it != events_end && this->backtrackingMatch(events_it, events_end))
                ++count;
        }

        return count;
    }
};

}
