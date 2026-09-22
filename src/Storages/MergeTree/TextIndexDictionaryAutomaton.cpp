#include <Storages/MergeTree/TextIndexDictionaryAutomaton.h>

#include <Common/re2.h>
#include <base/defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <re2/prog.h>
#pragma clang diagnostic pop

#include <algorithm>
#include <limits>

namespace DB
{

TextIndexDictionaryAutomaton::TextIndexDictionaryAutomaton(std::vector<State> states_) : states(std::move(states_))
{
    chassert(!states.empty());
    std::vector<std::vector<StateID>> predecessors(states.size());
    std::vector<bool> live(states.size(), false);
    std::vector<StateID> pending;
    for (size_t i = 0; i < states.size(); ++i)
    {
        if (states[i].accepting)
        {
            live[i] = true;
            pending.push_back(static_cast<StateID>(i));
        }
        for (const auto & transition : states[i].transitions)
        {
            chassert(transition.destination < states.size());
            predecessors[transition.destination].push_back(static_cast<StateID>(i));
        }
    }
    while (!pending.empty())
    {
        StateID state = pending.back();
        pending.pop_back();
        for (StateID previous : predecessors[state])
        {
            if (!live[previous])
            {
                live[previous] = true;
                pending.push_back(previous);
            }
        }
    }
    for (auto & state : states)
        std::erase_if(state.transitions, [&](const auto & transition) { return !live[transition.destination]; });
}

std::shared_ptr<const TextIndexDictionaryAutomaton> TextIndexDictionaryAutomaton::literal(std::string_view value, bool prefix, size_t max_memory)
{
    if (max_memory < sizeof(State) + sizeof(Transition)
        || value.size() >= max_memory / (sizeof(State) + sizeof(Transition)))
        return {};
    std::vector<State> result(value.size() + 1);
    for (size_t i = 0; i < value.size(); ++i)
    {
        auto byte = static_cast<uint8_t>(value[i]);
        result[i].transitions.push_back({byte, byte, static_cast<StateID>(i + 1)});
    }
    result.back().accepting = true;
    if (prefix)
        result.back().transitions.push_back({0, 255, static_cast<StateID>(value.size())});
    return std::make_shared<TextIndexDictionaryAutomaton>(std::move(result));
}

std::shared_ptr<const TextIndexDictionaryAutomaton> TextIndexDictionaryAutomaton::fromRegexp(const re2::RE2 & regexp, size_t max_memory)
{
    if (!regexp.ok() || max_memory < sizeof(State) || max_memory > static_cast<size_t>(std::numeric_limits<int64_t>::max()))
        return {};

    /// Anchor the entire language, with arbitrary bytes on either side of the original
    /// expression. Reuse its parsed tree to preserve encoding and parse options. In
    /// particular, the outer byte wildcards must not change the context seen by \b/^/$.
    re2::Regexp * parts[] = {
        re2::Regexp::Parse("\\A\\C*", re2::Regexp::LikePerl, nullptr),
        regexp.Regexp()->Incref(),
        re2::Regexp::Parse("\\C*\\z", re2::Regexp::LikePerl, nullptr)};
    auto decrement = [](re2::Regexp * expression) { expression->Decref(); };
    std::unique_ptr<re2::Regexp, decltype(decrement)> expression(re2::Regexp::Concat(parts, 3, regexp.Regexp()->parse_flags()), decrement);
    std::unique_ptr<re2::Prog> program(expression->CompileToProg(static_cast<int64_t>(max_memory)));
    if (!program)
        return {};

    std::vector<State> result;
    std::vector<int> end_states;
    std::vector<bool> matching;
    bool complete = true;
    size_t bytes = 0;
    program->BuildEntireDFA(re2::Prog::kLongestMatch, [&](const int * next, bool match)
    {
        if (!next)
        {
            complete = false;
            return;
        }
        /// RE2 bounds its own DFA. Bound the exported representation separately,
        /// including its temporary end-of-text and accepting-state tables.
        bytes += sizeof(State) + sizeof(int) + sizeof(bool);
        if (!complete || bytes > max_memory)
        {
            complete = false;
            return;
        }
        State state;
        for (unsigned byte = 0; byte < 256; ++byte)
        {
            int destination = next[program->bytemap()[byte]];
            if (destination < 0)
                continue;
            if (!state.transitions.empty() && state.transitions.back().destination == static_cast<StateID>(destination)
                && state.transitions.back().last + 1 == byte)
                state.transitions.back().last = static_cast<uint8_t>(byte);
            else
            {
                bytes += sizeof(Transition);
                if (bytes > max_memory)
                {
                    complete = false;
                    return;
                }
                state.transitions.push_back({static_cast<uint8_t>(byte), static_cast<uint8_t>(byte), static_cast<StateID>(destination)});
            }
        }
        result.push_back(std::move(state));
        end_states.push_back(next[program->bytemap_range()]);
        matching.push_back(match);
    });
    if (!complete || result.empty())
        return {};

    /// RE2 reports a match after consuming its end-of-text symbol. Dictionary
    /// keys have no sentinel byte: fold that transition into the accepting flag.
    for (size_t i = 0; i < result.size(); ++i)
        result[i].accepting = end_states[i] >= 0 && matching[end_states[i]];
    return std::make_shared<TextIndexDictionaryAutomaton>(std::move(result));
}

const TextIndexDictionaryAutomaton::Transition * TextIndexDictionaryAutomaton::nextTransition(StateID state, unsigned byte) const
{
    const auto & transitions = states[state].transitions;
    auto it = std::lower_bound(transitions.begin(), transitions.end(), byte,
        [](const auto & transition, unsigned value) { return transition.last < value; });
    return it == transitions.end() ? nullptr : &*it;
}

TextIndexDictionaryAutomaton::StateID TextIndexDictionaryAutomaton::step(StateID state, uint8_t byte) const
{
    const auto * transition = nextTransition(state, byte);
    return transition && transition->first <= byte ? transition->destination : dead;
}

bool TextIndexDictionaryAutomaton::canSkipPrefixes() const
{
    for (unsigned byte = 0; byte < 128; ++byte)
        if (step(0, static_cast<uint8_t>(byte)) == dead)
            return true;
    return false;
}

TextIndexDictionaryAutomaton::Result TextIndexDictionaryAutomaton::Cursor::next(std::string_view key, std::string & seek_target)
{
    if (path.empty())
        path.push_back(0);
    /// Adjacent dictionary keys share long prefixes. Reuse their states, including
    /// across block boundaries, but never reuse a byte after a rejected transition.
    size_t position = 0;
    const size_t common_limit = std::min({key.size(), previous_key.size(), path.size() - 1});
    while (position < common_limit && key[position] == previous_key[position])
        ++position;
    path.resize(position + 1);
    previous_key.assign(key);
    for (; position < key.size(); ++position)
    {
        StateID next_state = automaton.step(path.back(), static_cast<uint8_t>(key[position]));
        if (next_state == dead)
            break;
        path.push_back(next_state);
    }
    if (position == key.size())
    {
        if (automaton.states[path.back()].accepting)
            return Result::Match;
        if (const auto * transition = automaton.nextTransition(path.back(), 0))
        {
            seek_target.assign(key);
            seek_target.push_back(static_cast<char>(transition->first));
            return Result::Seek;
        }
    }
    /// Backtrack to the first larger live edge, skipping the entire rejected
    /// subtree. Byte 0xff has no successor; carry into the previous byte.
    while (true)
    {
        if (position < key.size())
        {
            unsigned next_byte = static_cast<uint8_t>(key[position]) + 1U;
            if (const auto * transition = automaton.nextTransition(path[position], next_byte))
            {
                seek_target.assign(key.substr(0, position));
                seek_target.push_back(static_cast<char>(std::max<unsigned>(next_byte, transition->first)));
                return Result::Seek;
            }
        }
        if (position == 0)
            return Result::Exhausted;
        --position;
    }
}

}
