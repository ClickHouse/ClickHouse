#include <Storages/MergeTree/TextIndexDictionaryDFA.h>

#include <Common/re2.h>
#include <base/defines.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wgnu-anonymous-struct"
#pragma clang diagnostic ignored "-Wnested-anon-types"
#pragma clang diagnostic ignored "-Wnullability-extension"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#include <re2/prog.h>
#pragma clang diagnostic pop

#include <algorithm>
#include <limits>

namespace DB
{

/// Removes all transitions into "dead-end" states, i.e. states from which no accepting state can be reached.
///
/// Why: `Cursor::next` computes seek targets by following transitions. A transition into a dead end would make it
/// return seek targets which are not a prefix of any matching token, and it could not return `Exhausted` while such
/// transitions remain. E.g. take the DFA for `a*b` plus a non-accepting state which is entered on 'c' and moves to
/// itself on every byte: without pruning, the tokens `c`, `cc`, `ccc`, ... are all tested one by one; with pruning,
/// the cursor stops at `c`.
///
/// How: first find the "live" states (from which an accepting state is reachable) by walking the transitions
/// backwards, starting from the accepting states. Then delete every transition whose destination is not live.
TextIndexDictionaryDFA::TextIndexDictionaryDFA(std::vector<State> states_) : states(std::move(states_))
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
    /// A state is live if it has a transition into a live state.
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

/// Builds a chain of `value.size() + 1` states: state `i` moves to state `i + 1` on the byte `value[i]`, and the last
/// state is accepting. For a prefix, the last state also moves to itself on every byte, so any continuation of `value`
/// is accepted. The memory estimate is one `State` and one `Transition` per byte of `value`.
std::shared_ptr<const TextIndexDictionaryDFA> TextIndexDictionaryDFA::literal(std::string_view value, bool prefix, size_t max_memory)
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
    return std::make_shared<TextIndexDictionaryDFA>(std::move(result));
}

/// Lets RE2 build the complete DFA of a slightly modified `regexp`, then converts it into `State`s.
///
/// Why RE2: it already implements the regexp syntax, UTF-8 decoding, case folding and the construction of a DFA
/// within a memory limit. Reusing it guarantees that the DFA accepts exactly the tokens `RE2::PartialMatch` accepts.
///
/// How:
/// 1. RE2 searches for a match anywhere in the text, whereas our DFA decides about a whole token. So the expression
///    is wrapped into `\A\C*` ... `\C*\z` (`\C` is any byte): a token is accepted if it consists of arbitrary bytes,
///    a match of `regexp`, and arbitrary bytes.
/// 2. `BuildEntireDFA` calls the callback once per DFA state, in the order of the state numbers. For every class of
///    bytes (RE2 groups bytes which behave the same, see `bytemap`), the callback gets the number of the next state,
///    or -1 if no match is possible anymore. One more entry holds the next state at the end of the text.
/// 3. The 256 next states of every state are converted into sorted byte ranges: adjacent bytes with the same next
///    state are merged into one `Transition`, and bytes with -1 get no transition.
/// 4. Accepting states are derived from the end-of-text entries (see the comment at the end).
std::shared_ptr<const TextIndexDictionaryDFA> TextIndexDictionaryDFA::fromRegexp(const re2::RE2 & regexp, size_t max_memory)
{
    /// `CompileToProg` takes the memory budget as `int64_t`.
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
    /// For every state: the next state at the end of the text (-1 if none), and whether RE2 marked the state as matching.
    std::vector<int> end_states;
    std::vector<bool> matching;
    bool complete = true;
    size_t bytes = 0;
    program->BuildEntireDFA(re2::Prog::kLongestMatch, [&](const int * next, bool match)
    {
        /// RE2 has exhausted its memory budget and stops building the DFA.
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
            /// Extend the previous range if this byte directly follows it and leads to the same state.
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

    /// RE2 marks a DFA state as matching one symbol late: whether a text matches is known only in the state reached
    /// after the end-of-text symbol (this lets assertions like `$` and `\b` look at the following symbol). Dictionary
    /// tokens have no end-of-text byte, so a state is accepting if its end-of-text transition leads to a matching state.
    for (size_t i = 0; i < result.size(); ++i)
        result[i].accepting = end_states[i] >= 0 && matching[end_states[i]];
    return std::make_shared<TextIndexDictionaryDFA>(std::move(result));
}

/// Returns the first transition of `state` whose range ends at or after `byte`: the transition containing `byte` if
/// there is one, otherwise the nearest transition above `byte`, or null if there is none. So it answers both "where does
/// `byte` lead" (see `step`) and "which is the smallest byte >= `byte` that still leads somewhere" (see `Cursor::next`).
/// `byte` is `unsigned` so that `Cursor::next` can pass 256, the successor of 0xFF, for which the result is always null.
const TextIndexDictionaryDFA::Transition * TextIndexDictionaryDFA::nextTransition(StateID state, unsigned byte) const
{
    const auto & transitions = states[state].transitions;
    auto it = std::lower_bound(transitions.begin(), transitions.end(), byte,
        [](const auto & transition, unsigned value) { return transition.last < value; });
    return it == transitions.end() ? nullptr : &*it;
}

/// Returns the state reached from `state` by reading `byte`, or `dead` if `state` has no transition for `byte`.
TextIndexDictionaryDFA::StateID TextIndexDictionaryDFA::step(StateID state, uint8_t byte) const
{
    const auto * transition = nextTransition(state, byte);
    return transition && transition->first <= byte ? transition->destination : dead;
}

/// A token starting with `byte` is rejected at once if the start state has no transition for `byte`.
/// See the header for why only ASCII bytes are checked.
bool TextIndexDictionaryDFA::canSkipPrefixes() const
{
    for (unsigned byte = 0; byte < 128; ++byte)
        if (step(0, static_cast<uint8_t>(byte)) == dead)
            return true;
    return false;
}

/// Reads `token` with the DFA. If `token` is rejected, computes the smallest string greater than `token` which is still
/// a prefix of a matching token. This works like incrementing a number: keep the longest prefix of `token` which can be
/// continued with a larger byte than in `token`, and append the smallest such byte.
///
/// Why the smallest such string: the caller skips all dictionary tokens between `token` and `lower_bound`. A smaller
/// `lower_bound` would skip fewer tokens, a larger one could skip a matching token.
///
/// 1. Reuse the states for the prefix which `token` shares with the previous token, then read the remaining bytes.
/// 2. If all bytes were read and the state is accepting -> `Match`. If it is not accepting but has transitions, the
///    smallest continuation which can still match is `token` + the smallest byte with a transition -> `Seek`.
/// 3. Otherwise, either some byte `token[position]` has no transition, or all bytes were read and the state has no
///    transitions. No token starting with `token[0, position]` can match. Go back byte by byte: at each position, look
///    for a transition on a byte greater than `token[position]`. The first one found gives
///    `lower_bound` = `token[0, position)` + that byte. If there is none even at position 0 -> `Exhausted`.
TextIndexDictionaryDFA::Cursor::Result TextIndexDictionaryDFA::Cursor::next(std::string_view token, std::string & lower_bound)
{
    if (path.empty())
        path.push_back(0);
    /// Adjacent dictionary tokens share long prefixes. Reuse their states, including
    /// across block boundaries, but never reuse a byte after a rejected transition.
    size_t position = 0;
    const size_t common_limit = std::min({token.size(), previous_token.size(), path.size() - 1});
    while (position < common_limit && token[position] == previous_token[position])
        ++position;
    path.resize(position + 1);
    previous_token.assign(token);
    for (; position < token.size(); ++position)
    {
        StateID next_state = automaton.step(path.back(), static_cast<uint8_t>(token[position]));
        if (next_state == dead)
            break;
        path.push_back(next_state);
    }
    if (position == token.size())
    {
        if (automaton.states[path.back()].accepting)
            return Result::Match;
        /// The smallest transition, i.e. the smallest byte which continues `token` towards a match.
        if (const auto * transition = automaton.nextTransition(path.back(), 0))
        {
            lower_bound.assign(token);
            lower_bound.push_back(static_cast<char>(transition->first));
            return Result::Seek;
        }
    }
    /// Backtrack to the first larger live edge, skipping the entire rejected subtree. Byte 0xFF has no larger byte:
    /// `next_byte` = 256 finds no transition, so continue with the previous byte, like a carry when incrementing a number.
    while (true)
    {
        if (position < token.size())
        {
            unsigned next_byte = static_cast<uint8_t>(token[position]) + 1U;
            if (const auto * transition = automaton.nextTransition(path[position], next_byte))
            {
                /// The found range either contains `next_byte` or starts above it.
                lower_bound.assign(token.substr(0, position));
                lower_bound.push_back(static_cast<char>(std::max<unsigned>(next_byte, transition->first)));
                return Result::Seek;
            }
        }
        if (position == 0)
            return Result::Exhausted;
        --position;
    }
}

}
