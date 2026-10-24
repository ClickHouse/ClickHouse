#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace re2
{
class RE2;
}

namespace DB
{

/// A deterministic finite automaton (DFA, https://en.wikipedia.org/wiki/Deterministic_finite_automaton) over bytes.
/// It is used to find the tokens of a sorted text index dictionary which match a pattern, e.g. `LIKE 'service%error'`,
/// without testing every token of the dictionary.
///
/// The DFA reads one token byte by byte. It starts in state 0 and, for every byte, follows the transition whose byte
/// range contains that byte. The token matches if the DFA is in an accepting state after reading the last byte of the
/// token. If the current state has no transition for the next byte, the token does not match (there is no explicit
/// "dead" state). The DFA always decides about a whole token, not about a substring of it.
///
/// The class knows nothing about the text index format or the tokenizer, it only consists of states and transitions.
/// They are currently built by `literal` (an exact token or a token prefix) and `fromRegexp` (e.g. a `LIKE` pattern
/// translated into a regular expression). Other matchers, e.g. an edit-distance (Levenshtein) automaton for fuzzy
/// search, can build their own `State`s and pass them to the constructor.
///
/// Example: `LIKE 'ab%c'` is the regular expression `^ab.*c$`. Treating `.` as "any byte" for brevity (`fromRegexp`
/// matches whole UTF-8 characters and therefore has a few more states), the DFA is:
///
///     state 0:             'a' -> 1
///     state 1:             'b' -> 2
///     state 2:             [0x00, 'b'] -> 2,  'c' -> 3,  ['d', 0xFF] -> 2
///     state 3, accepting:  [0x00, 'b'] -> 2,  'c' -> 3,  ['d', 0xFF] -> 2
///
///   - `abxc` matches:         0 -a-> 1 -b-> 2 -x-> 2 -c-> 3, and state 3 is accepting.
///   - `abcd` does not match:  it ends in state 2, which is not accepting.
///   - `ac` does not match:    state 1 has no transition for 'c'.
///
/// Why a DFA: the dictionary is sorted, so for a token which does not match, the DFA can also tell the smallest string
/// the next matching token can start with. The caller then jumps there with a binary search instead of testing all
/// tokens in between (see `Cursor::next`). For the dictionary `aa, aab, aac, abc, abcd, abx, ac, b, ca` and the DFA
/// above:
///
///   - `aa`:    rejected at the 2nd byte. The smallest byte > 'a' with a transition from state 1 is 'b', so the next
///              match cannot be smaller than `ab` -> seek to `ab`. This skips `aab` and `aac`.
///   - `abc`:   matches.
///   - `abcd`:  ends in the non-accepting state 2 -> seek to `abcd\0`, i.e. simply continue with the next token `abx`.
///   - `abx`:   same, continue with `ac`.
///   - `ac`:    rejected at the 2nd byte. State 1 has no transition for a byte > 'c' and state 0 has none for a byte
///              > 'a', so no token after `ac` can match -> stop. `b` and `ca` are never read.
///
/// `MergeTreeIndexGranuleText::analyzeDictionaryForPatterns` also tests the first token of every dictionary block
/// (from the sparse index) this way, so dictionary blocks which cannot contain a matching token are not read at all.
class TextIndexDictionaryDFA
{
public:
    using StateID = uint32_t;

    /// Moves the DFA to state `destination` on every byte `b` with `first <= b <= last` (both bounds inclusive).
    /// Example: {'a', 'z', 5} moves to state 5 on any lower-case ASCII letter, {'x', 'x', 5} only on 'x'.
    struct Transition
    {
        uint8_t first;
        uint8_t last;
        StateID destination;
    };

    struct State
    {
        /// A token matches if the DFA is in an accepting state after reading all bytes of the token.
        bool accepting = false;
        /// Sorted by byte range: the `first` byte of each transition is greater than the `last` byte of the previous
        /// one. So the ranges don't overlap, and the transition for a byte can be found with a binary search.
        /// Bytes which are not covered by any range have no transition.
        std::vector<Transition> transitions;
    };

    /// `states_` - all states of the DFA, indexed by `StateID`. State 0 is the start state, so at least one state is
    ///             required. Every `Transition::destination` must be a valid index into `states_`, and the transitions
    ///             of every state must be sorted as described in `State::transitions`.
    /// Removes all transitions into states from which no accepting state can be reached. Such states can never lead
    /// to a match, and removing them guarantees that every seek target of `Cursor::next` is a prefix of a matching token.
    explicit TextIndexDictionaryDFA(std::vector<State> states_);

    /// Builds a DFA which accepts exactly the token `value` (`prefix` = false), or every token which starts with `value`
    /// (`prefix` = true, like `LIKE 'value%'`). The bytes of `value` are compared as is, without case folding.
    /// `max_memory` - upper bound (in bytes) for the memory of the states and transitions of the DFA.
    /// Returns null if the DFA would exceed `max_memory`.
    /// Example: `literal("ab", true)` builds 0 -a-> 1 -b-> 2, where state 2 is accepting and moves to itself on any byte.
    static std::shared_ptr<const TextIndexDictionaryDFA> literal(std::string_view value, bool prefix, size_t max_memory = 1 << 20);

    /// Builds a DFA which accepts exactly the tokens `t` for which `RE2::PartialMatch(t, regexp)` is true. That is,
    /// `regexp` may match anywhere inside the token unless it is anchored with `^` or `$`, exactly like in RE2. The
    /// options of `regexp` are preserved: e.g. UTF-8 or Latin-1 encoding, case-insensitive matching, and empty-width
    /// assertions such as `\b`, `^` and `$` (which see the real neighbouring bytes of the token).
    /// Example: `LIKE 'service%error'` is translated into `^service.*error$`. The DFA accepts `serviceXerror` and rejects
    /// `servicezok` and `myserviceerror`. Without the anchors, `service.*error` also accepts `myserviceerror`.
    /// `regexp` - a compiled regular expression.
    /// `max_memory` - upper bound (in bytes) for the memory used by RE2 to build its DFA, and separately for the memory
    ///                of the returned states and transitions.
    /// Returns null if `regexp` is invalid or its DFA does not fit into `max_memory`. For example, `^[ab]*a[ab]{15}$`
    /// (the 16th byte from the end is 'a') needs 2^16 DFA states. Null means "no DFA is available, use the regular
    /// dictionary scan". It must never be treated as "no token matches".
    static std::shared_ptr<const TextIndexDictionaryDFA> fromRegexp(const re2::RE2 & regexp, size_t max_memory = 1 << 20);

    /// Returns true if some ASCII byte cannot be the first byte of a matching token, i.e. the DFA rejects all tokens
    /// starting with that byte at once. E.g. for `LIKE 'service%error'`, every token which does not start with 's' is
    /// rejected at its first byte, and seeking skips everything before and after the tokens starting with `service`.
    /// If every ASCII byte can start a matching token, as for `LIKE '_error'` (`^.error$`), the matching tokens can be
    /// anywhere in the dictionary, seeking still reads dictionary blocks everywhere, and the existing substring filter
    /// (here: for `error`) is usually cheaper.
    /// Non-ASCII bytes are ignored: in UTF-8 mode, `^.error$` rejects the bytes which cannot start a UTF-8 character
    /// (e.g. 0x80), but dictionary tokens hardly ever start with them, so this would not skip anything.
    bool canSkipPrefixes() const;

    /// Intersects the DFA with a sorted sequence of tokens, i.e. a dictionary. The DFA itself is immutable: it is built
    /// once per query and shared by the dictionary scans of all parts. Every scan uses its own `Cursor`, which remembers
    /// the states visited for the previous token. Adjacent dictionary tokens usually share a long prefix, and the states
    /// for the shared prefix are reused instead of being computed again.
    class Cursor
    {
    public:
        enum class Result : uint8_t
        {
            /// The DFA accepts `token`.
            Match,
            /// The DFA rejects `token`, and there is no matching token in [`token`, `lower_bound`).
            Seek,
            /// The DFA rejects `token` and all tokens greater than `token`.
            Exhausted,
        };

        explicit Cursor(const TextIndexDictionaryDFA & automaton_) : automaton(automaton_) {}

        /// Tests the dictionary token `token`. Strings are compared as unsigned bytes, in the order of the dictionary.
        /// Returns:
        ///   - `Match`: the caller processes `token` and continues with the next dictionary token.
        ///   - `Seek`: `lower_bound` is set to a string greater than `token`, such that every matching token greater
        ///     than `token` is >= `lower_bound`. The caller continues with the first dictionary token >= `lower_bound`
        ///     and skips all tokens in between.
        ///   - `Exhausted`: no dictionary token >= `token` matches, the caller can stop.
        ///
        /// `lower_bound` is a prefix of a matching token, but it is not necessarily a matching token itself. Returning
        /// the smallest matching token greater than `token` instead is not always possible, because it may not exist.
        /// For example, for `^a*b$` and `token` = `a`, the matching tokens greater than `a` are `ab` > `aab` > `aaab` > ...,
        /// and none of them is the smallest. So the cursor returns `lower_bound` = `aa`: the caller seeks to the first
        /// dictionary token >= `aa` and calls `next` again.
        ///
        /// The result depends only on `token`. The previously tested tokens are only used to compute it faster.
        Result next(std::string_view token, std::string & lower_bound);

    private:
        const TextIndexDictionaryDFA & automaton;
        /// `path[i]` is the state after reading the first `i` bytes of `previous_token`. It ends where the previous
        /// token was rejected, so `path.size() - 1` is the number of bytes of `previous_token` which the DFA has read.
        std::vector<StateID> path;
        std::string previous_token;
    };

private:
    /// Returned by `step` if the state has no transition for the byte.
    static constexpr StateID dead = UINT32_MAX;
    StateID step(StateID state, uint8_t byte) const;
    const Transition * nextTransition(StateID state, unsigned byte) const;
    /// Indexed by `StateID`, state 0 is the start state.
    std::vector<State> states;
};

}
