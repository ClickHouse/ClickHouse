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

/// A deterministic byte automaton over complete dictionary keys. No index-format or
/// tokenizer assumptions: regex, wildcard and edit-distance compilers can supply the
/// same transitions. State zero is the initial state; absent transitions reject.
class TextIndexDictionaryAutomaton
{
public:
    using StateID = uint32_t;

    struct Transition
    {
        uint8_t first;
        uint8_t last;
        StateID destination;
    };

    struct State
    {
        bool accepting = false;
        /// Ordered, non-overlapping inclusive byte ranges.
        std::vector<Transition> transitions;
    };

    /// The caller supplies at least one state and valid destination indices.
    /// Removes transitions into states from which no accepting state is reachable.
    explicit TextIndexDictionaryAutomaton(std::vector<State> states_);

    static std::shared_ptr<const TextIndexDictionaryAutomaton> literal(std::string_view value, bool prefix, size_t max_memory = 1 << 20);

    /// Preserves RE2 partial-match semantics, including anchors, UTF-8 and empty-width
    /// assertions. A null result means the optimization is unavailable within the
    /// budget; it must never be treated as an empty matching language.
    static std::shared_ptr<const TextIndexDictionaryAutomaton> fromRegexp(const re2::RE2 & regexp, size_t max_memory = 1 << 20);

    /// Whether a starting ASCII byte rules out every extension. Otherwise a literal
    /// search is usually a better candidate filter than seeking a sorted dictionary.
    bool canSkipPrefixes() const;

    enum class Result : uint8_t
    {
        Match,
        Seek,
        Exhausted,
    };

    /// Query-local scratch; the automaton itself is immutable and shareable across parts.
    class Cursor
    {
    public:
        explicit Cursor(const TextIndexDictionaryAutomaton & automaton_) : automaton(automaton_) {}

        /// Match: `key` is accepted. Seek: no accepted key lies in [key, next),
        /// and next > key in unsigned byte order. Exhausted: no key >= key matches.
        /// A seek target need only be a prefix of an accepted key: cyclic languages
        /// need not have a lexicographically smallest accepted extension.
        Result next(std::string_view key, std::string & seek_target);

    private:
        const TextIndexDictionaryAutomaton & automaton;
        std::vector<StateID> path;
        std::string previous_key;
    };

private:
    static constexpr StateID dead = UINT32_MAX;
    StateID step(StateID state, uint8_t byte) const;
    const Transition * nextTransition(StateID state, unsigned byte) const;
    std::vector<State> states;
};

}
