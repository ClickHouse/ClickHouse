#pragma once

#include <Common/ByteSetLookup.h>
#include <base/defines.h>
#include <base/types.h>

#include <memory>
#include <string_view>
#include <vector>

namespace re2
{
class RE2;
}

namespace DB
{

/// The masks of the `regexpMasked` tokenizer: regular expressions for values such as an IP address, a UUID or a
/// timestamp, whose matches become single tokens. `matchAt` returns the length of the longest match of any mask
/// anchored at a position.
///
/// There are two groups of masks:
///   - built-in masks, selected by name (`getBuiltinMaskNames`). A name may stand for several patterns.
///     They are ASCII-only (compiled in Latin-1 mode), so they fold the case of ASCII letters only.
///   - custom masks, an array of RE2 patterns, matched case-insensitively in UTF-8 mode. They are compiled into one
///     alternation, so that one DFA pass finds the longest match over all of them.
///
/// The object is immutable and can be shared between threads.
class TokenMasks
{
public:
    TokenMasks(const std::vector<String> & builtin_names_, const std::vector<String> & custom_patterns_);
    ~TokenMasks();

    /// The names of all built-in masks, in canonical order.
    static const std::vector<String> & getBuiltinMaskNames();

    /// The selected built-in masks, deduplicated, in canonical order.
    [[nodiscard]] const std::vector<String> & getBuiltinNames() const { return builtin_names; }
    /// The custom patterns, sorted and deduplicated.
    [[nodiscard]] const std::vector<String> & getCustomPatterns() const { return custom_patterns; }

    [[nodiscard]] bool empty() const { return builtin_patterns.empty() && !custom; }

    /// False if no mask can match at a position holding byte `c`.
    [[nodiscard]] ALWAYS_INLINE bool mayStartWith(char c) const { return first_bytes.contains(c); }

    /// The width of `builtin_candidates`.
    static constexpr size_t MAX_BUILTIN_PATTERNS = 16;

    /// The state of a left-to-right scan of one input. For each built-in pattern, the position before which it is known
    /// not to match: a failed match can show that every later start up to some position fails the same way, as for the
    /// starts within the first segment of a would-be path. Skipping them keeps a scan linear.
    struct ScanState
    {
        const char * no_match_until[MAX_BUILTIN_PATTERNS] = {};
    };

    /// Returns the length of the longest match of any mask that starts at `data`, or 0 if there is none.
    /// The calls of one scan pass the same `state` and increasing `data`.
    size_t matchAt(const char * data, size_t length, ScanState & state) const;

    static constexpr size_t MAX_CUSTOM_MASKS = 64;
    static constexpr size_t MAX_CUSTOM_MASK_SIZE = 1024;

    /// The definition of a built-in pattern, for tests.
    struct BuiltinPatternSpec
    {
        std::string_view mask;
        std::string_view pattern;
        /// The match is accepted only if it contains a decimal digit.
        bool needs_digit;
    };

    static std::vector<BuiltinPatternSpec> getBuiltinPatternSpecs();

    /// The bytes a match of `pattern` can start with: a superset computed from the parse tree.
    /// Throws `BAD_ARGUMENTS` if `pattern` contains an anchor or a word-boundary assertion.
    /// Exposed for tests.
    static ByteSetLookup getFirstBytes(const re2::RE2 & pattern);

    struct BuiltinPattern;

private:
    std::vector<String> builtin_names;
    std::vector<String> custom_patterns;

    /// The patterns of the selected built-in masks. They are compiled once per process.
    std::vector<const BuiltinPattern *> builtin_patterns;
    /// For each byte, bit `i` is set if `builtin_patterns[i]` can match at a position holding it.
    UInt16 builtin_candidates[256] = {};

    /// The alternation of all custom patterns, or nullptr.
    std::unique_ptr<re2::RE2> custom;
    ByteSetLookup custom_first_bytes;

    /// The union of the first bytes of all masks.
    ByteSetLookup first_bytes;
};

}
