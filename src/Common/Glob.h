#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <string>
#include <string_view>


namespace DB
{

/// A path pattern with globs, compiled for matching without a regular expression.
///
/// `Glob(pattern).matches(candidate)` gives the same answer as
/// `re2::RE2::FullMatch(candidate, makeRegexpPatternFromGlobs(pattern))`, bug for bug, for every pattern
/// that function turns into a regexp `re2` compiles. The language is therefore defined by what that
/// function does, and not by what a glob is expected to mean. It only covers matching: which paths a
/// pattern expands to (`expandSelectionGlob`) is a different language, and this class does not speak it.
///
/// Groups. A group candidate is a '{', a body without '{' and '}', and a '}'. Candidates are taken from
/// left to right. A candidate is:
/// - a range, if its body is `N..M` with decimal `N` and `M`;
/// - an enum, if its body is not empty, neither starts nor ends with ',', and either has two code points
///   or more, or is one of the single characters `[ ] | + - ( ) \`. Its alternatives are the body split
///   at every ','; the ones in the middle may be empty: `{a,,b}`;
/// - otherwise not a group, and its braces are literal characters, like every other brace: `{a}`, `{}`,
///   `{,a}`, `{{a,b}}` (literal braces around an enum).
///
/// A range `{N..M}` matches every number between `N` and `M`, in either order, written the way it is in
/// the pattern: padded with zeros when the smaller endpoint starts with '0', to the width of the wider
/// endpoint - unless that smaller endpoint is a single '0', as in `{0..10}`, which is not padded.
///
/// Everywhere else, including inside the alternatives of an enum:
/// - `?` matches one character other than '/';
/// - `*` matches any number of characters other than '/';
/// - `*` right after another `*`, with nothing or only `?` in between, matches any number of characters
///   other than '{' and '}', '/' included: `data/**`, `a*?*b`;
/// - `**` followed by '/', at the beginning of the pattern or right after a '/', matches any number of
///   whole directories, none included. It is not recognized at the beginning of an alternative, nor
///   right after a group;
/// - `^` matches nothing and requires the candidate to begin there, and `$` to end there: they reach
///   the regexp as they are. So `^a` matches `a`, and `a$b` matches nothing;
/// - every other character matches itself.
///
/// A character is a code point in UTF-8, as lenient as `re2` is about it: a byte below 0x80, or a lead
/// byte 0xC2..0xDF, 0xE0..0xEF, 0xF0..0xF4 followed by one, two, three bytes 0x80..0xBF. Bytes of the
/// candidate that do not form one are matched by nothing but themselves.
///
/// The patterns there is no regexp for are matched all the same:
/// - a range of any size, where the regexp is refused above 100000 values, or above 8 MiB of it;
/// - a pattern that is not valid UTF-8, byte by byte, where `re2` does not compile the regexp.
///
/// And one kind of pattern is refused here, although there is a regexp for it: a range with a bound
/// that does not fit into 64 bits. The regexp is built from such a bound wrapped around.
class Glob
{
public:
    /// Throws `BAD_ARGUMENTS` for a pattern that is too complex, see `MAX_PROGRAM_SIZE`.
    explicit Glob(std::string pattern_);

    /// Whether the whole `candidate` matches the pattern. Keeps no state between calls,
    /// so one object can be used from many threads.
    [[nodiscard]] bool matches(std::string_view candidate) const;

    [[nodiscard]] const std::string & getPattern() const { return pattern; }

    /// The number of instructions a pattern may compile to. A literal run, a wildcard, a range and an
    /// enum of literal alternatives take one instruction each whatever their length, so only a pattern
    /// with tens of thousands of wildcards, or of alternatives containing them, gets there. Matching
    /// takes time and memory proportional to the number of instructions times the length of the
    /// candidate in the worst case, and that is what this bounds.
    static constexpr size_t MAX_PROGRAM_SIZE = 65536;

private:
    enum class OpType : UInt8
    {
        Literal,        /// The bytes `pattern[first, first + second)`.
        AnyOne,         /// `?`
        Star,           /// `*`
        StarNoBrace,    /// `*` right after a `*`
        Globstar,       /// `**/` as a whole path component
        AssertBegin,    /// `^`
        AssertEnd,      /// `$`
        Range,          /// `ranges[first]`
        AltLiteral,     /// One of the literal alternatives in `literal_groups[first, first + second)`.
        Alt,            /// Continue at every one of `branch_targets[first, first + second)`.
        Jump,           /// Continue at the instruction `first`.
        Match,          /// The end of the pattern.
    };

    struct Op
    {
        OpType type = OpType::Match;
        size_t first = 0;
        size_t second = 0;
    };

    struct Range
    {
        UInt64 min = 0;
        UInt64 max = 0;
        /// The number of digits every value is padded to with zeros; zero if values are not padded.
        size_t width = 0;
    };

    /// A literal alternative of an enum: the bytes `pattern[offset, offset + size)`.
    struct LiteralAlternative
    {
        size_t offset = 0;
        size_t size = 0;
    };

    /// The literal alternatives of one enum that have the same length: `literal_alternatives[first, first + count)`,
    /// sorted, so that the one equal to a piece of the candidate is found by binary search.
    struct LiteralGroup
    {
        size_t size = 0;
        size_t first = 0;
        size_t count = 0;
    };

    void compile();
    /// Compiles `pattern[begin, end)`: the text between two groups, or an alternative of an enum.
    void compileText(size_t begin, size_t end);
    /// The body of a group candidate is `pattern[begin, end)`.
    bool tryParseRange(size_t begin, size_t end, Range & range) const;
    [[nodiscard]] bool isEnum(size_t begin, size_t end) const;
    void compileEnum(size_t begin, size_t end);
    void compileLiteralAlternatives(VectorWithMemoryTracking<LiteralAlternative> & alternatives);
    void addOp(OpType type, size_t first = 0, size_t second = 0);

    std::string pattern;

    /// Everything below refers to `pattern` by offset, so the object can be copied and moved.
    VectorWithMemoryTracking<Op> program;
    VectorWithMemoryTracking<Range> ranges;
    VectorWithMemoryTracking<LiteralAlternative> literal_alternatives;
    VectorWithMemoryTracking<LiteralGroup> literal_groups;
    VectorWithMemoryTracking<size_t> branch_targets;
};

}
