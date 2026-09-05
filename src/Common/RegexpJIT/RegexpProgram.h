#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>


/** Intermediate representation of a regular expression that belongs to a "simple" subset,
  * suitable for compilation into a native, branch-light, SIMD-friendly matcher (see `CompileRegexp.h`).
  *
  * The representation is intentionally tiny and free of any LLVM dependency, so it can live in
  * `Common` and be unit-tested in isolation. `tryCompileToProgram` is the parser: it returns
  * `std::nullopt` for anything outside the supported subset, in which case the caller must fall
  * back to the general engine (RE2). The parser is conservative on purpose - false negatives
  * (falling back to RE2 for a pattern we could have handled) are harmless, false positives
  * (accepting a pattern whose semantics we get wrong) are bugs.
  *
  * Semantics are byte-wise (Latin-1), matching how RE2 is used by the affected functions when
  * the pattern stays in the supported subset; any construct that would require Unicode-aware
  * matching makes the parser bail out.
  */
namespace DB::RegexpJIT
{

/// A set of bytes, as a 256-bit membership bitmap. `[abc]`, `[^abc]`, `.`, `\d`, ... all reduce to this.
struct CharSet
{
    std::array<uint64_t, 4> bitmap{};

    void add(uint8_t c) { bitmap[c >> 6] |= (1ULL << (c & 63u)); }
    bool contains(uint8_t c) const { return (bitmap[c >> 6] >> (c & 63u)) & 1u; }

    void addRange(uint8_t lo, uint8_t hi)
    {
        for (unsigned c = lo; c <= hi; ++c)
            add(static_cast<uint8_t>(c));
    }

    void invert()
    {
        for (auto & word : bitmap)
            word = ~word;
    }

    void unite(const CharSet & other)
    {
        for (size_t i = 0; i < 4; ++i)
            bitmap[i] |= other.bitmap[i];
    }

    bool intersects(const CharSet & other) const
    {
        for (size_t i = 0; i < 4; ++i)
            if (bitmap[i] & other.bitmap[i])
                return true;
        return false;
    }

    size_t count() const
    {
        size_t n = 0;
        for (auto word : bitmap)
            n += static_cast<size_t>(__builtin_popcountll(word));
        return n;
    }

    /// True if every member is a single ASCII byte (no byte >= 0x80). Such a set matches the same span
    /// byte-wise as RE2 matches code-point-wise, so any quantifier (including fixed counts) is safe.
    bool isAsciiOnly() const { return bitmap[2] == 0 && bitmap[3] == 0; }

    /// Fold ASCII case: if a letter is present, add its other case. Used for case-insensitive matching.
    void foldAsciiCase()
    {
        for (unsigned c = 'a'; c <= 'z'; ++c)
            if (contains(static_cast<uint8_t>(c)))
                add(static_cast<uint8_t>(c - 'a' + 'A'));
        for (unsigned c = 'A'; c <= 'Z'; ++c)
            if (contains(static_cast<uint8_t>(c)))
                add(static_cast<uint8_t>(c - 'A' + 'a'));
    }
};

enum class OpKind : uint8_t
{
    Literal,        /// Match a fixed byte string at the cursor and advance past it.
    PrefixAnchor,   /// `^` - assert the cursor is at the string start.
    SuffixAnchor,   /// `$` - assert the cursor is at the string end.
    CharQuant,      /// `[set]`, `[set]?`, `[set]*`, `[set]+`, `[set]{n,m}`, `.`, `.*`, a single char/class.
    Optional,       /// `(?:...)?` / `(...)?` - greedily match `body`, or skip it.
    CaptureStart,   /// Record the cursor as the start of capture group `capture_index`.
    CaptureEnd,     /// Record the cursor as the end of capture group `capture_index`.
};

struct Op
{
    OpKind kind = OpKind::Literal;

    /// OpKind::Literal
    std::vector<uint8_t> literal;

    /// OpKind::CharQuant
    CharSet set;
    uint32_t min = 1;
    uint32_t max = 1;                       /// UINT32_MAX means unbounded.
    bool greedy = true;
    /// True if the stop position is unambiguous (the following first-byte set is disjoint from `set`,
    /// or the quantifier is right-anchored), so it can be matched in a single greedy pass with no
    /// backtracking. Filled by `tryCompileToProgram`. See the no-backtracking analysis there.
    bool deterministic = true;

    /// OpKind::CaptureStart / CaptureEnd
    int capture_index = -1;

    /// OpKind::Optional
    std::vector<Op> body;
    bool optional_greedy = true;
};

struct RegexpProgram
{
    std::vector<Op> ops;
    int num_captures = 1;          /// Includes group 0 (the whole match). At least 1.
    bool anchored_start = false;   /// Whole pattern begins with `^`.
    bool anchored_end = false;     /// Whole pattern ends with `$`.
    bool case_insensitive = false;
    bool dot_all = false;          /// `(?s)` - `.` matches newline too.
};

struct ParseFlags
{
    bool case_insensitive = false;
    bool dot_all = false;          /// RE2 `DOT_NL` option, i.e. `.` matches `\n`.
    bool utf8 = false;             /// If the surrounding engine runs RE2 in UTF-8 mode, we must bail out.
};

/// Parse `pattern` into a `RegexpProgram`, or return nullopt if it is outside the supported subset.
std::optional<RegexpProgram> tryCompileToProgram(std::string_view pattern, const ParseFlags & flags);

}
