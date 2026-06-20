#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>


namespace DB
{

/** JIT compilation of a subset of simple regular expressions into a native, SIMD-friendly matcher.
  *
  * The pattern is first parsed into a `RegexpJIT::RegexpProgram` (see `Common/RegexpJIT/RegexpProgram.h`);
  * if it falls into the supported subset it is compiled with LLVM (`CHJIT`) into a per-string matcher.
  * Callers that get a null matcher (unsupported pattern, JIT disabled, or below the compile count
  * threshold) must fall back to the general RE2 engine. The compiled matcher produces results that
  * are bit-for-bit identical to RE2 for the supported subset.
  *
  * The header intentionally exposes no LLVM types, so it can be included from `src/Functions`.
  */

/// Signature of a compiled matcher. The string is `[begin, end)`; the (leftmost, unanchored) match
/// is searched at or after `search_from` (in `[begin, end]`), while `^`/`$`/group 0 stay relative to
/// `begin`/`end`. Passing `search_from == begin` matches the whole string; advancing `search_from`
/// past a previous match drives iteration for `extractAll`/`replaceRegexp`.
/// Returns 1 on a match, 0 on no match. On a match, fills `capture_starts[g]` / `capture_ends[g]` for
/// every group `g` in `[0, num_captures)` (group 0 is the whole match); a group that did not
/// participate is reported as `nullptr`. The caller must provide arrays of at least `num_captures`
/// elements. The matcher never reads the capture arrays.
using JITRegexpMatcherFunc = uint8_t (*)(
    const uint8_t * begin, const uint8_t * end, const uint8_t * search_from,
    const uint8_t ** capture_starts, const uint8_t ** capture_ends);

struct RegexpJITMatcher
{
    JITRegexpMatcherFunc func = nullptr;
    int num_captures = 1;
    /// If true, the matcher does byte-wise matching of `.`/negated classes, which only agrees with
    /// RE2's UTF-8 mode on ASCII input; the caller must evaluate non-ASCII rows with RE2 instead
    /// (see `isAsciiData`).
    bool ascii_fallback = false;
    /// Keeps the compiled module (and its `CHJIT` instance) alive while the caller holds this handle.
    std::shared_ptr<void> keep_alive;

    explicit operator bool() const { return func != nullptr; }
};

/// Are all bytes in `[begin, end)` ASCII (< 0x80)? Used to decide, per row, whether the byte-wise
/// JIT matcher agrees with RE2's UTF-8 semantics for `ascii_fallback` patterns.
inline bool isAsciiData(const uint8_t * begin, const uint8_t * end)
{
    const uint8_t * pos = begin;
    for (; pos + 8 <= end; pos += 8)
    {
        uint64_t word;
        memcpy(&word, pos, sizeof(word));
        if (word & 0x8080808080808080ULL)
            return false;
    }
    for (; pos < end; ++pos)
        if (*pos & 0x80)
            return false;
    return true;
}

/// Get a JIT matcher for `pattern` with the given flags, compiling and caching it on demand.
/// Returns an empty handle (`func == nullptr`) if the pattern is not in the supported subset, if
/// the embedded compiler is disabled, or if the pattern has been seen fewer than
/// `min_count_to_compile` times (so that rarely-used patterns are not compiled).
RegexpJITMatcher getRegexpJITMatcher(
    const std::string & pattern, bool case_insensitive, bool dot_all, size_t min_count_to_compile);

/// Drop the static `CHJIT` instance used for compiled regular expressions (see `resetExpressionJITInstance`).
void resetRegexpJITInstance();

}
