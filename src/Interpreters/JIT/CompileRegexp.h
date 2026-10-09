#pragma once

#include <cstdint>
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
///
/// Matching is byte-wise. For the supported subset this matches RE2 (which runs in UTF-8 mode)
/// exactly on valid UTF-8 input: the subset only allows `.` and negated classes with `*`/`+`
/// (maximal runs whose stop byte is ASCII, hence at a code-point boundary), and char classes are
/// ASCII-only, so the byte- and code-point interpretations have the same match span. On invalid
/// UTF-8 the results may differ from RE2, which is implementation-specific and acceptable.
using JITRegexpMatcherFunc = uint8_t (*)(
    const uint8_t * begin, const uint8_t * end, const uint8_t * search_from,
    const uint8_t ** capture_starts, const uint8_t ** capture_ends);

struct RegexpJITMatcher
{
    JITRegexpMatcherFunc func = nullptr;
    int num_captures = 1;
    /// Keeps the compiled module (and its `CHJIT` instance) alive while the caller holds this handle.
    std::shared_ptr<void> keep_alive;

    explicit operator bool() const { return func != nullptr; }
};

/// Get a JIT matcher for `pattern` with the given flags, compiling and caching it on demand.
/// Returns an empty handle (`func == nullptr`) if the pattern is not in the supported subset, if
/// the embedded compiler is disabled, or if the pattern has been seen fewer than
/// `min_count_to_compile` times (so that rarely-used patterns are not compiled).
RegexpJITMatcher getRegexpJITMatcher(
    const std::string & pattern, bool case_insensitive, bool dot_all, size_t min_count_to_compile);

/// Drop the static `CHJIT` instance used for compiled regular expressions (see `resetExpressionJITInstance`).
void resetRegexpJITInstance();

}
