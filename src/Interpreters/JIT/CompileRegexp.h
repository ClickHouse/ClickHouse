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

/// Signature of a compiled matcher. Matches one string `[begin, end)`.
/// Returns 1 on (an unanchored) match, 0 on no match. On a match, fills `capture_starts[g]` /
/// `capture_ends[g]` for every group `g` in `[0, num_captures)` (group 0 is the whole match);
/// a group that did not participate is reported as `nullptr`. The caller must provide arrays of
/// at least `num_captures` elements. The matcher never reads the capture arrays.
/// May return 2 ("non-ASCII") when `ascii_fallback` is set: the string has a byte >= 0x80, whose
/// byte-wise interpretation of `.`/negated classes would not match RE2's UTF-8 mode, so the caller
/// must evaluate this row with RE2 instead.
using JITRegexpMatcherFunc = uint8_t (*)(
    const uint8_t * begin, const uint8_t * end, const uint8_t ** capture_starts, const uint8_t ** capture_ends);

struct RegexpJITMatcher
{
    JITRegexpMatcherFunc func = nullptr;
    int num_captures = 1;
    /// If true, the matcher may return 2 for non-ASCII strings; the caller must use RE2 for them.
    bool ascii_fallback = false;
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
