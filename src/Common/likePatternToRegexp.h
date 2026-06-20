#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

/// Rewrites a LIKE pattern with custom escape character into a LIKE pattern with standard escape character (backslash).
/// Example: with escape_char='#': "50#%off" -> "50\%off"
String likePatternWithCustomEscapeToLikePattern(std::string_view pattern, char escape_char);

/// Whether a standard-backslash LIKE pattern is unsafe for index analysis because the index-side prefix
/// extractor / `nextInStringLike` tokenizer would treat a backslash escape differently from row-level
/// matching (see `likePatternToRegexp`). This is true for an "unknown" escape `\c` where `c` is not `%`,
/// `_` or `\` (kept as a literal backslash at row-level but dropped index-side), and for a trailing
/// backslash (rejected with CANNOT_PARSE_ESCAPE_SEQUENCE at row-level but dropped index-side). Callers
/// must decline patterns for which this returns true and fall back to row-level evaluation.
bool likePatternHasUnknownBackslashEscape(std::string_view pattern);

}
