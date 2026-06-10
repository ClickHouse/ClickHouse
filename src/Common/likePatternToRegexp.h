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

/// Whether a standard-backslash LIKE pattern contains an "unknown" escape `\c` where `c` is not `%`, `_` or `\`.
/// Such a sequence keeps the literal backslash at row-level (see `likePatternToRegexp`), but the index-side
/// prefix extractor and the `nextInStringLike` tokenizer drop it, so they must decline patterns that contain it.
bool likePatternHasUnknownBackslashEscape(std::string_view pattern);

}
