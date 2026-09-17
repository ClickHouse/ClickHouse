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

}
