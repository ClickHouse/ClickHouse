#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
String likePatternToRegexp(std::string_view pattern);

/// Is the [I]LIKE pattern equivalent to a substring search?
/// Returns true if the pattern is of the form '%substring%' (with no other wildcards),
/// and writes the extracted substring to 'res'.
bool likePatternIsSubstring(std::string_view pattern, String & res);

}
