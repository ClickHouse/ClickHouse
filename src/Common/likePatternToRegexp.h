#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Transforms the [I]LIKE expression into regexp re2. For example, abc%def -> ^abc.*def$
/// `has_end_anchor`, if not null, reports whether the regexp ends with `$`. Callers which run the
/// regexp through a PCRE-flavoured engine need it: there `$` also matches before a final newline.
String likePatternToRegexp(std::string_view pattern, bool * has_end_anchor = nullptr);

}
