#pragma once

#include <base/types.h>

namespace DB
{

/// Removes new lines from query.
///
/// But with some care:
/// - don't join lines inside non-whitespace tokens (e.g. multiline string literals)
/// - don't join line after comment (because it can be single-line comment).
/// All other whitespaces replaced to a single whitespace.
String toOneLineQuery(const String & query);

}
