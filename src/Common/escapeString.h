#pragma once

#include <base/types.h>

namespace DB
{

String escapeString(std::string_view value);

/// Writes invisible characters (control, formatting and combining characters, such as a byte order mark
/// or a zero-width space, unassigned code points, and blank characters such as a no-break space or a Hangul filler)
/// and bytes of invalid UTF-8 as `\xNN`. The ASCII space is kept.
String escapeNonPrintableCharacters(String value);

}
