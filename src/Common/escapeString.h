#pragma once

#include <base/types.h>

namespace DB
{

String escapeString(std::string_view value);

/// Writes invisible characters (control, formatting and combining characters, such as a byte order mark
/// or a zero-width space, and unassigned code points) and bytes of invalid UTF-8 as `\xNN`.
String escapeNonPrintableCharacters(String value);

}
