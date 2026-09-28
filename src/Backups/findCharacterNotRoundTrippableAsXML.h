#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

/// A value written into the manifest has to read back unchanged.
///
/// XML 1.0 represents no C0 control, no surrogate half, no `U+FFFE`/`U+FFFF` and no invalid UTF-8, so a
/// value holding one can only be refused.
///
/// A carriage return is refused too: it is legal XML, but normalization rewrites it to a line feed on
/// read, so the value comes back different.
///
/// Returns the byte offset of the first such character, or nothing if the whole string survives the
/// round trip.
std::optional<size_t> findCharacterNotRoundTrippableAsXML(std::string_view s);

}
