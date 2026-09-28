#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

/// XML 1.0 represents neither the C0 controls, the surrogate halves, `U+FFFE`/`U+FFFF`, nor invalid UTF-8,
/// so such a value can only be refused.
///
/// A carriage return is not reported: it is legal XML, and normalizing it to a line feed on read alters the
/// value but keeps it readable.
///
/// Returns the byte offset of the first character `writeXMLStringForTextElementOrAttributeValue` would emit
/// unreadably, or nothing.
std::optional<size_t> findCharacterNotWritableAsXML(std::string_view s);

}
