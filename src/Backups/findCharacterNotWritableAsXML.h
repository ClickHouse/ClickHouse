#pragma once

#include <base/types.h>

#include <optional>
#include <string_view>

namespace DB
{

/// Escaping is not enough to make an arbitrary string writable as XML, which the `.backup` metadata document
/// is. XML 1.0 forbids the C0 control characters other than tab, line feed and carriage return, the surrogate
/// halves and `U+FFFE`/`U+FFFF` outright - the code points, not just their literal spelling - so no character
/// reference can carry one, and a byte sequence that is not valid UTF-8 names no code point at all. A value
/// holding one of those can only be refused: writing it produces a document that no parser will read back,
/// and encoding it differently would not be read back by an older server either.
///
/// A carriage return is not reported: it is a legal XML character, and end-of-line normalization rewriting it
/// to a line feed on read changes the value but still leaves the document readable.
///
/// Returns the byte offset of the first character `writeXMLStringForTextElementOrAttributeValue` would emit
/// unreadably, or nothing if the whole string can be written.
std::optional<size_t> findCharacterNotWritableAsXML(std::string_view s);

}
