#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Text-index token format for the `keyValuePairs` tokenizer.
///
///     token   = namespace ‖ key ‖ value ‖ trailer
///     trailer = reversed-varint(key.size())
///
/// The leading `namespace` byte tags the token kind; today: 0 = the key's first occurrence in the row,
/// 1 = a later duplicate. Positional `m['key']` lookups (first-value semantics) match namespace 0 only;
/// existence over any occurrence unions namespaces 0 and 1. Keeping the discriminator in a dedicated byte
/// (rather than the trailer) leaves the trailer a plain key-length varint and reserves room for future
/// token kinds without changing the on-disk layout - e.g. a value-first namespace laid out as
/// `value ‖ key` so tokens sort by value, giving value-prefix (or, reversed, value-suffix) scans the way
/// namespace 0's `key ‖ value` gives key-prefix scans.
///
/// After the namespace, `key` comes first so tokens sort by (namespace, key, value) - key-prefix scans
/// work within a namespace. The key length lives in the trailer, written reversed so a decoder can scan
/// backward from the end; the middle bytes stay exactly `key ‖ value`, so values may hold arbitrary bytes.
String encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest);

/// Same encoding, appended into the caller-provided `out` (cleared first). Lets a hot loop reuse one
/// buffer instead of allocating a String per `(key, value)` pair.
void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out);

}
