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
/// The leading `namespace` byte tags the token kind: `MAP_KV_NAMESPACE_REST` = a later duplicate of a key
/// already seen in the row, `MAP_KV_NAMESPACE_FIRST` = the key's first occurrence. Positional `m['key']`
/// lookups (first-value semantics) match the FIRST namespace only; existence over any occurrence unions
/// both. Duplicate keys are rare, so the duplicate kind is given the smaller byte value (0) and therefore
/// sorts first: when a part does contain duplicate-key tokens they become the dictionary's minimum token,
/// so their presence is detectable from the resident sparse index (its first boundary token) without
/// reading a dictionary block - a "does this part have duplicate keys?" check for free.
///
/// Keeping the discriminator in a dedicated byte (rather than the trailer) leaves the trailer a plain
/// key-length varint and reserves room for future token kinds without changing the on-disk layout - e.g. a
/// value-first namespace laid out as `value ‖ key` so tokens sort by value, giving value-prefix (or,
/// reversed, value-suffix) scans the way the FIRST namespace's `key ‖ value` gives key-prefix scans.
///
/// After the namespace, `key` comes first so tokens sort by (namespace, key, value) - key-prefix scans
/// work within a namespace. The key length lives in the trailer, written reversed so a decoder can scan
/// backward from the end; the middle bytes stay exactly `key ‖ value`, so values may hold arbitrary bytes.

/// Namespace byte values (see the format description above).
static constexpr char MAP_KV_NAMESPACE_FIRST = 1;   /// key's first occurrence in the row (the `m['key']` value)
static constexpr char MAP_KV_NAMESPACE_REST = 0;    /// a later duplicate of the same key in the row

String encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest);

/// Same encoding, appended into the caller-provided `out` (cleared first). Lets a hot loop reuse one
/// buffer instead of allocating a String per `(key, value)` pair.
void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out);

}
