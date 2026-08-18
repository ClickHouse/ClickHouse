#pragma once

#include <base/types.h>
#include <string_view>

namespace DB
{

/// Text-index token format for the `keyValuePairs` tokenizer.
///
///     token   = key ‖ value ‖ trailer
///     trailer = reversed-varint((key.size() << 1) | is_rest)
///
/// Key first so tokens sort by key then value (key-prefix scans work). The key length sits in the
/// trailer, so the leading bytes stay exactly `key ‖ value`; the varint is written reversed so a decoder
/// can scan backward from the end. Length-delimited, so values may hold arbitrary bytes.
///
/// `is_rest` (trailer LSB): 0 for a key's first occurrence in the row, 1 for later duplicates. Positional
/// `m['key']` lookups match is_rest = 0 only.
String encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest);

/// Same encoding, appended into the caller-provided `out` (cleared first). Lets a hot loop reuse one
/// buffer instead of allocating a String per `(key, value)` pair.
void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out);

}
