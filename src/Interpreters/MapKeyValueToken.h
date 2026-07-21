#pragma once

#include <base/types.h>
#include <string_view>
#include <utility>

namespace DB
{

/// Text-index token format for the `keyValuePairs` tokenizer.
///
///     token   = key ‖ value ‖ trailer
///     trailer = reversed-varint((key.size() << 1) | is_rest)
///
/// Key first, so tokens sort by key then value: shared key prefixes front-code and key-prefix scans
/// (`mapContainsKey`) work. The key length is a trailer, not a prefix, so the leading bytes stay exactly
/// `key ‖ value`; it is a *reversed* varint (bytes emitted last-to-first) so decode scans backward from
/// the end and the terminator byte (high bit clear) stops it.
///
/// Decoding is length-delimited: read `key.size()` from the trailer, `value_end = token.size() -
/// trailer_bytes`, then `key = [0, key.size())`, `value = [key.size(), value_end)`. So the value may
/// hold any bytes with no ambiguity and distinct pairs never collide.
///
/// `is_rest` (LSB of the trailer integer): 0 for a key's first occurrence in the row, 1 for a later one.
/// `m['key']` returns the first value, so `m['key'] = v` must not match a later duplicate — it pins
/// is_rest = 0. First = 0 keeps unique-key maps all-zero and sorts the first occurrence first. Decode
/// recovers `is_rest = x & 1`, `key.size() = x >> 1`.
///
/// Fast path: a key < 64 bytes keeps the packed value < 128 = one trailer byte with its high bit clear
/// (its own reverse), so one check on the last token byte avoids the backward scan and `readVarUInt`.
///
/// Flag use per lookup: `m['key'] = v` emits one token (is_rest = 0); `mapContainsKeyValue` (existence)
/// emits both and unions them. Decode-scan matchers (LIKE / prefix / suffix / key- or value-only) test
/// each decoded pair: the `m['key']` positional forms require is_rest = 0, the `mapContains*` existence
/// forms ignore it.
struct DecodedMapKeyValueToken
{
    std::string_view key;
    std::string_view value;
    bool is_rest = false;
};

String encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest);

/// Same encoding, appended into the caller-provided `out` (cleared first). Lets a hot loop reuse one
/// buffer instead of allocating a String per `(key, value)` pair.
void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out);

/// Splits a token produced by encodeMapKeyValueToken back into its (key, value) views and `is_rest`.
DecodedMapKeyValueToken decodeMapKeyValueToken(std::string_view token);

}
