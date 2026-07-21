#pragma once

#include <base/types.h>
#include <string_view>
#include <utility>

namespace DB
{

/// Text-index token format for the `keyValuePairs` tokenizer.
///
/// A map `(key, value)` pair is encoded as one token:
///
///     token   = key ‖ value ‖ trailer
///     trailer = reversed-varint((key.size() << 1) | is_rest)
///
/// Layout rationale:
/// - The `key` is at the FRONT, so tokens sharing a key sort together: front-coding compresses the
///   shared key prefix, and key-prefix scans (`mapContainsKey`, `mapContainsKeyLike`) stay usable.
///   The `value` follows, so tokens sort by `key` then `value`.
/// - The key length is a TRAILER, not a prefix, so the leading bytes are exactly `key ‖ value`
///   (undisturbed for sorting / front-coding). It is written as a *reversed* varint (its bytes
///   emitted last-to-first) so the decoder recovers it by scanning BACKWARD from the end of the
///   token: the varint terminator byte (high bit clear) ends up leftmost and stops the scan.
///
/// Decoding is LENGTH-delimited, never marker-delimited: `decodeMapKeyValueToken` reads `key.size()`
/// from the trailer, computes `value_end = token.size() - trailer_bytes`, and returns
/// `key = [0, key.size())`, `value = [key.size(), value_end)`. Because the boundaries come from
/// lengths, the value may contain ANY bytes (including a byte equal to the trailer or to a would-be
/// delimiter) with no ambiguity, and no two distinct pairs ever collide.
///
/// Per-key occurrence flag (`is_rest`): `m['key']` returns the FIRST value for a key, whereas the
/// index records every pair, so an exact `m['key'] = v` over a map with a duplicated key must not
/// match a later occurrence. The trailer therefore packs an `is_rest` bit (0 for the first occurrence
/// of the key within a row, 1 for any later occurrence) into the LSB of the length integer:
/// `trailer = reversed-varint((key.size() << 1) | is_rest)`. First occurrence = 0 is deliberate — a
/// unique-key map (the common case) is then all-zero-flag, and for equal `(key, value)` the first
/// occurrence sorts before a later one. The varint codec is untouched (it just encodes a different
/// integer); decode reads the varint, then `is_rest = x & 1` and `key.size() = x >> 1`.
///
/// Fast path: because the flag rides in the LSB, a key shorter than 64 bytes keeps the packed value
/// under 128 → a single trailer byte with its high bit clear, which after reversal is exactly "the
/// last token byte has its high bit clear" — so one check on the last byte avoids the backward scan
/// and `readVarUInt` on the hot per-token path (and unique-key maps stay one trailer byte).
///
/// How a lookup uses the flag depends on whether it can name an exact token:
///   - Exact-token lookups (both key and value known):
///       * first-value `m['key'] = v` generates ONE token — `encode(key, value, is_rest = 0)` — so it
///         matches only the key's first value, exactly like a plain scan of `m['key']`;
///       * existence (`mapContainsKeyValue`, matches any position) generates TWO tokens, `is_rest = 0`
///         and `is_rest = 1`, and unions their postings — a union of two exact tokens is still an
///         exact existence answer.
///   - Decode-scan matchers (LIKE / prefix / suffix, or when only the key or only the value is known)
///     decode each dictionary entry and test its `(key, value)`; the `m['key']` positional forms
///     additionally require `is_rest = 0` (first value only), while the `mapContains*` existence forms
///     IGNORE the flag, so first- and later occurrences match inherently.
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
