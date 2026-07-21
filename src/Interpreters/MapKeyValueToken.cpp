#include <Interpreters/MapKeyValueToken.h>

#include <IO/VarInt.h>

#include <algorithm>

namespace DB
{

void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out)
{
    /// Encode into the caller's buffer so a per-token String allocation is avoided on the hot index
    /// build path: reused across a loop, `out` keeps its capacity and stops reallocating once it has
    /// grown to the widest token.
    out.clear();
    out.reserve(key.size() + value.size() + 1);
    out.append(key);
    out.append(value);

    /// Trailer = reversed varint of `(key.size() << 1) | is_rest`. The per-key occurrence flag rides
    /// in the LSB (0 = first occurrence of the key in the row, 1 = a later one); decode recovers
    /// `key.size() = x >> 1` and `is_rest = x & 1`. The varint is written reversed so it can be decoded
    /// by scanning backward from the end of the token (the terminator byte ends up leftmost). See the
    /// format notes in MapKeyValueToken.h.
    const UInt64 packed = (static_cast<UInt64>(key.size()) << 1) | (is_rest ? 1ULL : 0ULL);
    /// A varint occupies a single byte exactly when its value is below 0x80 (128) — the high bit is
    /// the "more bytes follow" marker. The occurrence flag takes the low bit, so `packed < 0x80` is
    /// the same as `key.size() < 64` — the overwhelmingly common case. Then the trailer is that one
    /// byte (high bit already clear, and a single byte is its own reverse), so emit it directly and
    /// skip writeVarUInt and the byte-reversal loop.
    if (packed < 0x80)
    {
        out.push_back(static_cast<char>(packed));
    }
    else
    {
        char buf[10];
        size_t num_bytes = writeVarUInt(packed, buf) - buf;
        for (size_t i = num_bytes; i-- > 0;)
            out.push_back(buf[i]);
    }
}

String encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest)
{
    String result;
    encodeMapKeyValueToken(key, value, is_rest, result);
    return result;
}

DecodedMapKeyValueToken decodeMapKeyValueToken(std::string_view token)
{
    /// Precondition: `token` was produced by `encodeMapKeyValueToken` (i.e. it comes from a
    /// `keyValuePairs` index dictionary). A malformed token could make `key_len` exceed the
    /// token size; callers must not pass untrusted tokens here.

    if (token.empty())
        return {};

    UInt64 packed = 0;
    size_t trailer_bytes = 0;

    /// Fast path: a key shorter than 64 bytes has a single trailer byte with its high bit clear. In
    /// the reversed encoding a single-byte trailer is exactly the case where the last token byte has
    /// its high bit clear (a multi-byte trailer ends in a continuation byte). Avoids the buffer, the
    /// scan and readVarUInt on the hot per-token path.
    const UInt8 last_byte = static_cast<UInt8>(token.back());
    if (!(last_byte & 0x80))
    {
        packed = last_byte;
        trailer_bytes = 1;
    }
    else
    {
        /// Rare: multi-byte length. Scan backward collecting the reversed varint bytes; they come
        /// out in normal varint order, so readVarUInt decodes them directly. The scan is bounded by
        /// the widest a valid trailer can be: the trailer encodes `packed = (key.size() << 1) | is_rest`
        /// and `key.size() <= token.size()`, so `packed <= (token.size() << 1) | 1` — bound by the varint
        /// length of that. (Bounding by `token.size()` alone would be too small: e.g. a 64-byte key with
        /// an empty value gives `packed = 128`, a 2-byte trailer, yet `token.size() = 66` whose varint is
        /// 1 byte, so the scan would stop before the terminator and mis-decode to `packed = 0`.) This also
        /// rejects a corrupt token claiming a longer trailer and never exceeds 10 (the max for a UInt64),
        /// so buf is always large enough.
        char buf[10];
        size_t num_bytes = 0;
        bool terminated = false;
        const size_t max_trailer_bytes = getLengthOfVarUInt((static_cast<UInt64>(token.size()) << 1) | 1);
        size_t pos = token.size();
        while (pos > 0 && num_bytes < max_trailer_bytes)
        {
            UInt8 byte = static_cast<UInt8>(token[pos - 1]);
            buf[num_bytes++] = static_cast<char>(byte);
            --pos;
            if (!(byte & 0x80))
            {
                terminated = true;
                break;
            }
        }

        /// Decode only a complete varint (one that ended with a terminator byte). A truncated
        /// trailer would otherwise make readVarUInt read past the buffer; treat it as packed 0.
        if (terminated)
            readVarUInt(packed, buf, num_bytes);
        trailer_bytes = num_bytes;
    }

    /// `trailer_bytes <= token.size()`, so `value_end` cannot underflow. Clamp the decoded key
    /// length to the available bytes so the substr calls can never throw or overrun.
    size_t value_end = token.size() - trailer_bytes;
    UInt64 key_len = std::min<UInt64>(packed >> 1, value_end);
    const bool is_rest = (packed & 1) != 0;
    return {token.substr(0, key_len), token.substr(key_len, value_end - key_len), is_rest};
}

}
