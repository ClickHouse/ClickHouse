#include <Interpreters/MapKeyValueToken.h>

#include <IO/VarInt.h>

#include <algorithm>

namespace DB
{

String encodeMapKeyValueToken(std::string_view key, std::string_view value)
{
    String result;
    result.reserve(key.size() + value.size() + 1);
    result.append(key);
    result.append(value);

    /// Append the key length as a varint whose bytes are reversed, so it can be decoded by reading
    /// backward from the end of the token (the varint's terminator byte, with its high bit clear,
    /// ends up leftmost and stops the backward scan).
    if (key.size() < 0x80)
    {
        /// Fast path: almost every key is shorter than 128 bytes, so its length is a single byte
        /// with the high bit clear — no varint or reversal needed.
        result.push_back(static_cast<char>(key.size()));
    }
    else
    {
        char buf[10];
        size_t num_bytes = writeVarUInt(key.size(), buf) - buf;
        for (size_t i = num_bytes; i-- > 0;)
            result.push_back(buf[i]);
    }
    return result;
}

std::pair<std::string_view, std::string_view> decodeMapKeyValueToken(std::string_view token)
{
    /// Precondition: `token` was produced by `encodeMapKeyValueToken` (i.e. it comes from a
    /// `keyValuePairs` index dictionary). A malformed token could make `key_len` exceed the
    /// token size; callers must not pass untrusted tokens here.

    if (token.empty())
        return {{}, {}};

    UInt64 key_len = 0;
    size_t trailer_bytes = 0;

    /// Fast path: almost every key is shorter than 128 bytes, so the length is a single trailer
    /// byte with its high bit clear. In the reversed encoding a single-byte trailer is exactly the
    /// case where the last token byte has its high bit clear (a multi-byte trailer ends in a
    /// continuation byte). Avoids the buffer, the scan and readVarUInt on the hot per-token path.
    const UInt8 last_byte = static_cast<UInt8>(token.back());
    if (!(last_byte & 0x80))
    {
        key_len = last_byte;
        trailer_bytes = 1;
    }
    else
    {
        /// Rare: multi-byte length. Scan backward collecting the reversed varint bytes; they come
        /// out in normal varint order, so readVarUInt decodes them directly. The scan is bounded by
        /// getLengthOfVarUInt(token.size()) — the widest a valid trailer can be, since
        /// key_len <= token.size() — which also rejects a corrupt token claiming a longer trailer
        /// and never exceeds 10 (the max for a UInt64), so buf is always large enough.
        char buf[10];
        size_t num_bytes = 0;
        bool terminated = false;
        const size_t max_trailer_bytes = getLengthOfVarUInt(token.size());
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
        /// trailer would otherwise make readVarUInt read past the buffer; treat it as key_len 0.
        if (terminated)
            readVarUInt(key_len, buf, num_bytes);
        trailer_bytes = num_bytes;
    }

    /// `trailer_bytes <= token.size()`, so `value_end` cannot underflow. Clamp the decoded key
    /// length to the available bytes so the substr calls can never throw or overrun.
    size_t value_end = token.size() - trailer_bytes;
    key_len = std::min<UInt64>(key_len, value_end);
    return {token.substr(0, key_len), token.substr(key_len, value_end - key_len)};
}

}
