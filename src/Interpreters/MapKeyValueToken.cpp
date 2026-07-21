#include <Interpreters/MapKeyValueToken.h>

#include <IO/VarInt.h>

#include <algorithm>

namespace DB
{

void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out)
{
    /// Append into the caller's buffer so the hot index-build loop reuses one allocation instead of a
    /// String per token.
    out.clear();
    out.reserve(key.size() + value.size() + 1);
    out.append(key);
    out.append(value);

    /// Trailer = reversed varint of `(key.size() << 1) | is_rest`, written reversed so decode can scan
    /// backward from the token end. See MapKeyValueToken.h.
    const UInt64 packed = (static_cast<UInt64>(key.size()) << 1) | (is_rest ? 1ULL : 0ULL);
    /// `packed < 0x80` (i.e. key.size() < 64) fits one varint byte, whose high bit is already clear and
    /// which is its own reverse — emit it directly, skipping writeVarUInt and the reversal loop.
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
    /// Precondition: `token` came from `encodeMapKeyValueToken`. Callers must not pass untrusted tokens
    /// (a malformed trailer could make `key_len` exceed the token size).

    if (token.empty())
        return {};

    UInt64 packed = 0;
    size_t trailer_bytes = 0;

    /// Fast path: a single-byte trailer is exactly "last token byte has its high bit clear" (a multi-byte
    /// trailer ends in a continuation byte). Avoids the backward scan and readVarUInt.
    const UInt8 last_byte = static_cast<UInt8>(token.back());
    if (!(last_byte & 0x80))
    {
        packed = last_byte;
        trailer_bytes = 1;
    }
    else
    {
        /// Rare: multi-byte length. Scan backward collecting the reversed varint bytes (they come out in
        /// normal order for readVarUInt). Bound the scan by the varint length of `(token.size() << 1) | 1`,
        /// the widest a valid trailer can be. Bounding by `token.size()` alone is too small: a 64-byte key
        /// with an empty value packs to 128 (a 2-byte trailer) while `token.size() = 66` is a 1-byte varint,
        /// so the scan would stop early and mis-decode to 0. Never exceeds 10 (UInt64 max), so buf fits.
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
