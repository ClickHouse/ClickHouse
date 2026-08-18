#include <Interpreters/MapKeyValueToken.h>

#include <IO/VarInt.h>

namespace DB
{

void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out)
{
    /// Append into the caller's buffer so the hot index-build loop reuses one allocation instead of a
    /// String per token.
    out.clear();

    /// Trailer = reversed varint of `(key.size() << 1) | is_rest`, written reversed so a decoder can scan
    /// backward from the token end. See MapKeyValueToken.h.
    const UInt64 packed = (static_cast<UInt64>(key.size()) << 1) | (is_rest ? 1ULL : 0ULL);

    /// Reserve the exact size, trailer included: `getLengthOfVarUInt(packed)` is the trailer width, which
    /// is 1 for the common short-key case but grows for keys >= 64 bytes. Reserving `+ 1` would under-size
    /// the multi-byte trailer and force a reallocation.
    out.reserve(key.size() + value.size() + getLengthOfVarUInt(packed));
    out.append(key);
    out.append(value);

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

}
