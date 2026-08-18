#include <Interpreters/MapKeyValueToken.h>

#include <IO/VarInt.h>

namespace DB
{

void encodeMapKeyValueToken(std::string_view key, std::string_view value, bool is_rest, String & out)
{
    /// Append into the caller's buffer so the hot index-build loop reuses one allocation instead of a
    /// String per token.
    out.clear();

    const UInt64 key_length = key.size();

    /// Leading namespace byte + key + value + reversed varint of key.size (see MapKeyValueToken.h).
    out.reserve(1 + key.size() + value.size() + getLengthOfVarUInt(key_length));

    /// Namespace byte: 0 = the key's first occurrence in the row, 1 = a later duplicate.
    out.push_back(static_cast<char>(is_rest ? 1 : 0));
    out.append(key);
    out.append(value);

    /// Trailer = reversed varint of the key length, so a decoder can scan backward from the token end to
    /// find the key/value split. `key_length < 0x80` fits one byte (its own reverse); emit it directly.
    if (key_length < 0x80)
    {
        out.push_back(static_cast<char>(key_length));
    }
    else
    {
        char buf[10];
        size_t num_bytes = writeVarUInt(key_length, buf) - buf;
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
