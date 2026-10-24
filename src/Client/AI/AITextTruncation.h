#pragma once

#include <base/types.h>

#include <fmt/format.h>

#include <string_view>

namespace DB
{

/// Cut the string at the given byte size without splitting a UTF-8 sequence. Everything the agent
/// sends to the model - the conversation, the tool results, the query context - is truncated with
/// this helper: a string cut in the middle of a multi-byte sequence is invalid UTF-8 and breaks
/// both the JSON serialization of the history and the model request itself.
inline void truncateToUTF8Boundary(String & text, size_t size)
{
    if (text.size() <= size)
        return;
    /// Step back over the continuation bytes of the sequence the cut lands in, then over its
    /// leading byte.
    while (size > 0 && (static_cast<unsigned char>(text[size]) & 0xC0) == 0x80)
        --size;
    text.resize(size);
}

/// The same, keeping the last `size` bytes instead of the first ones.
inline void truncateToUTF8BoundaryFromLeft(String & text, size_t size)
{
    if (text.size() <= size)
        return;
    size_t offset = text.size() - size;
    /// Move forward past the continuation bytes so the remainder starts at a leading byte.
    while (offset < text.size() && (static_cast<unsigned char>(text[offset]) & 0xC0) == 0x80)
        ++offset;
    text.erase(0, offset);
}

/// Replace every byte that is not a part of a valid UTF-8 sequence with its `\xHH` escape. A value
/// of a `String` column can hold arbitrary bytes (`SELECT unhex('FF')`), and the escaped text formats
/// of ClickHouse keep them as they are, while the history of the conversation is serialized as JSON,
/// which accepts valid UTF-8 only: one such byte would make every later model request fail. So all
/// the text the agent stores - the results of the tools, the recent queries and their results, the
/// questions of the user - is passed through this before it is kept. The check is strict, as the JSON
/// serialization is: overlong forms, surrogates and code points above U+10FFFF are invalid.
inline String escapeInvalidUTF8(std::string_view text)
{
    const auto valid_sequence_length = [&](size_t pos) -> size_t
    {
        const auto byte = [&](size_t i) { return static_cast<unsigned char>(text[i]); };
        const unsigned char lead = byte(pos);
        if (lead < 0x80)
            return 1;

        size_t length = 0;
        UInt32 code_point = 0;
        UInt32 min_code_point = 0;
        if ((lead & 0xE0) == 0xC0)
        {
            length = 2;
            code_point = lead & 0x1F;
            min_code_point = 0x80;
        }
        else if ((lead & 0xF0) == 0xE0)
        {
            length = 3;
            code_point = lead & 0x0F;
            min_code_point = 0x800;
        }
        else if ((lead & 0xF8) == 0xF0)
        {
            length = 4;
            code_point = lead & 0x07;
            min_code_point = 0x10000;
        }
        else
            return 0;

        if (pos + length > text.size())
            return 0;
        for (size_t i = pos + 1; i < pos + length; ++i)
        {
            if ((byte(i) & 0xC0) != 0x80)
                return 0;
            code_point = (code_point << 6) | (byte(i) & 0x3F);
        }
        if (code_point < min_code_point || code_point > 0x10FFFF || (code_point >= 0xD800 && code_point <= 0xDFFF))
            return 0;
        return length;
    };

    String result;
    size_t copied = 0;
    size_t pos = 0;
    while (pos < text.size())
    {
        if (const size_t length = valid_sequence_length(pos))
        {
            pos += length;
            continue;
        }
        result.append(text.substr(copied, pos - copied));
        result += fmt::format("\\x{:02X}", static_cast<unsigned char>(text[pos]));
        ++pos;
        copied = pos;
    }
    if (copied == 0)
        return String(text);
    result.append(text.substr(copied));
    return result;
}

}
