#pragma once

#include <base/types.h>

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

}
