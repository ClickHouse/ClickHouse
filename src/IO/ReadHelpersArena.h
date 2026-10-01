#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/Arena.h>
#include <Common/StringWithMemoryTracking.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int CANNOT_READ_ALL_DATA;
}

/// Appends bytes to `s` until it holds `size` of them, extending it only by what the buffer already has.
template <typename StringType>
inline void readStringGrowing(StringType & s, size_t size, ReadBuffer & buf)
{
    while (s.size() < size)
    {
        if (buf.eof())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data. Bytes read: {}. Bytes expected: {}.", s.size(), size);

        const size_t bytes_to_copy = std::min(size - s.size(), buf.available());
        s.append(buf.position(), bytes_to_copy);
        buf.position() += bytes_to_copy;
    }
}

inline std::string_view readStringBinaryInto(Arena & arena, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (unlikely(size > DEFAULT_MAX_STRING_SIZE))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    if (buf.available() >= size)
    {
        char * data = arena.alloc(size);
        buf.readStrict(data, size);
        return std::string_view(data, size);
    }

    /// An `Arena` cannot reuse a superseded block, so it is allocated once the value is complete;
    /// the staging container enforces the memory limit.
    StringWithMemoryTracking staged;
    readStringGrowing(staged, size, buf);

    char * data = arena.alloc(size);
    memcpy(data, staged.data(), size);
    return std::string_view(data, size);
}

}
