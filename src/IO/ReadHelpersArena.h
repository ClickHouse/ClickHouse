#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <Common/Arena.h>
#include <Common/StringWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int CANNOT_READ_ALL_DATA;
}

/// Appends bytes to `s` until it holds `size` of them, extending it only by what the buffer already has.
inline void readStringGrowing(String & s, size_t size, ReadBuffer & buf)
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

/// Like `readStringBinary`, but the destination grows as the bytes arrive.
inline void readStringBinaryGrowing(String & s, ReadBuffer & buf, size_t max_string_size = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (unlikely(size > max_string_size))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    s.clear();
    readStringGrowing(s, size, buf);
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

    /// An Arena cannot reuse a superseded block, so it is allocated once, after the value is complete.
    /// Staging in throwing containers keeps the memory limit enforced as the Arena enforces it.
    VectorWithMemoryTracking<StringWithMemoryTracking> chunks;
    size_t bytes_read = 0;
    while (bytes_read < size)
    {
        if (buf.eof())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data. Bytes read: {}. Bytes expected: {}.", bytes_read, size);

        const size_t bytes_to_copy = std::min(size - bytes_read, buf.available());
        chunks.emplace_back(buf.position(), bytes_to_copy);
        buf.position() += bytes_to_copy;
        bytes_read += bytes_to_copy;
    }

    char * data = arena.alloc(size);
    char * pos = data;
    for (const auto & chunk : chunks)
    {
        memcpy(pos, chunk.data(), chunk.size());
        pos += chunk.size();
    }

    return std::string_view(data, size);
}

}
