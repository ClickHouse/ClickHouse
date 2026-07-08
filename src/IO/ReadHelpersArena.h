#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <base/StringRef.h>
#include <Common/Arena.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
}

inline StringRef readStringBinaryInto(Arena & arena, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (unlikely(size > DEFAULT_MAX_STRING_SIZE))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    char * data = arena.alloc(size);
    buf.readStrict(data, size);

    return StringRef(data, size);
}

}
