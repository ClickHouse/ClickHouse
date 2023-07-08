#pragma once

#include <iostream>
#include <base/types.h>
#include <base/defines.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// Variable-Length Quantity (VLQ) Base-128 compression, also known as Variable Byte (VB) or Varint encoding.

/// Write UInt64 in variable length format (base128)
void writeVarUInt(UInt64 x, std::ostream & ostr);
void writeVarUInt(UInt64 x, WriteBuffer & ostr);
char * writeVarUInt(UInt64 x, char * ostr);

/// Read UInt64, written in variable length format (base128)
void readVarUInt(UInt64 & x, std::istream & istr);
void readVarUInt(UInt64 & x, ReadBuffer & istr);
const char * readVarUInt(UInt64 & x, const char * istr, size_t size);

/// Get the length of an variable-length-encoded integer
size_t getLengthOfVarUInt(UInt64 x);
size_t getLengthOfVarInt(Int64 x);

[[noreturn]] void throwReadAfterEOF();
[[noreturn]] void throwValueTooLargeForVarIntEncoding(UInt64 x);

/// Write Int64 in variable length format (base128)
template <typename Out>
inline void writeVarInt(Int64 x, Out & ostr)
{
    writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char * writeVarInt(Int64 x, char * ostr)
{
    return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}


/// Read Int64, written in variable length format (base128)
template <typename In>
inline void readVarInt(Int64 & x, In & istr)
{
    readVarUInt(*reinterpret_cast<UInt64*>(&x), istr);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
}

inline const char * readVarInt(Int64 & x, const char * istr, size_t size)
{
    const char * res = readVarUInt(*reinterpret_cast<UInt64*>(&x), istr, size);
    x = (static_cast<UInt64>(x) >> 1) ^ -(x & 1);
    return res;
}


/// For [U]Int32, [U]Int16, size_t.

inline void readVarUInt(UInt32 & x, ReadBuffer & istr)
{
    UInt64 tmp;
    readVarUInt(tmp, istr);
    x = static_cast<UInt32>(tmp);
}

inline void readVarInt(Int32 & x, ReadBuffer & istr)
{
    Int64 tmp;
    readVarInt(tmp, istr);
    x = static_cast<Int32>(tmp);
}

inline void readVarUInt(UInt16 & x, ReadBuffer & istr)
{
    UInt64 tmp;
    readVarUInt(tmp, istr);
    x = tmp;
}

inline void readVarInt(Int16 & x, ReadBuffer & istr)
{
    Int64 tmp;
    readVarInt(tmp, istr);
    x = tmp;
}

template <typename T>
requires (!std::is_same_v<T, UInt64>)
inline void readVarUInt(T & x, ReadBuffer & istr)
{
    UInt64 tmp;
    readVarUInt(tmp, istr);
    x = tmp;
}

template <bool fast>
inline void readVarUIntImpl(UInt64 & x, ReadBuffer & istr)
{
    x = 0;
    for (size_t i = 0; i < 9; ++i)
    {
        if constexpr (!fast)
            if (istr.eof()) [[unlikely]]
                throwReadAfterEOF();

        UInt64 byte = *istr.position();
        ++istr.position();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
}

inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
    if (istr.buffer().end() - istr.position() >= 9)
        return readVarUIntImpl<true>(x, istr);
    return readVarUIntImpl<false>(x, istr);
}


inline void readVarUInt(UInt64 & x, std::istream & istr)
{
    x = 0;
    for (size_t i = 0; i < 9; ++i)
    {
        UInt64 byte = istr.get();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
}

inline const char * readVarUInt(UInt64 & x, const char * istr, size_t size)
{
    const char * end = istr + size;

    x = 0;
    for (size_t i = 0; i < 9; ++i)
    {
        if (istr == end) [[unlikely]]
            throwReadAfterEOF();

        UInt64 byte = *istr;
        ++istr;
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return istr;
    }

    return istr;
}

/// NOTE: Due to historical reasons, only values up to 1<<63-1 can be safely encoded/decoded (bigger values are not idempotent under
/// encoding/decoding). This cannot be changed without breaking backward compatibility (some drivers, e.g. clickhouse-rs (Rust), have the
/// same limitation, others support the full 1<<64 range, e.g. clickhouse-driver (Python))
constexpr UInt64 VAR_UINT_MAX = (1ULL<<63) - 1;

inline void writeVarUInt(UInt64 x, WriteBuffer & ostr)
{
    if (x > VAR_UINT_MAX) [[unlikely]]
        throwValueTooLargeForVarIntEncoding(x);

    for (size_t i = 0; i < 9; ++i)
    {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

        ostr.nextIfAtEnd();
        *ostr.position() = byte;
        ++ostr.position();

        x >>= 7;
        if (!x)
            return;
    }
}


inline void writeVarUInt(UInt64 x, std::ostream & ostr)
{
    if (x > VAR_UINT_MAX) [[unlikely]]
        throwValueTooLargeForVarIntEncoding(x);

    for (size_t i = 0; i < 9; ++i)
    {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

        ostr.put(byte);

        x >>= 7;
        if (!x)
            return;
    }
}


inline char * writeVarUInt(UInt64 x, char * ostr)
{
    if (x > VAR_UINT_MAX) [[unlikely]]
        throwValueTooLargeForVarIntEncoding(x);

    for (size_t i = 0; i < 9; ++i)
    {
        uint8_t byte = x & 0x7F;
        if (x > 0x7F)
            byte |= 0x80;

        *ostr = byte;
        ++ostr;

        x >>= 7;
        if (!x)
            return ostr;
    }

    return ostr;
}


inline size_t getLengthOfVarUInt(UInt64 x)
{
    return x < (1ULL << 7) ? 1
        : (x < (1ULL << 14) ? 2
        : (x < (1ULL << 21) ? 3
        : (x < (1ULL << 28) ? 4
        : (x < (1ULL << 35) ? 5
        : (x < (1ULL << 42) ? 6
        : (x < (1ULL << 49) ? 7
        : (x < (1ULL << 56) ? 8
        : 9)))))));
}


inline size_t getLengthOfVarInt(Int64 x)
{
    return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

}
