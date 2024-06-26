#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <istream>
#include <ostream>


namespace DB
{

/// Variable-Length Quantity (VLQ) Base-128 compression, also known as Variable Byte (VB) or Varint encoding.

[[noreturn]] void throwReadAfterEOF();


inline void writeVarUInt(UInt64 x, WriteBuffer & ostr)
{
    while (x > 0x7F)
    {
        uint8_t byte = 0x80 | (x & 0x7F);

        ostr.nextIfAtEnd();
        *ostr.position() = byte;
        ++ostr.position();

        x >>= 7;
    }

    uint8_t final_byte = static_cast<uint8_t>(x);

    ostr.nextIfAtEnd();
    *ostr.position() = final_byte;
    ++ostr.position();
}

inline void writeVarUInt(UInt64 x, std::ostream & ostr)
{
    while (x > 0x7F)
    {
        uint8_t byte = 0x80 | (x & 0x7F);
        ostr.put(byte);

        x >>= 7;
    }

    uint8_t final_byte = static_cast<uint8_t>(x);
    ostr.put(final_byte);
}

inline char * writeVarUInt(UInt64 x, char * ostr)
{
    while (x > 0x7F)
    {
        uint8_t byte = 0x80 | (x & 0x7F);

        *ostr = byte;
        ++ostr;

        x >>= 7;
    }

    uint8_t final_byte = static_cast<uint8_t>(x);

    *ostr = final_byte;
    ++ostr;

    return ostr;
}

template <typename Out>
inline void writeVarInt(Int64 x, Out & ostr)
{
    writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char * writeVarInt(Int64 x, char * ostr)
{
    return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

namespace varint_impl
{

template <bool check_eof>
inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
    x = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        if constexpr (check_eof)
            if (istr.eof()) [[unlikely]]
                throwReadAfterEOF();

        UInt64 byte = static_cast<unsigned char>(*istr.position());
        ++istr.position();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
}

}

inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
    if (istr.buffer().end() - istr.position() >= 10)
        varint_impl::readVarUInt<false>(x, istr);
    else
        varint_impl::readVarUInt<true>(x, istr);
}

inline void readVarUInt(UInt64 & x, std::istream & istr)
{
    x = 0;
    for (size_t i = 0; i < 10; ++i)
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
    for (size_t i = 0; i < 10; ++i)
    {
        if (istr == end) [[unlikely]]
            throwReadAfterEOF();

        UInt64 byte = static_cast<unsigned char>(*istr);
        ++istr;
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return istr;
    }

    return istr;
}

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
        : (x < (1ULL << 63) ? 9
        : 10))))))));
}


inline size_t getLengthOfVarInt(Int64 x)
{
    return getLengthOfVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)));
}

}
