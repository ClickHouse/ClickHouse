#pragma once

#include <iostream>
#include <Core/Types.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{


/** Write UInt64 in variable length format (base128) NOTE Only up to 2^63 - 1 are supported. */
void writeVarUInt(UInt64 x, std::ostream & ostr);
void writeVarUInt(UInt64 x, WriteBuffer & ostr);
char * writeVarUInt(UInt64 x, char * ostr);


/** Read UInt64, written in variable length format (base128) */
void readVarUInt(UInt64 & x, std::istream & istr);
void readVarUInt(UInt64 & x, ReadBuffer & istr);
const char * readVarUInt(UInt64 & x, const char * istr, size_t size);


/** Get the length of UInt64 in VarUInt format */
size_t getLengthOfVarUInt(UInt64 x);

/** Get the Int64 length in VarInt format */
size_t getLengthOfVarInt(Int64 x);


/** Write Int64 in variable length format (base128) */
template <typename OUT>
inline void writeVarInt(Int64 x, OUT & ostr)
{
    writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}

inline char * writeVarInt(Int64 x, char * ostr)
{
    return writeVarUInt(static_cast<UInt64>((x << 1) ^ (x >> 63)), ostr);
}


/** Read Int64, written in variable length format (base128) */
template <typename IN>
inline void readVarInt(Int64 & x, IN & istr)
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


inline void writeVarT(UInt64 x, std::ostream & ostr) { writeVarUInt(x, ostr); }
inline void writeVarT(Int64 x, std::ostream & ostr) { writeVarInt(x, ostr); }
inline void writeVarT(UInt64 x, WriteBuffer & ostr) { writeVarUInt(x, ostr); }
inline void writeVarT(Int64 x, WriteBuffer & ostr) { writeVarInt(x, ostr); }
inline char * writeVarT(UInt64 x, char * & ostr) { return writeVarUInt(x, ostr); }
inline char * writeVarT(Int64 x, char * & ostr) { return writeVarInt(x, ostr); }

inline void readVarT(UInt64 & x, std::istream & istr) { readVarUInt(x, istr); }
inline void readVarT(Int64 & x, std::istream & istr) { readVarInt(x, istr); }
inline void readVarT(UInt64 & x, ReadBuffer & istr) { readVarUInt(x, istr); }
inline void readVarT(Int64 & x, ReadBuffer & istr) { readVarInt(x, istr); }
inline const char * readVarT(UInt64 & x, const char * istr, size_t size) { return readVarUInt(x, istr, size); }
inline const char * readVarT(Int64 & x, const char * istr, size_t size) { return readVarInt(x, istr, size); }


/// For [U]Int32, [U]Int16, size_t.

inline void readVarUInt(UInt32 & x, ReadBuffer & istr)
{
    UInt64 tmp;
    readVarUInt(tmp, istr);
    x = tmp;
}

inline void readVarInt(Int32 & x, ReadBuffer & istr)
{
    Int64 tmp;
    readVarInt(tmp, istr);
    x = tmp;
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
inline std::enable_if_t<!std::is_same_v<T, UInt64>, void>
readVarUInt(T & x, ReadBuffer & istr)
{
    UInt64 tmp;
    readVarUInt(tmp, istr);
    x = tmp;
}


inline void throwReadAfterEOF()
{
    throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}

inline void readVarUInt(UInt64 & x, ReadBuffer & istr)
{
    x = 0;
    for (size_t i = 0; i < 9; ++i)
    {
        if (istr.eof())
            throwReadAfterEOF();

        UInt64 byte = *istr.position();
        ++istr.position();
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return;
    }
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
        if (istr == end)
            throwReadAfterEOF();

        UInt64 byte = *istr;
        ++istr;
        x |= (byte & 0x7F) << (7 * i);

        if (!(byte & 0x80))
            return istr;
    }

    return istr;
}


inline void writeVarUInt(UInt64 x, WriteBuffer & ostr)
{
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
