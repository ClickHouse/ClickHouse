#pragma once
#include <string>


/// Maps 0..15 to 0..9A..F or 0..9a..f correspondingly.

extern const char * const hex_digit_to_char_uppercase_table;
extern const char * const hex_digit_to_char_lowercase_table;

inline char hexDigitUppercase(unsigned char c)
{
    return hex_digit_to_char_uppercase_table[c];
}

inline char hexDigitLowercase(unsigned char c)
{
    return hex_digit_to_char_lowercase_table[c];
}


#include <cstring>
#include <cstddef>

#include <common/types.h>


/// Maps 0..255 to 00..FF or 00..ff correspondingly

extern const char * const hex_byte_to_char_uppercase_table;
extern const char * const hex_byte_to_char_lowercase_table;

inline void writeHexByteUppercase(UInt8 byte, void * out)
{
    memcpy(out, &hex_byte_to_char_uppercase_table[static_cast<size_t>(byte) * 2], 2);
}

inline void writeHexByteLowercase(UInt8 byte, void * out)
{
    memcpy(out, &hex_byte_to_char_lowercase_table[static_cast<size_t>(byte) * 2], 2);
}

extern const char * const bin_byte_to_char_table;

inline void writeBinByte(UInt8 byte, void * out)
{
    memcpy(out, &bin_byte_to_char_table[static_cast<size_t>(byte) * 8], 8);
}

/// Produces hex representation of an unsigned int with leading zeros (for checksums)
template <typename TUInt>
inline void writeHexUIntImpl(TUInt uint_, char * out, const char * const table)
{
    union
    {
        TUInt value;
        UInt8 uint8[sizeof(TUInt)];
    };

    value = uint_;

    /// Use little endian
    for (size_t i = 0; i < sizeof(TUInt); ++i)
        memcpy(out + i * 2, &table[static_cast<size_t>(uint8[sizeof(TUInt) - 1 - i]) * 2], 2);
}

template <typename TUInt>
inline void writeHexUIntUppercase(TUInt uint_, char * out)
{
    writeHexUIntImpl(uint_, out, hex_byte_to_char_uppercase_table);
}

template <typename TUInt>
inline void writeHexUIntLowercase(TUInt uint_, char * out)
{
    writeHexUIntImpl(uint_, out, hex_byte_to_char_lowercase_table);
}

template <typename TUInt>
std::string getHexUIntUppercase(TUInt uint_)
{
    std::string res(sizeof(TUInt) * 2, '\0');
    writeHexUIntUppercase(uint_, res.data());
    return res;
}

template <typename TUInt>
std::string getHexUIntLowercase(TUInt uint_)
{
    std::string res(sizeof(TUInt) * 2, '\0');
    writeHexUIntLowercase(uint_, res.data());
    return res;
}


/// Maps 0..9, A..F, a..f to 0..15. Other chars are mapped to implementation specific value.

extern const char * const hex_char_to_digit_table;

inline UInt8 unhex(char c)
{
    return hex_char_to_digit_table[static_cast<UInt8>(c)];
}

inline UInt8 unhex2(const char * data)
{
    return
          static_cast<UInt8>(unhex(data[0])) * 0x10
        + static_cast<UInt8>(unhex(data[1]));
}

inline UInt16 unhex4(const char * data)
{
    return
          static_cast<UInt16>(unhex(data[0])) * 0x1000
        + static_cast<UInt16>(unhex(data[1])) * 0x100
        + static_cast<UInt16>(unhex(data[2])) * 0x10
        + static_cast<UInt16>(unhex(data[3]));
}
