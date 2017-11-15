#pragma once


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

#include <common/Types.h>


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


/// Maps 0..9, A..F, a..f to 0..15. Other chars are mapped to implementation specific value.

extern const char * const hex_char_to_digit_table;

inline char unhex(char c)
{
    return hex_char_to_digit_table[static_cast<UInt8>(c)];
}

inline char unhex2(const char * data)
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
