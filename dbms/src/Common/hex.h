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

/// Maps 0..255 to 00..FF or 00..ff correspondingly

extern const char * const hex_byte_to_char_uppercase_table;
extern const char * const hex_byte_to_char_lowercase_table;

inline void writeHexByteUppercase(unsigned char byte, void * out)
{
    memcpy(out, &hex_byte_to_char_uppercase_table[static_cast<size_t>(byte) * 2], 2);
}

inline void writeHexByteLowercase(unsigned char byte, void * out)
{
    memcpy(out, &hex_byte_to_char_lowercase_table[static_cast<size_t>(byte) * 2], 2);
}


/// Maps 0..9, A..F, a..f to 0..15. Other chars are mapped to implementation specific value.

extern const char * const hex_char_to_digit_table;

inline char unhex(char c)
{
    return hex_char_to_digit_table[static_cast<unsigned char>(c)];
}
