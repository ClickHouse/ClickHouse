#pragma once


/// Maps 0..15 to 0..9A..F or 0..9a..f correspondingly.

extern const char * const hex_digit_to_char_uppercase_table;
extern const char * const hex_digit_to_char_lowercase_table;

inline char hexUppercase(unsigned char c)
{
    return hex_digit_to_char_uppercase_table[c];
}

inline char hexLowercase(unsigned char c)
{
    return hex_digit_to_char_lowercase_table[c];
}


/// Maps 0..9, A..F, a..f to 0..15. Other chars are mapped to implementation specific value.

extern const char * const hex_char_to_digit_table;

inline char unhex(char c)
{
    return hex_char_to_digit_table[static_cast<unsigned char>(c)];
}
