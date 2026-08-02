#pragma once

#include <algorithm>
#include <string>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <base/types.h>


namespace impl
{
    bool startsWith(const std::string & s, const char * prefix, size_t prefix_size);
    bool endsWith(const std::string & s, const char * suffix, size_t suffix_size);
}


inline bool startsWith(const std::string & s, const std::string & prefix)
{
    return impl::startsWith(s, prefix.data(), prefix.size());
}

inline bool endsWith(const std::string & s, const std::string & suffix)
{
    return impl::endsWith(s, suffix.data(), suffix.size());
}


/// With GCC, strlen is evaluated compile time if we pass it a constant
/// string that is known at compile time.
inline bool startsWith(const std::string & s, const char * prefix)
{
    return impl::startsWith(s, prefix, strlen(prefix));
}

inline bool endsWith(const std::string & s, const char * suffix)
{
    return impl::endsWith(s, suffix, strlen(suffix));
}

/// Given an integer, return the adequate suffix for
/// printing an ordinal number.
template <typename T>
std::string getOrdinalSuffix(T n)
{
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>,
        "Unsigned integer value required");

    const auto last_digit = n % 10;

    if ((last_digit < 1 || last_digit > 3)
        || ((n > 10) && (((n / 10) % 10) == 1)))
        return "th";

    switch (last_digit)
    {
        case 1: return "st";
        case 2: return "nd";
        case 3: return "rd";
        default: return "th";
    }
}

/// More efficient than libc, because doesn't respect locale. But for some functions table implementation could be better.

inline bool isASCII(char c)
{
    return static_cast<unsigned char>(c) < 0x80;
}

inline bool isLowerAlphaASCII(char c)
{
    return (c >= 'a' && c <= 'z');
}

inline bool isUpperAlphaASCII(char c)
{
    return (c >= 'A' && c <= 'Z');
}

inline bool isAlphaASCII(char c)
{
    return isLowerAlphaASCII(c) || isUpperAlphaASCII(c);
}

inline bool isNumericASCII(char c)
{
    /// This is faster than
    /// return UInt8(UInt8(c) - UInt8('0')) < UInt8(10);
    /// on Intel CPUs when compiled by gcc 8.
    return (c >= '0' && c <= '9');
}

inline bool isHexDigit(char c)
{
    return isNumericASCII(c)
        || (c >= 'a' && c <= 'f')
        || (c >= 'A' && c <= 'F');
}

inline bool isAlphaNumericASCII(char c)
{
    return isAlphaASCII(c)
        || isNumericASCII(c);
}

inline bool isWordCharASCII(char c)
{
    return isAlphaNumericASCII(c)
        || c == '_';
}

inline bool isValidIdentifierBegin(char c)
{
    return isAlphaASCII(c)
        || c == '_';
}

inline bool isWhitespaceASCII(char c)
{
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/// Since |isWhiteSpaceASCII()| is used inside algorithms it's easier to implement another function than add extra argument.
inline bool isWhitespaceASCIIOneLine(char c)
{
    return c == ' ' || c == '\t' || c == '\f' || c == '\v';
}

inline bool isControlASCII(char c)
{
    return static_cast<unsigned char>(c) <= 31;
}

inline bool isPrintableASCII(char c)
{
    uint8_t uc = c;
    return uc >= 32 && uc <= 126;   /// 127 is ASCII DEL.
}

inline bool isCSIParameterByte(char c)
{
    uint8_t uc = c;
    return uc >= 0x30 && uc <= 0x3F; /// ASCII 0–9:;<=>?
}

inline bool isCSIIntermediateByte(char c)
{
    uint8_t uc = c;
    return uc >= 0x20 && uc <= 0x2F; /// ASCII !"#$%&'()*+,-./
}

inline bool isCSIFinalByte(char c)
{
    uint8_t uc = c;
    return uc >= 0x40 && uc <= 0x7E; /// ASCII @A–Z[\]^_`a–z{|}~
}

inline bool isPunctuationASCII(char c)
{
    uint8_t uc = c;
    return (uc >= 33 && uc <= 47)
        || (uc >= 58 && uc <= 64)
        || (uc >= 91 && uc <= 96)
        || (uc >= 123 && uc <= 125);
}


inline bool isNumberSeparator(bool is_start_of_block, bool is_hex, const char * pos, const char * end)
{
    if (*pos != '_')
        return false;
    if (is_start_of_block && *pos == '_')
        return false; // e.g. _123, 12e_3
    if (pos + 1 < end && !(is_hex ? isHexDigit(pos[1]) : isNumericASCII(pos[1])))
        return false; // e.g. 1__2, 1_., 1_e, 1_p, 1_;
    if (pos + 1 == end)
        return false; // e.g. 12_
    return true;
}

/// Works assuming isAlphaASCII.
inline char toLowerIfAlphaASCII(char c)
{
    return c | 0x20;
}

inline char toUpperIfAlphaASCII(char c)
{
    return c & (~0x20);
}

inline char alternateCaseIfAlphaASCII(char c)
{
    return c ^ 0x20;
}

inline const char * skipWhitespacesUTF8(const char * pos, const char * end)
{
    /// https://en.wikipedia.org/wiki/Whitespace_character
    /// with some adjustments.

    /// Code points: 0085 00A0 180E 2000..200A 2028..2029 200B..200D 202F 205F 2060 3000 FEFF
    /// The corresponding UTF-8 is: C285 C2A0 E1A08E E28080..E2808A E280A8..E280A9 E2808B..E2808D E280AF E2819F E281A0 E38080 EFBBBF

    /// We check for these bytes directly in UTF8 for simplicity reasons.

    /** C2
      *    85
      *    A0
      * E1 A0 8E
      * E2
      *    80
      *       80..8A
      *       A8..A9
      *       8B..8D
      *       AF
      *    81
      *       9F
      *       A0
      * E3 80 80
      * EF BB BF
      */

    while (pos < end)
    {
        if (isWhitespaceASCII(*pos))
        {
            ++pos;
        }
        else
        {
            const uint8_t * upos = reinterpret_cast<const uint8_t *>(pos);

            if (pos + 1 < end && upos[0] == 0xC2 && (upos[1] == 0x85 || upos[1] == 0xA0))
            {
                pos += 2;
            }
            else if (pos + 2 < end
                &&    ((upos[0] == 0xE1 && upos[1] == 0xA0 && upos[2] == 0x8E)
                    || (upos[0] == 0xE2
                        &&    ((upos[1] == 0x80
                            &&    ((upos[2] >= 0x80 && upos[2] <= 0x8A)
                                || (upos[2] >= 0xA8 && upos[2] <= 0xA9)
                                || (upos[2] >= 0x8B && upos[2] <= 0x8D)
                                || (upos[2] == 0xAF)))
                            || (upos[1] == 0x81 && (upos[2] == 0x9F || upos[2] == 0xA0))))
                    || (upos[0] == 0xE3 && upos[1] == 0x80 && upos[2] == 0x80)
                    || (upos[0] == 0xEF && upos[1] == 0xBB && upos[2] == 0xBF)))
            {
                pos += 3;
            }
            else
                break;
        }
    }

    return pos;
}

inline bool equalsCaseInsensitive(char a, char b)
{
    return a == b || (isAlphaASCII(a) && alternateCaseIfAlphaASCII(a) == b);
}


template <typename F>
std::string trim(const std::string & str, F && predicate)
{
    size_t cut_front = 0;
    size_t cut_back = 0;
    size_t size = str.size();

    for (size_t i = 0; i < size; ++i)
    {
        if (predicate(str[i]))
            ++cut_front;
        else
            break;
    }

    if (cut_front == size)
        return {};

    for (auto it = str.rbegin(); it != str.rend(); ++it)
    {
        if (predicate(*it))
            ++cut_back;
        else
            break;
    }

    return str.substr(cut_front, size - cut_front - cut_back);
}

inline void trimLeft(std::string_view & str, char c = ' ')
{
    while (str.starts_with(c))
        str.remove_prefix(1);
}

inline void trimLeft(std::string & str, char c = ' ')
{
    str.erase(0, str.find_first_not_of(c));
}

inline void trimRight(std::string_view & str, char c = ' ')
{
    while (str.ends_with(c))
        str.remove_suffix(1);
}

inline void trimRight(std::string & str, char c = ' ')
{
    str.erase(str.find_last_not_of(c) + 1);
}

inline void trim(std::string_view & str, char c = ' ')
{
    trimLeft(str, c);
    trimRight(str, c);
}

inline void trim(std::string & str, char c = ' ')
{
    trimRight(str, c);
    trimLeft(str, c);
}

/// If all characters in the string are ASCII, return true
bool isAllASCII(const UInt8 * data, size_t size);

constexpr bool containsGlobs(const std::string & str)
{
    return str.find_first_of("*?{") != std::string::npos;
}

inline bool isValidIdentifier(std::string_view str)
{
    return !str.empty()
        && isValidIdentifierBegin(str[0])
        && std::all_of(str.begin() + 1, str.end(), isWordCharASCII)
        /// NULL is not a valid identifier in SQL, any case.
        && !(str.size() == strlen("null")
            && toLowerIfAlphaASCII(str[0]) == 'n'
            && toLowerIfAlphaASCII(str[1]) == 'u'
            && toLowerIfAlphaASCII(str[2]) == 'l'
            && toLowerIfAlphaASCII(str[3]) == 'l');
}
