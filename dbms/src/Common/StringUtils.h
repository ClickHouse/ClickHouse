#pragma once

#include <Common/Exception.h>
#include <string>
#include <cstring>

namespace DB { namespace ErrorCodes {

extern const int LOGICAL_ERROR;

}}

namespace detail
{
    bool startsWith(const std::string & s, const char * prefix, size_t prefix_size);
    bool endsWith(const std::string & s, const char * suffix, size_t suffix_size);
}


inline bool startsWith(const std::string & s, const std::string & prefix)
{
    return detail::startsWith(s, prefix.data(), prefix.size());
}

inline bool endsWith(const std::string & s, const std::string & suffix)
{
    return detail::endsWith(s, suffix.data(), suffix.size());
}


/// With GCC, strlen is evaluated compile time if we pass it a constant
/// string that is known at compile time.
inline bool startsWith(const std::string & s, const char * prefix)
{
    return detail::startsWith(s, prefix, strlen(prefix));
}

inline bool endsWith(const std::string & s, const char * suffix)
{
    return detail::endsWith(s, suffix, strlen(suffix));
}

/// Given an integer, return the adequate suffix for
/// printing an ordinal number.
template <typename T>
std::string getOrdinalSuffix(T n)
{
    static_assert(std::is_integral<T>::value && std::is_unsigned<T>::value,
        "Unsigned integer value required");

    const auto val = n % 10;

    bool is_th;
    if ((val >= 1) && (val <= 3))
        is_th = (n > 10) && (((n / 10) % 10) == 1);
    else
        is_th = true;

    if (is_th)
        return "th";
    else
    {
        switch (val)
        {
            case 1: return "st";
            case 2: return "nd";
            case 3: return "rd";
            default: throw DB::Exception{"getOrdinalSuffix: internal error",
                DB::ErrorCodes::LOGICAL_ERROR};
        };
    }
}

/// More efficient than libc, because doesn't respect locale.

inline bool isASCII(char c)
{
    return static_cast<unsigned char>(c) < 0x80;
}

inline bool isAlphaASCII(char c)
{
    return (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z');
}

inline bool isNumericASCII(char c)
{
    return (c >= '0' && c <= '9');
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

inline bool equalsCaseInsensitive(char a, char b)
{
    return a == b || (isAlphaASCII(a) && alternateCaseIfAlphaASCII(a) == b);
}

