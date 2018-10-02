#pragma once

#include <string>
#include <cstring>
#include <cstddef>
#include <type_traits>


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
    };
}

/// More efficient than libc, because doesn't respect locale. But for some functions table implementation could be better.

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

inline bool isControlASCII(char c)
{
    return static_cast<unsigned char>(c) <= 31;
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
