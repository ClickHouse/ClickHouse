#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/Decimal_fwd.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
}

enum class ReadIntTextCheckOverflow : uint8_t
{
    DO_NOT_CHECK_OVERFLOW,
    CHECK_OVERFLOW,
};

void assertEOF(ReadBuffer & buf);
[[noreturn]] void throwReadAfterEOF();

template <int base, typename T, typename ReturnType, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW>
ReturnType readIntTextInBaseImpl(T & x, ReadBuffer & buf)
{
    using UnsignedT = make_unsigned_t<T>;
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    static_assert(2 <= base && base <= 16);

    bool negative = false;
    UnsignedT res{};
    if (buf.eof()) [[unlikely]]
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        else
            return ReturnType(false);
    }

    const size_t initial_pos = buf.count();
    bool has_sign = false;
    bool has_number = false;
    for (; !buf.eof(); ++buf.position())
    {
        char c = *buf.position();
        char digit = 0;
        switch (c)
        {
            case '+':
            {
                /// 123+ or +123+, just stop after 123 or +123.
                if (has_number)
                    goto end;

                /// No digits read yet, but we already read sign, like ++, -+.
                if (has_sign)
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number with multiple sign (+/-) characters");
                    else
                        return ReturnType(false);
                }

                has_sign = true;
                continue;
            }
            case '-':
            {
                if (has_number)
                    goto end;

                if (has_sign)
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number with multiple sign (+/-) characters");
                    else
                        return ReturnType(false);
                }

                if constexpr (is_signed_v<T>)
                    negative = true;
                else
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unsigned type must not contain '-' symbol");
                    else
                        return ReturnType(false);
                }
                has_sign = true;
                continue;
            }
            case '0': [[fallthrough]];
            case '1': [[fallthrough]];
            case '2': [[fallthrough]];
            case '3': [[fallthrough]];
            case '4': [[fallthrough]];
            case '5': [[fallthrough]];
            case '6': [[fallthrough]];
            case '7': [[fallthrough]];
            case '8': [[fallthrough]];
            case '9':
            {
                digit = c - '0';
                goto handle_digit;
            }
            case 'A': [[fallthrough]];
            case 'B': [[fallthrough]];
            case 'C': [[fallthrough]];
            case 'D': [[fallthrough]];
            case 'E': [[fallthrough]];
            case 'F':
            {
                if constexpr (base > 10)
                {
                    digit = c - ('A' - 10);
                    goto handle_digit;
                }
                else
                    goto end;
            }
            case 'a': [[fallthrough]];
            case 'b': [[fallthrough]];
            case 'c': [[fallthrough]];
            case 'd': [[fallthrough]];
            case 'e': [[fallthrough]];
            case 'f':
            {
                if constexpr (base > 10)
                {
                    digit = c - ('a' - 10);
                    goto handle_digit;
                }
                else
                    goto end;
            }
            default:
            {
                goto end;
            }

handle_digit:
            if constexpr (base != 10)
            {
                if (digit >= base)
                    goto end;
            }
            has_number = true;
            if constexpr (check_overflow == ReadIntTextCheckOverflow::CHECK_OVERFLOW && !is_big_int_v<T>)
            {
                /// Perform relativelly slow overflow check only when
                /// number of decimal digits so far is close to the max for given type.
                /// Example: 20 * 10 will overflow Int8.
                constexpr size_t max_digits = (base == 10) ? std::numeric_limits<T>::max_digits10 : (
                                              (base == 2) ? std::numeric_limits<T>::digits : (
                                              (base == 8) ? ((std::numeric_limits<T>::digits + 2) / 3) : (
                                              (base == 16) ? ((std::numeric_limits<T>::digits + 3) / 4) : 0)));

                if (buf.count() - initial_pos + 1 >= max_digits)
                {
                    if (negative)
                    {
                        T signed_res = static_cast<T>(-res);
                        if (common::mulOverflow<T>(signed_res, base, signed_res) ||
                            common::subOverflow<T>(signed_res, digit, signed_res))
                        {
                            if constexpr (throw_exception)
                                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Overflow while parsing a number");
                            else
                                return ReturnType(false);
                        }

                        res = static_cast<UnsignedT>(-static_cast<UnsignedT>(signed_res));
                    }
                    else
                    {
                        T signed_res = res;
                        if (common::mulOverflow<T>(signed_res, base, signed_res) ||
                            common::addOverflow<T>(signed_res, digit, signed_res))
                        {
                            if constexpr (throw_exception)
                                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Overflow while parsing a number");
                            else
                                return ReturnType(false);
                        }

                        res = signed_res;
                    }
                    break;
                }
            }
            res *= base;
            res += digit;
        }
    }

end:
    if (has_sign && !has_number)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number without any digits");
        else
            return ReturnType(false);
    }
    x = res;
    if constexpr (is_signed_v<T>)
    {
        if (negative)
        {
            if constexpr (check_overflow == ReadIntTextCheckOverflow::CHECK_OVERFLOW)
            {
                if (common::mulOverflow<UnsignedT, Int8, T>(res, -1, x))
                {
                    if constexpr (throw_exception)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Overflow while parsing a number");
                    else
                        return ReturnType(false);
                }
            }
            else
                x = static_cast<T>(-res);
        }
    }

    return ReturnType(true);
}

template <typename T, typename ReturnType, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW>
ReturnType readIntTextImpl(T & x, ReadBuffer & buf)
{
    return readIntTextInBaseImpl<10, T, ReturnType, check_overflow>(x, buf);
}


/// Parses an integer in a specific base (2 or 8 or 10 or 16).
template <int base, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW, typename T>
void readIntTextInBase(T & x, ReadBuffer & buf)
{
    if constexpr (is_decimal<T>)
        readIntTextInBase<base, check_overflow>(x.value, buf);
    else
        readIntTextInBaseImpl<base, T, void, check_overflow>(x, buf);
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW, typename T>
void readIntText(T & x, ReadBuffer & buf)
{
    readIntTextInBase<10, check_overflow>(x, buf);
}


/// Tries to parse an integer in a specific base (2 or 8 or 10 or 16), returns false if fails.
template <int base, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryReadIntTextInBase(T & x, ReadBuffer & buf)
{
    if constexpr (is_decimal<T>)
        return tryReadIntTextInBase<base, check_overflow>(x.value, buf);
    else
        return readIntTextInBaseImpl<base, T, bool, check_overflow>(x, buf);
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryReadIntText(T & x, ReadBuffer & buf)
{
    return tryReadIntTextInBase<10, check_overflow>(x, buf);
}


/// Reads a decimal integer into an `Int128`, saturating to the `Int128` range instead of wrapping:
/// `readIntText` skips the overflow check for big-int types, so a literal wider than `Int128` (39+ digits)
/// is silently accepted modulo 2^128. Overflow is detected with an explicit per-digit threshold check
/// (`common::mulOverflow` is a no-op stub for big-int types). The whole digit run is consumed and the
/// saturated value keeps its sign, so a later range check sees the value on the correct side of its bounds.
/// Unlike `readIntText`, a token without any digits (empty or a bare sign) is an error — an exception for
/// `ReturnType = void`, `false` for `bool` — so the raw-value path rejects a missing token such as `{"t":}`.
template <typename ReturnType = void>
ReturnType readIntText128Saturating(Int128 & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// `res * 10 + digit` overflows the non-negative accumulator iff `res > max / 10`, or
    /// `res == max / 10` and `digit > max % 10`.
    constexpr Int128 max_div_10 = std::numeric_limits<Int128>::max() / 10;
    constexpr Int128 max_mod_10 = std::numeric_limits<Int128>::max() % 10;

    bool negative = false;
    bool has_number = false;
    bool overflow = false;
    Int128 res = 0;

    if (!buf.eof() && (*buf.position() == '-' || *buf.position() == '+'))
    {
        negative = *buf.position() == '-';
        ++buf.position();
    }

    while (!buf.eof() && *buf.position() >= '0' && *buf.position() <= '9')
    {
        Int128 digit = *buf.position() - '0';
        ++buf.position();
        has_number = true;

        if (overflow)
            continue;
        if (res > max_div_10 || (res == max_div_10 && digit > max_mod_10))
            overflow = true;
        else
            res = res * 10 + digit;
    }

    if (!has_number)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse number without any digits");
        else
            return ReturnType(false);
    }

    if (overflow)
        x = negative ? std::numeric_limits<Int128>::min() : std::numeric_limits<Int128>::max();
    else
        x = negative ? -res : res;

    return ReturnType(true);
}


/// Parses an integer in a specific base (2 or 8 or 10 or 16).
template <int base, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
void parseIntInBase(T & x, std::string_view str)
{
    ReadBufferFromMemory buf{std::move(str)};
    readIntTextInBase<base, check_overflow>(x, buf);
    assertEOF(buf);
}

template <int base, typename T, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW>
T parseIntInBase(std::string_view str)
{
    T x;
    parseIntInBase<base, check_overflow>(x, str);
    return x;
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
void parseInt(T & x, std::string_view str)
{
    parseIntInBase<10, check_overflow>(x, str);
}

template <typename T, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW>
T parseInt(std::string_view str)
{
    T x;
    parseInt<check_overflow>(x, str);
    return x;
}

/// Tries to parse an integer in a specific base (2 or 8 or 10 or 16), returns false if fails.
template <int base, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryParseIntInBase(T & x, std::string_view str)
{
    ReadBufferFromMemory buf{std::move(str)};
    return tryReadIntTextInBase<base, check_overflow>(x, buf) && buf.eof();
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryParseInt(T & x, std::string_view str)
{
    return tryParseIntInBase<10>(x, str);
}


/** More efficient variant (about 1.5 times on real dataset).
  * Differs in following:
  * - for numbers starting with zero, parsed only zero;
  * - symbol '+' before number is not supported;
  */
template <typename T, typename ReturnType = void>
ReturnType readIntTextUnsafe(T & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    bool negative = false;
    make_unsigned_t<T> res = 0;

    auto on_error = []
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        return ReturnType(false);
    };

    if (buf.eof()) [[unlikely]]
        return on_error();

    if (is_signed_v<T> && *buf.position() == '-')
    {
        ++buf.position();
        negative = true;
        if (buf.eof()) [[unlikely]]
            return on_error();
    }

    if (*buf.position() == '0') /// There are many zeros in real datasets.
    {
        ++buf.position();
        x = 0;
        return ReturnType(true);
    }

    while (!buf.eof())
    {
        unsigned char value = *buf.position() - '0';

        if (value < 10)
        {
            res *= 10;
            res += value;
            ++buf.position();
        }
        else
            break;
    }

    /// See note about undefined behaviour above.
    x = static_cast<T>(is_signed_v<T> && negative ? -res : res);
    return ReturnType(true);
}

template <typename T>
bool tryReadIntTextUnsafe(T & x, ReadBuffer & buf)
{
    return readIntTextUnsafe<T, bool>(x, buf);
}

}
