#pragma once
#include <type_traits>
#include <IO/ReadHelpers.h>
#include <Core/Defines.h>
#include <base/shift10.h>
#include <Common/StringUtils/StringUtils.h>
#include <double-conversion/double-conversion.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif
#include <fast_float/fast_float.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

/** Methods for reading floating point numbers from text with decimal representation.
  * There are "precise", "fast" and "simple" implementations.
  *
  * Neither of methods support hexadecimal numbers (0xABC), binary exponent (1p100), leading plus sign.
  *
  * Precise method always returns a number that is the closest machine representable number to the input.
  *
  * Fast method is faster (up to 3 times) and usually return the same value,
  *  but in rare cases result may differ by lest significant bit (for Float32)
  *  and by up to two least significant bits (for Float64) from precise method.
  * Also fast method may parse some garbage as some other unspecified garbage.
  *
  * Simple method is little faster for cases of parsing short (few digit) integers, but less precise and slower in other cases.
  * It's not recommended to use simple method and it is left only for reference.
  *
  * For performance test, look at 'read_float_perf' test.
  *
  * For precision test.
  * Parse all existing Float32 numbers:

CREATE TABLE test.floats ENGINE = Log AS SELECT reinterpretAsFloat32(reinterpretAsString(toUInt32(number))) AS x FROM numbers(0x100000000);

WITH
    toFloat32(toString(x)) AS y,
    reinterpretAsUInt32(reinterpretAsString(x)) AS bin_x,
    reinterpretAsUInt32(reinterpretAsString(y)) AS bin_y,
    abs(bin_x - bin_y) AS diff
SELECT
    diff,
    count()
FROM test.floats
WHERE NOT isNaN(x)
GROUP BY diff
ORDER BY diff ASC
LIMIT 100

  * Here are the results:
  *
    Precise:
    ┌─diff─┬────count()─┐
    │    0 │ 4278190082 │
    └──────┴────────────┘
    (100% roundtrip property)

    Fast:
    ┌─diff─┬────count()─┐
    │    0 │ 3685260580 │
    │    1 │  592929502 │
    └──────┴────────────┘
    (The difference is 1 in least significant bit in 13.8% of numbers.)

    Simple:
    ┌─diff─┬────count()─┐
    │    0 │ 2169879994 │
    │    1 │ 1807178292 │
    │    2 │  269505944 │
    │    3 │   28826966 │
    │    4 │    2566488 │
    │    5 │     212878 │
    │    6 │      18276 │
    │    7 │       1214 │
    │    8 │         30 │
    └──────┴────────────┘

  * Parse random Float64 numbers:

WITH
    rand64() AS bin_x,
    reinterpretAsFloat64(reinterpretAsString(bin_x)) AS x,
    toFloat64(toString(x)) AS y,
    reinterpretAsUInt64(reinterpretAsString(y)) AS bin_y,
    abs(bin_x - bin_y) AS diff
SELECT
    diff,
    count()
FROM numbers(100000000)
WHERE NOT isNaN(x)
GROUP BY diff
ORDER BY diff ASC
LIMIT 100

  */


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
}


/// Returns true, iff parsed.
bool parseInfinity(ReadBuffer & buf);
bool parseNaN(ReadBuffer & buf);

void assertInfinity(ReadBuffer & buf);
void assertNaN(ReadBuffer & buf);


template <bool throw_exception>
bool assertOrParseInfinity(ReadBuffer & buf)
{
    if constexpr (throw_exception)
    {
        assertInfinity(buf);
        return true;
    }
    else
        return parseInfinity(buf);
}

template <bool throw_exception>
bool assertOrParseNaN(ReadBuffer & buf)
{
    if constexpr (throw_exception)
    {
        assertNaN(buf);
        return true;
    }
    else
        return parseNaN(buf);
}


template <typename T, typename ReturnType>
ReturnType readFloatTextPreciseImpl(T & x, ReadBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextFastFloatImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII"); //-V590

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// Fast path (avoid copying) if the buffer have at least MAX_LENGTH bytes.
    static constexpr int MAX_LENGTH = 316;

    if (likely(!buf.eof() && buf.position() + MAX_LENGTH <= buf.buffer().end()))
    {
        auto * initial_position = buf.position();
        auto res = fast_float::from_chars(initial_position, buf.buffer().end(), x);

        if (unlikely(res.ec != std::errc()))
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return ReturnType(false);
        }

        buf.position() += res.ptr - initial_position;

        return ReturnType(true);
    }
    else
    {
        /// Slow path. Copy characters that may be present in floating point number to temporary buffer.
        bool negative = false;

        /// We check eof here because we can parse +inf +nan
        while (!buf.eof())
        {
            switch (*buf.position())
            {
                case '+':
                    ++buf.position();
                    continue;

                case '-':
                {
                    negative = true;
                    ++buf.position();
                    continue;
                }

                case 'i': [[fallthrough]];
                case 'I':
                {
                    if (assertOrParseInfinity<throw_exception>(buf))
                    {
                        x = std::numeric_limits<T>::infinity();
                        if (negative)
                            x = -x;
                        return ReturnType(true);
                    }
                    return ReturnType(false);
                }

                case 'n': [[fallthrough]];
                case 'N':
                {
                    if (assertOrParseNaN<throw_exception>(buf))
                    {
                        x = std::numeric_limits<T>::quiet_NaN();
                        if (negative)
                            x = -x;
                        return ReturnType(true);
                    }
                    return ReturnType(false);
                }

                default:
                    break;
            }

            break;
        }


        char tmp_buf[MAX_LENGTH];
        int num_copied_chars = 0;

        while (!buf.eof() && num_copied_chars < MAX_LENGTH)
        {
            char c = *buf.position();
            if (!(isNumericASCII(c) || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E'))
                break;

            tmp_buf[num_copied_chars] = c;
            ++buf.position();
            ++num_copied_chars;
        }

        auto res = fast_float::from_chars(tmp_buf, tmp_buf + num_copied_chars, x);

        if (unlikely(res.ec != std::errc()))
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return ReturnType(false);
        }

        if (negative)
            x = -x;

        return ReturnType(true);
    }
}


// credit: https://johnnylee-sde.github.io/Fast-numeric-string-to-int/
static inline bool is_made_of_eight_digits_fast(uint64_t val) noexcept
{
    return (((val & 0xF0F0F0F0F0F0F0F0) | (((val + 0x0606060606060606) & 0xF0F0F0F0F0F0F0F0) >> 4)) == 0x3333333333333333);
}

static inline bool is_made_of_eight_digits_fast(const char * chars) noexcept
{
    uint64_t val;
    ::memcpy(&val, chars, 8);
    return is_made_of_eight_digits_fast(val);
}

template <size_t N, typename T>
static inline void readUIntTextUpToNSignificantDigits(T & x, ReadBuffer & buf)
{
    /// In optimistic case we can skip bound checking for first loop.
    if (buf.position() + N <= buf.buffer().end())
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                return;
        }
    }
    else
    {
        for (size_t i = 0; i < N; ++i)
        {
            if (!buf.eof() && isNumericASCII(*buf.position()))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
                ++buf.position();
            }
            else
                return;
        }
    }

    while (!buf.eof() && (buf.position() + 8 <= buf.buffer().end()) &&
         is_made_of_eight_digits_fast(buf.position()))
    {
        buf.position() += 8;
    }

    while (!buf.eof() && isNumericASCII(*buf.position()))
        ++buf.position();
}


template <typename T, typename ReturnType>
ReturnType readFloatTextFastImpl(T & x, ReadBuffer & in)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII"); //-V590

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    x = 0;
    UInt64 before_point = 0;
    UInt64 after_point = 0;
    int after_point_exponent = 0;
    int exponent = 0;

    if (in.eof())
    {
        if constexpr (throw_exception)
            throw ParsingException("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
        else
            return false;
    }

    if (*in.position() == '-')
    {
        negative = true;
        ++in.position();
    }
    else if (*in.position() == '+')
        ++in.position();

    auto count_after_sign = in.count();

    constexpr int significant_digits = std::numeric_limits<UInt64>::digits10;
    readUIntTextUpToNSignificantDigits<significant_digits>(before_point, in);

    int read_digits = in.count() - count_after_sign;

    if (unlikely(read_digits > significant_digits))
    {
        int before_point_additional_exponent = read_digits - significant_digits;
        x = shift10(before_point, before_point_additional_exponent);
    }
    else
    {
        x = before_point;

        /// Shortcut for the common case when there is an integer that fit in Int64.
        if (read_digits && (in.eof() || *in.position() < '.'))
        {
            if (negative)
                x = -x;
            return ReturnType(true);
        }
    }

    if (checkChar('.', in))
    {
        auto after_point_count = in.count();

        while (!in.eof() && *in.position() == '0')
            ++in.position();

        auto after_leading_zeros_count = in.count();
        auto after_point_num_leading_zeros = after_leading_zeros_count - after_point_count;

        readUIntTextUpToNSignificantDigits<significant_digits>(after_point, in);
        read_digits = in.count() - after_leading_zeros_count;
        after_point_exponent = (read_digits > significant_digits ? -significant_digits : -read_digits) - after_point_num_leading_zeros;
    }

    if (checkChar('e', in) || checkChar('E', in))
    {
        if (in.eof())
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot read floating point value: nothing after exponent", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return false;
        }

        bool exponent_negative = false;
        if (*in.position() == '-')
        {
            exponent_negative = true;
            ++in.position();
        }
        else if (*in.position() == '+')
        {
            ++in.position();
        }

        readUIntTextUpToNSignificantDigits<4>(exponent, in);
        if (exponent_negative)
            exponent = -exponent;
    }

    if (after_point)
        x += shift10(after_point, after_point_exponent);

    if (exponent)
        x = shift10(x, exponent);

    if (negative)
        x = -x;

    auto num_characters_without_sign = in.count() - count_after_sign;

    /// Denormals. At most one character is read before denormal and it is '-'.
    if (num_characters_without_sign == 0)
    {
        if (in.eof())
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot read floating point value: no digits read", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return false;
        }

        if (*in.position() == '+')
        {
            ++in.position();
            if (in.eof())
            {
                if constexpr (throw_exception)
                    throw ParsingException("Cannot read floating point value: nothing after plus sign", ErrorCodes::CANNOT_PARSE_NUMBER);
                else
                    return false;
            }
            else if (negative)
            {
                if constexpr (throw_exception)
                    throw ParsingException("Cannot read floating point value: plus after minus sign", ErrorCodes::CANNOT_PARSE_NUMBER);
                else
                    return false;
            }
        }

        if (*in.position() == 'i' || *in.position() == 'I')
        {
            if (assertOrParseInfinity<throw_exception>(in))
            {
                x = std::numeric_limits<T>::infinity();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
        else if (*in.position() == 'n' || *in.position() == 'N')
        {
            if (assertOrParseNaN<throw_exception>(in))
            {
                x = std::numeric_limits<T>::quiet_NaN();
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
            return ReturnType(false);
        }
    }

    return ReturnType(true);
}

template <typename T, typename ReturnType>
ReturnType readFloatTextSimpleImpl(T & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    x = 0;
    bool after_point = false;
    double power_of_ten = 1;

    if (buf.eof())
        throwReadAfterEOF();

    while (!buf.eof())
    {
        switch (*buf.position())
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '.':
                after_point = true;
                break;
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
                if (after_point)
                {
                    power_of_ten /= 10;
                    x += (*buf.position() - '0') * power_of_ten;
                }
                else
                {
                    x *= 10;
                    x += *buf.position() - '0';
                }
                break;
            case 'e': [[fallthrough]];
            case 'E':
            {
                ++buf.position();
                Int32 exponent = 0;
                readIntText(exponent, buf);
                x = shift10(x, exponent);
                if (negative)
                    x = -x;
                return ReturnType(true);
            }

            case 'i': [[fallthrough]];
            case 'I':
            {
                if (assertOrParseInfinity<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::infinity();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            case 'n': [[fallthrough]];
            case 'N':
            {
                if (assertOrParseNaN<throw_exception>(buf))
                {
                    x = std::numeric_limits<T>::quiet_NaN();
                    if (negative)
                        x = -x;
                    return ReturnType(true);
                }
                return ReturnType(false);
            }

            default:
            {
                if (negative)
                    x = -x;
                return ReturnType(true);
            }
        }
        ++buf.position();
    }

    if (negative)
        x = -x;

    return ReturnType(true);
}

template <typename T> void readFloatTextPrecise(T & x, ReadBuffer & in) { readFloatTextPreciseImpl<T, void>(x, in); }
template <typename T> bool tryReadFloatTextPrecise(T & x, ReadBuffer & in) { return readFloatTextPreciseImpl<T, bool>(x, in); }

template <typename T> void readFloatTextFast(T & x, ReadBuffer & in) { readFloatTextFastImpl<T, void>(x, in); }
template <typename T> bool tryReadFloatTextFast(T & x, ReadBuffer & in) { return readFloatTextFastImpl<T, bool>(x, in); }

template <typename T> void readFloatTextSimple(T & x, ReadBuffer & in) { readFloatTextSimpleImpl<T, void>(x, in); }
template <typename T> bool tryReadFloatTextSimple(T & x, ReadBuffer & in) { return readFloatTextSimpleImpl<T, bool>(x, in); }


/// Implementation that is selected as default.

template <typename T> void readFloatText(T & x, ReadBuffer & in) { readFloatTextFast(x, in); }
template <typename T> bool tryReadFloatText(T & x, ReadBuffer & in) { return tryReadFloatTextFast(x, in); }

}
