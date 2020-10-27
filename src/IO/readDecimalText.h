#pragma once

#include <IO/ReadHelpers.h>
#include <Common/intExp.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

/// Try to read Decimal into underlying type T from ReadBuffer. Throws if 'digits_only' is set and there's unexpected symbol in input.
/// Returns integer 'exponent' factor that x should be muntiplyed by to get correct Decimal value: result = x * 10^exponent.
/// Use 'digits' input as max allowed meaning decimal digits in result. Place actual number of meaning digits in 'digits' output.
/// Do not care about decimal scale, only about meaning digits in decimal text representation.
template <bool _throw_on_error, typename T>
inline bool readDigits(ReadBuffer & buf, T & x, uint32_t & digits, int32_t & exponent, bool digits_only = false)
{
    x = 0;
    exponent = 0;
    uint32_t max_digits = digits;
    digits = 0;
    uint32_t places = 0;
    typename T::NativeType sign = 1;
    bool leading_zeroes = true;
    bool after_point = false;

    if (buf.eof())
    {
        if constexpr (_throw_on_error)
            throwReadAfterEOF();
        return false;
    }

    switch (*buf.position())
    {
        case '-':
            sign = -1;
            [[fallthrough]];
        case '+':
            ++buf.position();
            break;
    }

    bool stop = false;
    while (!buf.eof() && !stop)
    {
        const char & byte = *buf.position();
        switch (byte)
        {
            case '.':
                after_point = true;
                leading_zeroes = false;
                break;
            case '0':
            {
                if (leading_zeroes)
                    break;

                if (after_point)
                {
                    ++places; /// Count trailing zeroes. They would be used only if there's some other digit after them.
                    break;
                }
                [[fallthrough]];
            }
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
                leading_zeroes = false;

                ++places; // num zeroes before + current digit
                if (digits + places > max_digits)
                {
                    if (after_point)
                    {
                        /// Simply cut excessive digits.
                        break;
                    }
                    else
                    {
                        if constexpr (_throw_on_error)
                            throw Exception("Too many digits (" + std::to_string(digits + places) + " > " + std::to_string(max_digits)
                                + ") in decimal value", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

                        return false;
                    }
                }
                else
                {
                    digits += places;
                    if (after_point)
                        exponent -= places;

                    // TODO: accurate shift10 for big integers
                    x *= intExp10OfSize<T>(places);
                    places = 0;

                    x += (byte - '0');
                    break;
                }
            }
            case 'e': [[fallthrough]];
            case 'E':
            {
                ++buf.position();
                Int32 addition_exp = 0;
                if (!tryReadIntText(addition_exp, buf))
                {
                    if constexpr (_throw_on_error)
                        throw Exception("Cannot parse exponent while reading decimal", ErrorCodes::CANNOT_PARSE_NUMBER);
                    else
                        return false;
                }
                exponent += addition_exp;
                stop = true;
                continue;
            }

            default:
                if (digits_only)
                {
                    if constexpr (_throw_on_error)
                        throw Exception("Unexpected symbol while reading decimal", ErrorCodes::CANNOT_PARSE_NUMBER);
                    return false;
                }
                stop = true;
                continue;
        }
        ++buf.position();
    }

    x *= sign;
    return true;
}

template <typename T>
inline void readDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale, bool digits_only = false)
{
    uint32_t digits = precision;
    int32_t exponent;
    readDigits<true>(buf, x, digits, exponent, digits_only);

    if (static_cast<int32_t>(digits) + exponent > static_cast<int32_t>(precision - scale))
        throw Exception(fmt::format(
            "Decimal value is too big: {} digits were read: {}e{}."
            " Expected to read decimal with scale {} and precision {}",
            digits, x, exponent, scale, precision), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    if (static_cast<int32_t>(scale) + exponent < 0)
    {
        /// Too many digits after point. Just cut off excessive digits.
        auto divisor = intExp10OfSize<T>(-exponent - static_cast<int32_t>(scale));
        assert(divisor > 0);    /// This is for Clang Static Analyzer. It is not smart enough to infer it automatically.
        x.value /= divisor;
        scale = 0;
        return;
    }

    scale += exponent;
}

template <typename T>
inline bool tryReadDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale)
{
    uint32_t digits = precision;
    int32_t exponent;

    if (!readDigits<false>(buf, x, digits, exponent, true) ||
        static_cast<int32_t>(digits) + exponent > static_cast<int32_t>(precision - scale) ||
        static_cast<int32_t>(scale) + exponent < 0)
        return false;

    scale += exponent;
    return true;
}

template <typename T>
inline void readCSVDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readDecimalText(buf, x, precision, scale, false);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

}
