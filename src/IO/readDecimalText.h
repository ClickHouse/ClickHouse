#pragma once

#include <limits>
#include <IO/ReadHelpers.h>
#include <Common/intExp.h>
#include <base/wide_integer_to_string.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

/// Try to read Decimal into underlying type T from ReadBuffer. Throws if 'digits_only' is set and there's unexpected symbol in input.
/// Returns integer 'exponent' factor that x should be multiplied by to get correct Decimal value: result = x * 10^exponent.
/// Use 'digits' input as max allowed meaning decimal digits in result. Place actual number of meaning digits in 'digits' output.
/// Does not care about decimal scale, only about meaningful digits in decimal text representation.
template <bool _throw_on_error, typename T>
inline bool readDigits(ReadBuffer & buf, T & x, uint32_t & digits, int32_t & exponent, bool digits_only = false)
{
    x = T(0);
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

    switch (*buf.position()) /// NOLINT(bugprone-switch-missing-default-case)
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

                    if constexpr (_throw_on_error)
                        throw Exception(
                            ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Too many digits ({} > {}) in decimal value",
                            std::to_string(digits + places),
                            std::to_string(max_digits));

                    return false;
                }

                digits += places;
                if (after_point)
                    exponent -= places;

                // TODO: accurate shift10 for big integers
                x *= intExp10OfSize<typename T::NativeType>(places);
                places = 0;

                x += (byte - '0');
                break;
            }
            case 'e': [[fallthrough]];
            case 'E':
            {
                ++buf.position();
                Int32 addition_exp = 0;
                if (!tryReadIntText(addition_exp, buf))
                {
                    if constexpr (_throw_on_error)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Cannot parse exponent while reading decimal");
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
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected symbol while reading decimal");
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

template <typename T, typename ReturnType=void>
inline ReturnType readDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale, bool digits_only = false)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    uint32_t digits = precision;
    int32_t exponent;
    auto ok = readDigits<throw_exception>(buf, x, digits, exponent, digits_only);

    if (!throw_exception && !ok)
        return ReturnType(false);

    if (static_cast<int32_t>(digits) + exponent > static_cast<int32_t>(precision - scale))
    {
        if constexpr (throw_exception)
        {
            static constexpr auto pattern = "Decimal value is too big: {} digits were read: {}e{}."
                                                    " Expected to read decimal with scale {} and precision {}";

            if constexpr (is_big_int_v<typename T::NativeType>)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, pattern, digits, x.value, exponent, scale, precision);
            else
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, pattern, digits, x, exponent, scale, precision);
        }
        else
            return ReturnType(false);
    }

    if (static_cast<int32_t>(scale) + exponent < 0)
    {
        auto divisor_exp = -exponent - static_cast<int32_t>(scale);

        if (divisor_exp >= std::numeric_limits<typename T::NativeType>::digits10)
        {
            /// Too big negative exponent
            x.value = 0;
            scale = 0;
            return ReturnType(true);
        }

        /// Too many digits after point. Just cut off excessive digits.
        auto divisor = intExp10OfSize<typename T::NativeType>(divisor_exp);
        assert(divisor > 0); /// This is for Clang Static Analyzer. It is not smart enough to infer it automatically.
        x.value /= divisor;
        scale = 0;
        return ReturnType(true);
    }

    scale += exponent;
    return ReturnType(true);
}

template <typename T>
inline bool tryReadDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale)
{
    return readDecimalText<T, bool>(buf, x, precision, scale, true);
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

template <typename T>
inline bool tryReadCSVDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale)
{
    if (buf.eof())
        return false;

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    if (!tryReadDecimalText(buf, x, precision, scale))
        return false;

    if ((maybe_quote == '\'' || maybe_quote == '\"') && !checkChar(maybe_quote, buf))
        return false;

    return true;
}

}
