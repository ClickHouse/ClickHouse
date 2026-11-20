#pragma once

#include <Common/FieldVisitorToString.h>
#include <Common/intExp.h>
#include <base/arithmeticOverflow.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_NUMBER;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

/// Sets `x = x * 10^power + addend` and returns false if the result does not fit `N`. `addend` must be zero or
/// have the sign of `x`, or `x` must be zero. Exact for the wide integer types too, unlike `common::mulOverflow`,
/// which never reports overflow for them.
template <typename N>
inline bool mulPow10AddChecked(N & x, uint32_t power, N addend)
{
    if (x == 0)
    {
        x = addend;
        return true;
    }
    /// A nonzero `x` times `10^(digits10 + 1)` does not fit.
    if (power > static_cast<uint32_t>(std::numeric_limits<N>::digits10))
        return false;
    const N multiplier = intExp10OfSize<N>(static_cast<int>(power));
    if (x > 0 ? x > (std::numeric_limits<N>::max() - addend) / multiplier
              : x < (std::numeric_limits<N>::min() - addend) / multiplier)
        return false;
    x = x * multiplier + addend;
    return true;
}

/// Try to read Decimal into underlying type T from ReadBuffer. Throws if 'digits_only' is set and there's unexpected symbol in input.
/// Returns integer 'exponent' factor that x should be multiplied by to get correct Decimal value: result = x * 10^exponent.
/// Use 'digits' input as max allowed meaning decimal digits in result. Place actual number of meaning digits in 'digits' output.
/// Does not care about decimal scale, only about meaningful digits in decimal text representation.
/// When 'has_digits' is provided, it is set to whether at least one digit character was read. This lets a
/// caller with 'digits_only = false' tell a real number (including a bare zero) apart from a token with no
/// digits at all, such as ".", "-" or "e9", which is otherwise consumed as zero.
template <bool _throw_on_error, typename T>
inline bool readDigits(ReadBuffer & buf, T & x, uint32_t & digits, int32_t & exponent, bool digits_only = false, bool * has_digits = nullptr)
{
    x = T(0);
    exponent = 0;
    uint32_t max_digits = digits;
    digits = 0;
    uint32_t places = 0;
    typename T::NativeType sign = 1;
    /// When more digits are allowed than the native integer always holds (`Field` dumps allow any number), the
    /// mantissa is accumulated with its sign and checked, so it does not wrap and keeps the full native range.
    const bool check_overflow = max_digits > static_cast<uint32_t>(std::numeric_limits<typename T::NativeType>::digits10);
    bool leading_zeroes = true;
    bool after_point = false;

    if (has_digits)
        *has_digits = false;

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
        if (has_digits && byte >= '0' && byte <= '9')
            *has_digits = true;
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

                if (check_overflow)
                {
                    if (!mulPow10AddChecked(x.value, places, sign * static_cast<typename T::NativeType>(byte - '0')))
                    {
                        if constexpr (_throw_on_error)
                            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Decimal value does not fit its native integer type");
                        return false;
                    }
                }
                else
                {
                    x *= intExp10OfSize<typename T::NativeType>(places);
                    x += (byte - '0');
                }
                places = 0;
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
                /// The exponent comes from the text, so the sum can be anything, e.g. `0.1e-2147483648`.
                if (common::addOverflow(exponent, addition_exp, exponent))
                {
                    if constexpr (_throw_on_error)
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Exponent is out of range while reading decimal");
                    else
                        return false;
                }
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

    if (!check_overflow)
        x *= sign;
    return true;
}

template <typename T, typename ReturnType=void>
inline ReturnType readDecimalText(ReadBuffer & buf, T & x, uint32_t precision, uint32_t & scale, bool digits_only = false, bool * has_digits = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    uint32_t digits = precision;
    int32_t exponent = 0;
    auto ok = readDigits<throw_exception>(buf, x, digits, exponent, digits_only, has_digits);

    if (!throw_exception && !ok)
        return ReturnType(false);

    /// Compute in 64 bits: the exponent can be close to the `int32_t` limits.
    if (static_cast<int64_t>(digits) + exponent > static_cast<int64_t>(precision) - static_cast<int64_t>(scale))
    {
        if constexpr (throw_exception)
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Decimal value is too big: {} digits were read: {}e{}. Expected to read decimal with scale {} and precision {}",
                digits, convertFieldToString(x), exponent, scale, precision);
        }
        else
            return ReturnType(false);
    }

    if (static_cast<int64_t>(scale) + exponent < 0)
    {
        int64_t divisor_exp = -static_cast<int64_t>(exponent) - static_cast<int64_t>(scale);

        if (divisor_exp >= std::numeric_limits<typename T::NativeType>::digits10)
        {
            /// Too big negative exponent
            x.value = 0;
            scale = 0;
            return ReturnType(true);
        }

        /// Too many digits after point. Just cut off excessive digits.
        auto divisor = intExp10OfSize<typename T::NativeType>(static_cast<int>(divisor_exp));
        chassert(divisor > 0); /// This is for Clang Static Analyzer. It is not smart enough to infer it automatically.
        x.value /= divisor;  /// NOLINT(clang-analyzer-core.DivideZero)
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
