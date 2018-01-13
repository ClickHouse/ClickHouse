#include <type_traits>
#include <IO/ReadHelpers.h>
#include <common/shift10.h>
#include <double-conversion/double-conversion.h>


/** Methods for reading floating point numbers from text with decimal representation.
  * There are "precise", "fast" and "simple" implementations.
  * Precise method always returns a number that is the closest machine representable number to the input.
  * Fast method is faster and usually return the same value, but result may differ from precise method.
  * Simple method is even faster for cases of parsing short (few digit) integers, but less precise and slower in other cases.
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


/// Some garbage may be successfully parsed, examples: '--1' parsed as '1'.
template <typename T, typename ReturnType>
ReturnType readFloatTextPreciseImpl(T & x, ReadBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (buf.eof())
    {
        if constexpr (throw_exception)
            throw Exception("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
        else
            return ReturnType(false);
    }

    /// We use special code to read denormals (inf, nan), because we support slightly more variants that double-conversion library does:
    /// Example: inf and Infinity.

    bool negative = false;

    while (true)
    {
        switch (*buf.position())
        {
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

    static const double_conversion::StringToDoubleConverter converter(
        double_conversion::StringToDoubleConverter::ALLOW_TRAILING_JUNK,
        0, 0, nullptr, nullptr);

    /// Fast path (avoid copying) if the buffer have at least MAX_LENGTH bytes.
    static constexpr int MAX_LENGTH = 310;

    if (buf.position() + MAX_LENGTH <= buf.buffer().end())
    {
        int num_processed_characters = 0;

        if constexpr (std::is_same_v<T, double>)
            x = converter.StringToDouble(buf.position(), buf.buffer().end() - buf.position(), &num_processed_characters);
        else
            x = converter.StringToFloat(buf.position(), buf.buffer().end() - buf.position(), &num_processed_characters);

        if (num_processed_characters <= 0)
        {
            if constexpr (throw_exception)
                throw Exception("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return ReturnType(false);
        }

        buf.position() += num_processed_characters;

        if (negative)
            x = -x;
        return ReturnType(true);
    }
    else
    {
        /// Slow path. Copy characters that may be present in floating point number to temporary buffer.

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

        int num_processed_characters = 0;

        if constexpr (std::is_same_v<T, double>)
            x = converter.StringToDouble(tmp_buf, num_copied_chars, &num_processed_characters);
        else
            x = converter.StringToFloat(tmp_buf, num_copied_chars, &num_processed_characters);

        if (num_processed_characters < num_copied_chars)
        {
            if constexpr (throw_exception)
                throw Exception("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
            else
                return ReturnType(false);
        }

        if (negative)
            x = -x;
        return ReturnType(true);
    }
}


template <size_t N, typename T>
void readIntTextUpToNChars(T & x, ReadBuffer & buf)
{
    if (unlikely(buf.eof()))
        return;

    bool negative = false;

    if (std::is_signed_v<T> && *buf.position() == '-')
    {
        ++buf.position();
        negative = true;
    }

    for (size_t i = 0; !buf.eof(); ++i)
    {
        if ((*buf.position() & 0xF0) == 0x30)
        {
            if (likely(i < N))
            {
                x *= 10;
                x += *buf.position() & 0x0F;
            }
            ++buf.position();
        }
        else
            break;
    }
    if (std::is_signed_v<T> && negative)
        x = -x;
}


template <typename T, typename ReturnType>
ReturnType readFloatTextFastImpl(T & x, ReadBuffer & in)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.', "Layout of char is not like ASCII");

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto prev_count = in.count();

    Int64 before_point = 0;
    UInt64 after_point = 0;
    int after_point_exponent = 0;
    int exponent = 0;

    constexpr int significant_digits_before_point = std::numeric_limits<decltype(before_point)>::digits10;
    readIntTextUpToNChars<significant_digits_before_point>(before_point, in);
    int read_digits = in.count() - prev_count;

    int before_point_additional_exponent = 0;
    if (read_digits > significant_digits_before_point)
        before_point_additional_exponent = read_digits - significant_digits_before_point;
    else
    {
        /// Shortcut for the common case when there is an integer that fit in Int64.
        if (read_digits && (in.eof() || *in.position() < '.'))
        {
            x = before_point;
            return ReturnType(true);
        }
    }

    if (checkChar('.', in))
    {
        auto after_point_count = in.count();
        constexpr int significant_digits_after_point = std::numeric_limits<decltype(after_point)>::digits10;
        readIntTextUpToNChars<significant_digits_after_point>(after_point, in);
        int read_digits = in.count() - after_point_count;
        after_point_exponent = read_digits > significant_digits_after_point ? -significant_digits_after_point : -read_digits;
    }

    if (checkChar('e', in) || checkChar('E', in))
    {
        readIntTextUpToNChars<4>(exponent, in);
    }

    if (unlikely(before_point_additional_exponent))
        x = shift10(before_point, before_point_additional_exponent);
    else
        x = before_point;

    if (after_point)
        x += shift10(after_point, after_point_exponent);

    if (exponent)
        x = shift10(x, exponent);

    auto total_read_characters = in.count() - prev_count;

    if (total_read_characters == 0)
    {
        if constexpr (throw_exception)
            throw Exception("Cannot read floating point value", ErrorCodes::CANNOT_PARSE_NUMBER);
        else
            return false;
    }

    /// Denormals. At most one character is read before denormal and it is '-'.
    /// Note that it also can be '+' and '0', but we don't support this case and the behaviour is implementation specific.
    if (!in.eof() && *in.position() >= '.')
    {
        bool negative = total_read_characters == 1;

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
