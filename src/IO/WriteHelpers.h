#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>
#include <iterator>
#include <concepts>
#include <bit>

#include <pcg-random/pcg_random.hpp>

#include <Common/StackTrace.h>
#include <Common/formatIPv6.h>
#include <Common/DateLUT.h>
#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/transformEndianness.h>
#include <base/find_symbols.h>
#include <base/StringRef.h>
#include <base/DecomposedFloat.h>

#include <Core/DecimalFunctions.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <base/IPv4andIPv6.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/NaNUtils.h>
#include <Common/typeid_cast.h>

#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteIntText.h>
#include <IO/VarInt.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#pragma clang diagnostic ignored "-Wsign-compare"
#include <dragonbox/dragonbox_to_chars.h>
#pragma clang diagnostic pop

#include <Formats/FormatSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
}


/// Helper functions for formatted and binary output.

inline void writeChar(char x, WriteBuffer & buf)
{
    buf.write(x);
}

/// Write the same character n times.
inline void writeChar(char c, size_t n, WriteBuffer & buf)
{
    while (n)
    {
        buf.nextIfAtEnd();
        size_t count = std::min(n, buf.available());
        memset(buf.position(), c, count);
        n -= count;
        buf.position() += count;
    }
}

/// Write POD-type in native format. It's recommended to use only with packed (dense) data types.
template <typename T>
inline void writePODBinary(const T & x, WriteBuffer & buf)
{
    buf.write(reinterpret_cast<const char *>(&x), sizeof(x)); /// NOLINT
}

inline void writeUUIDBinary(const UUID & x, WriteBuffer & buf)
{
    const auto & uuid = x.toUnderType();
    writePODBinary(uuid.items[0], buf);
    writePODBinary(uuid.items[1], buf);
}

template <typename T>
inline void writeIntBinary(const T & x, WriteBuffer & buf)
{
    writePODBinary(x, buf);
}

template <typename T>
inline void writeFloatBinary(const T & x, WriteBuffer & buf)
{
    writePODBinary(x, buf);
}


inline void writeStringBinary(const std::string & s, WriteBuffer & buf)
{
    writeVarUInt(s.size(), buf);
    buf.write(s.data(), s.size());
}

/// For historical reasons we store IPv6 as a String
inline void writeIPv6Binary(const IPv6 & ip, WriteBuffer & buf)
{
    writeVarUInt(IPV6_BINARY_LENGTH, buf);
    buf.write(reinterpret_cast<const char *>(&ip.toUnderType()), IPV6_BINARY_LENGTH);
}

inline void writeStringBinary(StringRef s, WriteBuffer & buf)
{
    writeVarUInt(s.size, buf);
    buf.write(s.data, s.size);
}

inline void writeStringBinary(const char * s, WriteBuffer & buf)
{
    writeStringBinary(StringRef{s}, buf);
}

inline void writeStringBinary(std::string_view s, WriteBuffer & buf)
{
    writeStringBinary(StringRef{s}, buf);
}


template <typename T>
void writeVectorBinary(const std::vector<T> & v, WriteBuffer & buf)
{
    writeVarUInt(v.size(), buf);

    for (typename std::vector<T>::const_iterator it = v.begin(); it != v.end(); ++it)
        writeBinary(*it, buf);
}


inline void writeBoolText(bool x, WriteBuffer & buf)
{
    writeChar(x ? '1' : '0', buf);
}


template <typename T>
inline size_t writeFloatTextFastPath(T x, char * buffer)
{
    Int64 result = 0;

    if constexpr (std::is_same_v<T, double>)
    {
        /// The library Ryu has low performance on integers.
        /// This workaround improves performance 6..10 times.

        if (DecomposedFloat64(x).isIntegerInRepresentableRange())
            result = itoa(Int64(x), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(x, buffer) - buffer;
    }
    else
    {
        if (DecomposedFloat32(x).isIntegerInRepresentableRange())
            result = itoa(Int32(x), buffer) - buffer;
        else
            result = jkj::dragonbox::to_chars_n(x, buffer) - buffer;
    }

    if (result <= 0)
        throw Exception(ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print floating point number");
    return result;
}

template <typename T>
inline void writeFloatText(T x, WriteBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for writeFloatText must be float or double");

    using Converter = DoubleConverter<false>;
    if (likely(buf.available() >= Converter::MAX_REPRESENTATION_LENGTH))
    {
        buf.position() += writeFloatTextFastPath(x, buf.position());
        return;
    }

    Converter::BufferType buffer;
    size_t result = writeFloatTextFastPath(x, buffer);
    buf.write(buffer, result);
}


inline void writeString(const char * data, size_t size, WriteBuffer & buf)
{
    buf.write(data, size);
}

// Otherwise StringRef and string_view overloads are ambiguous when passing string literal. Prefer std::string_view
void writeString(std::same_as<StringRef> auto ref, WriteBuffer & buf)
{
    writeString(ref.data, ref.size, buf);
}

inline void writeString(std::string_view ref, WriteBuffer & buf)
{
    writeString(ref.data(), ref.size(), buf);
}

/** Writes a C-string without creating a temporary object. If the string is a literal, then `strlen` is executed at the compilation stage.
  * Use when the string is a literal.
  */
#define writeCString(s, buf) \
    (buf).write((s), strlen(s))

/** Writes a string for use in the JSON format:
 *  - the string is written in double quotes
 *  - slash character '/' is escaped for compatibility with JavaScript
 *  - bytes from the range 0x00-0x1F except `\b', '\f', '\n', '\r', '\t' are escaped as \u00XX
 *  - code points U+2028 and U+2029 (byte sequences in UTF-8: e2 80 a8, e2 80 a9) are escaped as \u2028 and \u2029
 *  - it is assumed that string is in UTF-8, the invalid UTF-8 is not processed
 *  - all other non-ASCII characters remain as is
 */
inline void writeJSONString(const char * begin, const char * end, WriteBuffer & buf, const FormatSettings & settings)
{
    writeChar('"', buf);
    for (const char * it = begin; it != end; ++it)
    {
        switch (*it)
        {
            case '\b':
                writeChar('\\', buf);
                writeChar('b', buf);
                break;
            case '\f':
                writeChar('\\', buf);
                writeChar('f', buf);
                break;
            case '\n':
                writeChar('\\', buf);
                writeChar('n', buf);
                break;
            case '\r':
                writeChar('\\', buf);
                writeChar('r', buf);
                break;
            case '\t':
                writeChar('\\', buf);
                writeChar('t', buf);
                break;
            case '\\':
                writeChar('\\', buf);
                writeChar('\\', buf);
                break;
            case '/':
                if (settings.json.escape_forward_slashes)
                    writeChar('\\', buf);
                writeChar('/', buf);
                break;
            case '"':
                writeChar('\\', buf);
                writeChar('"', buf);
                break;
            default:
                UInt8 c = *it;
                if (c <= 0x1F)
                {
                    /// Escaping of ASCII control characters.

                    UInt8 higher_half = c >> 4;
                    UInt8 lower_half = c & 0xF;

                    writeCString("\\u00", buf);
                    writeChar('0' + higher_half, buf);

                    if (lower_half <= 9)
                        writeChar('0' + lower_half, buf);
                    else
                        writeChar('A' + lower_half - 10, buf);
                }
                else if (end - it >= 3 && it[0] == '\xE2' && it[1] == '\x80' && (it[2] == '\xA8' || it[2] == '\xA9'))
                {
                    /// This is for compatibility with JavaScript, because unescaped line separators are prohibited in string literals,
                    ///  and these code points are alternative line separators.

                    if (it[2] == '\xA8')
                        writeCString("\\u2028", buf);
                    if (it[2] == '\xA9')
                        writeCString("\\u2029", buf);

                    /// Byte sequence is 3 bytes long. We have additional two bytes to skip.
                    it += 2;
                }
                else
                    writeChar(*it, buf);
        }
    }
    writeChar('"', buf);
}


/** Will escape quote_character and a list of special characters('\b', '\f', '\n', '\r', '\t', '\0', '\\').
 *   - when escape_quote_with_quote is true, use backslash to escape list of special characters,
 *      and use quote_character to escape quote_character. such as: 'hello''world'
 *     otherwise use backslash to escape list of special characters and quote_character
 *   - when escape_backslash_with_backslash is true, backslash is escaped with another backslash
 */
template <char quote_character, bool escape_quote_with_quote = false, bool escape_backslash_with_backslash = true>
void writeAnyEscapedString(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// On purpose we will escape more characters than minimally necessary.
        const char * next_pos = find_first_symbols<'\b', '\f', '\n', '\r', '\t', '\0', '\\', quote_character>(pos, end);

        /// NOLINTBEGIN(readability-else-after-return)
        if (next_pos == end)
        {
            buf.write(pos, next_pos - pos);
            break;
        }
        else
        {
            buf.write(pos, next_pos - pos);
            pos = next_pos;
            switch (*pos)
            {
                case quote_character:
                {
                    if constexpr (escape_quote_with_quote)
                        writeChar(quote_character, buf);
                    else
                        writeChar('\\', buf);
                    writeChar(quote_character, buf);
                    break;
                }
                case '\b':
                    writeChar('\\', buf);
                    writeChar('b', buf);
                    break;
                case '\f':
                    writeChar('\\', buf);
                    writeChar('f', buf);
                    break;
                case '\n':
                    writeChar('\\', buf);
                    writeChar('n', buf);
                    break;
                case '\r':
                    writeChar('\\', buf);
                    writeChar('r', buf);
                    break;
                case '\t':
                    writeChar('\\', buf);
                    writeChar('t', buf);
                    break;
                case '\0':
                    writeChar('\\', buf);
                    writeChar('0', buf);
                    break;
                case '\\':
                    if constexpr (escape_backslash_with_backslash)
                        writeChar('\\', buf);
                    writeChar('\\', buf);
                    break;
                default:
                    writeChar(*pos, buf);
            }
            ++pos;
        }
        /// NOLINTEND(readability-else-after-return)
    }
}

/// Define special characters in Markdown according to the standards specified by CommonMark.
inline void writeAnyMarkdownEscapedString(const char * begin, const char * end, WriteBuffer & buf)
{
    for (const char * it = begin; it != end; ++it)
    {
        switch (*it)
        {
            case '!':
                writeChar('\\', buf);
                writeChar('!', buf);
                break;
            case '"':
                writeChar('\\', buf);
                writeChar('"', buf);
                break;
            case '#':
                writeChar('\\', buf);
                writeChar('#', buf);
                break;
            case '$':
                writeChar('\\', buf);
                writeChar('$', buf);
                break;
            case '%':
                writeChar('\\', buf);
                writeChar('%', buf);
                break;
            case '&':
                writeChar('\\', buf);
                writeChar('&', buf);
                break;
            case '\'':
                writeChar('\\', buf);
                writeChar('\'', buf);
                break;
            case '(':
                writeChar('\\', buf);
                writeChar('(', buf);
                break;
            case ')':
                writeChar('\\', buf);
                writeChar(')', buf);
                break;
            case '*':
                writeChar('\\', buf);
                writeChar('*', buf);
                break;
            case '+':
                writeChar('\\', buf);
                writeChar('+', buf);
                break;
            case ',':
                writeChar('\\', buf);
                writeChar(',', buf);
                break;
            case '-':
                writeChar('\\', buf);
                writeChar('-', buf);
                break;
            case '.':
                writeChar('\\', buf);
                writeChar('.', buf);
                break;
            case '/':
                writeChar('\\', buf);
                writeChar('/', buf);
                break;
            case ':':
                writeChar('\\', buf);
                writeChar(':', buf);
                break;
            case ';':
                writeChar('\\', buf);
                writeChar(';', buf);
                break;
            case '<':
                writeChar('\\', buf);
                writeChar('<', buf);
                break;
            case '=':
                writeChar('\\', buf);
                writeChar('=', buf);
                break;
            case '>':
                writeChar('\\', buf);
                writeChar('>', buf);
                break;
            case '?':
                writeChar('\\', buf);
                writeChar('?', buf);
                break;
            case '@':
                writeChar('\\', buf);
                writeChar('@', buf);
                break;
            case '[':
                writeChar('\\', buf);
                writeChar('[', buf);
                break;
            case '\\':
                writeChar('\\', buf);
                writeChar('\\', buf);
                break;
            case ']':
                writeChar('\\', buf);
                writeChar(']', buf);
                break;
            case '^':
                writeChar('\\', buf);
                writeChar('^', buf);
                break;
            case '_':
                writeChar('\\', buf);
                writeChar('_', buf);
                break;
            case '`':
                writeChar('\\', buf);
                writeChar('`', buf);
                break;
            case '{':
                writeChar('\\', buf);
                writeChar('{', buf);
                break;
            case '|':
                writeChar('\\', buf);
                writeChar('|', buf);
                break;
            case '}':
                writeChar('\\', buf);
                writeChar('}', buf);
                break;
            case '~':
                writeChar('\\', buf);
                writeChar('~', buf);
                break;
            default:
                writeChar(*it, buf);
        }
    }
}

inline void writeJSONString(std::string_view s, WriteBuffer & buf, const FormatSettings & settings)
{
    writeJSONString(s.data(), s.data() + s.size(), buf, settings);
}

template <typename T>
void writeJSONNumber(T x, WriteBuffer & ostr, const FormatSettings & settings)
{
    bool is_finite = isFinite(x);

    const bool need_quote = (is_integer<T> && (sizeof(T) >= 8) && settings.json.quote_64bit_integers)
        || (settings.json.quote_denormals && !is_finite) || (is_floating_point<T> && (sizeof(T) >= 8) && settings.json.quote_64bit_floats);

    if (need_quote)
        writeChar('"', ostr);

    if (is_finite)
        writeText(x, ostr);
    else if (!settings.json.quote_denormals)
        writeCString("null", ostr);
    else
    {
        if constexpr (std::is_floating_point_v<T>)
        {
            if (std::signbit(x))
            {
                if (isNaN(x))
                    writeCString("-nan", ostr);
                else
                    writeCString("-inf", ostr);
            }
            else
            {
                if (isNaN(x))
                    writeCString("nan", ostr);
                else
                    writeCString("inf", ostr);
            }
        }
    }

    if (need_quote)
        writeChar('"', ostr);
}


template <char c>
void writeAnyEscapedString(std::string_view s, WriteBuffer & buf)
{
    writeAnyEscapedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeEscapedString(const char * str, size_t size, WriteBuffer & buf)
{
    writeAnyEscapedString<'\''>(str, str + size, buf);
}

inline void writeEscapedString(std::string_view ref, WriteBuffer & buf)
{
    writeEscapedString(ref.data(), ref.size(), buf);
}

inline void writeMarkdownEscapedString(const char * str, size_t size, WriteBuffer & buf)
{
    writeAnyMarkdownEscapedString(str, str + size, buf);
}

inline void writeMarkdownEscapedString(std::string_view ref, WriteBuffer & buf)
{
    writeMarkdownEscapedString(ref.data(), ref.size(), buf);
}

template <char quote_character>
void writeAnyQuotedString(const char * begin, const char * end, WriteBuffer & buf)
{
    writeChar(quote_character, buf);
    writeAnyEscapedString<quote_character>(begin, end, buf);
    writeChar(quote_character, buf);
}


template <char quote_character>
void writeAnyQuotedString(std::string_view ref, WriteBuffer & buf)
{
    writeAnyQuotedString<quote_character>(ref.data(), ref.data() + ref.size(), buf);
}


inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(s, buf);
}

inline void writeQuotedString(StringRef ref, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(ref.toView(), buf);
}

inline void writeQuotedString(std::string_view ref, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(ref.data(), ref.data() + ref.size(), buf);
}

inline void writeQuotedStringPostgreSQL(std::string_view ref, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeAnyEscapedString<'\'', true, false>(ref.data(), ref.data() + ref.size(), buf);
    writeChar('\'', buf);
}

inline void writeDoubleQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s, buf);
}

inline void writeDoubleQuotedString(StringRef s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s.toView(), buf);
}

inline void writeDoubleQuotedString(std::string_view s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s.data(), s.data() + s.size(), buf);
}

/// Outputs a string in backquotes.
inline void writeBackQuotedString(StringRef s, WriteBuffer & buf)
{
    writeAnyQuotedString<'`'>(s.toView(), buf);
}

/// Outputs a string in backquotes for MySQL.
inline void writeBackQuotedStringMySQL(StringRef s, WriteBuffer & buf)
{
    writeChar('`', buf);
    writeAnyEscapedString<'`', true>(s.data, s.data + s.size, buf);
    writeChar('`', buf);
}


/// Write quoted if the string doesn't look like and identifier.
void writeProbablyBackQuotedString(StringRef s, WriteBuffer & buf);
void writeProbablyDoubleQuotedString(StringRef s, WriteBuffer & buf);
void writeProbablyBackQuotedStringMySQL(StringRef s, WriteBuffer & buf);


/** Outputs the string in for the CSV format.
  * Rules:
  * - the string is outputted in quotation marks;
  * - the quotation mark inside the string is outputted as two quotation marks in sequence.
  */
template <char quote = '"'>
void writeCSVString(const char * begin, const char * end, WriteBuffer & buf)
{
    writeChar(quote, buf);

    const char * pos = begin;
    while (true)
    {
        const char * next_pos = find_first_symbols<quote>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }

        /// Quotation.
        ++next_pos;
        buf.write(pos, next_pos - pos);
        writeChar(quote, buf);

        pos = next_pos;
    }

    writeChar(quote, buf);
}

template <char quote = '"'>
void writeCSVString(const String & s, WriteBuffer & buf)
{
    writeCSVString<quote>(s.data(), s.data() + s.size(), buf);
}

template <char quote = '"'>
void writeCSVString(StringRef s, WriteBuffer & buf)
{
    writeCSVString<quote>(s.data, s.data + s.size, buf);
}

inline void writeXMLStringForTextElementOrAttributeValue(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        const char * next_pos = find_first_symbols<'<', '&', '>', '"', '\''>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }
        if (*next_pos == '<')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&lt;", buf);
        }
        else if (*next_pos == '&')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&amp;", buf);
        }
        else if (*next_pos == '>')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&gt;", buf);
        }
        else if (*next_pos == '"')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&quot;", buf);
        }
        else if (*next_pos == '\'')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&apos;", buf);
        }

        pos = next_pos;
    }
}

inline void writeXMLStringForTextElementOrAttributeValue(std::string_view s, WriteBuffer & buf)
{
    writeXMLStringForTextElementOrAttributeValue(s.data(), s.data() + s.size(), buf);
}

/// Writing a string to a text node in XML (not into an attribute - otherwise you need more escaping).
inline void writeXMLStringForTextElement(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// NOTE Perhaps for some XML parsers, you need to escape the zero byte and some control characters.
        const char * next_pos = find_first_symbols<'<', '&'>(pos, end);

        if (next_pos == end)
        {
            buf.write(pos, end - pos);
            break;
        }
        if (*next_pos == '<')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&lt;", buf);
        }
        else if (*next_pos == '&')
        {
            buf.write(pos, next_pos - pos);
            ++next_pos;
            writeCString("&amp;", buf);
        }

        pos = next_pos;
    }
}

inline void writeXMLStringForTextElement(std::string_view s, WriteBuffer & buf)
{
    writeXMLStringForTextElement(s.data(), s.data() + s.size(), buf);
}

/// @brief Serialize `uuid` into an array of characters in big-endian byte order.
/// @param uuid UUID to serialize.
/// @return Array of characters in big-endian byte order.
std::array<char, 36> formatUUID(const UUID & uuid);

inline void writeUUIDText(const UUID & uuid, WriteBuffer & buf)
{
    const auto serialized_uuid = formatUUID(uuid);
    buf.write(serialized_uuid.data(), serialized_uuid.size());
}

void writeIPv4Text(const IPv4 & ip, WriteBuffer & buf);
void writeIPv6Text(const IPv6 & ip, WriteBuffer & buf);

template <typename DecimalType, bool cut_trailing_zeros_align_to_groups_of_thousands = false>
inline void writeDateTime64FractionalText(typename DecimalType::NativeType fractional, UInt32 scale, WriteBuffer & buf)
{
    static constexpr UInt32 MaxScale = DecimalUtils::max_precision<DecimalType>;

    char data[20] = {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0', '0'};
    static_assert(sizeof(data) >= MaxScale);

    for (Int32 pos = scale - 1; pos >= 0 && fractional; --pos, fractional /= DateTime64(10))
        data[pos] += fractional % DateTime64(10);

    if constexpr (cut_trailing_zeros_align_to_groups_of_thousands)
    {
        UInt32 last_none_zero_pos = 0;
        for (UInt32 pos = 0; pos < scale; ++pos)
        {
            if (data[pos] != '0')
            {
                last_none_zero_pos = pos;
            }
        }
        size_t new_scale = (last_none_zero_pos >= 3 ? 6 : 3);
        writeString(&data[0], new_scale, buf);
    }
    else
    {
        writeString(&data[0], static_cast<size_t>(scale), buf);
    }
}

static const char digits100[201] =
    "00010203040506070809"
    "10111213141516171819"
    "20212223242526272829"
    "30313233343536373839"
    "40414243444546474849"
    "50515253545556575859"
    "60616263646566676869"
    "70717273747576777879"
    "80818283848586878889"
    "90919293949596979899";

/// in YYYY-MM-DD format
template <char delimiter = '-'>
inline void writeDateText(const LocalDate & date, WriteBuffer & buf)
{
    if (reinterpret_cast<intptr_t>(buf.position()) + 10 <= reinterpret_cast<intptr_t>(buf.buffer().end()))
    {
        memcpy(buf.position(), &digits100[date.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits100[date.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[date.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[date.day() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits100[date.year() / 100 * 2], 2);
        buf.write(&digits100[date.year() % 100 * 2], 2);
        buf.write(delimiter);
        buf.write(&digits100[date.month() * 2], 2);
        buf.write(delimiter);
        buf.write(&digits100[date.day() * 2], 2);
    }
}

template <char delimiter = '-'>
inline void writeDateText(DayNum date, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    writeDateText<delimiter>(LocalDate(date, time_zone), buf);
}

template <char delimiter = '-'>
inline void writeDateText(ExtendedDayNum date, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    writeDateText<delimiter>(LocalDate(date, time_zone), buf);
}

/// In the format YYYY-MM-DD HH:MM:SS
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(const LocalDateTime & datetime, WriteBuffer & buf)
{
    if (reinterpret_cast<intptr_t>(buf.position()) + 19 <= reinterpret_cast<intptr_t>(buf.buffer().end()))
    {
        memcpy(buf.position(), &digits100[datetime.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits100[datetime.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.day() * 2], 2);
        buf.position() += 2;
        *buf.position() = between_date_time_delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.hour() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.minute() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits100[datetime.second() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits100[datetime.year() / 100 * 2], 2);
        buf.write(&digits100[datetime.year() % 100 * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits100[datetime.month() * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits100[datetime.day() * 2], 2);
        buf.write(between_date_time_delimiter);
        buf.write(&digits100[datetime.hour() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits100[datetime.minute() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits100[datetime.second() * 2], 2);
    }
}

/// In the format YYYY-MM-DD HH:MM:SS, according to the specified time zone.
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    writeDateTimeText<date_delimeter, time_delimeter, between_date_time_delimiter>(LocalDateTime(datetime, time_zone), buf);
}

/// In the format YYYY-MM-DD HH:MM:SS.NNNNNNNNN, according to the specified time zone.
template <
    char date_delimeter = '-',
    char time_delimeter = ':',
    char between_date_time_delimiter = ' ',
    char fractional_time_delimiter = '.',
    bool cut_trailing_zeros_align_to_groups_of_thousands = false>
inline void writeDateTimeText(DateTime64 datetime64, UInt32 scale, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    static constexpr UInt32 MaxScale = DecimalUtils::max_precision<DateTime64>;
    scale = scale > MaxScale ? MaxScale : scale;

    auto components = DecimalUtils::split(datetime64, scale);
    /// Case1:
    /// -127914467.877
    /// => whole = -127914467, fraction = 877(After DecimalUtils::split)
    /// => new whole = -127914468(1965-12-12 12:12:12), new fraction = 1000 - 877 = 123(.123)
    /// => 1965-12-12 12:12:12.123
    ///
    /// Case2:
    /// -0.877
    /// => whole = 0, fractional = -877(After DecimalUtils::split)
    /// => whole = -1(1969-12-31 23:59:59), fractional = 1000 + (-877) = 123(.123)
    using T = typename DateTime64::NativeType;
    if (datetime64.value < 0 && components.fractional)
    {
        components.fractional = DecimalUtils::scaleMultiplier<T>(scale) + (components.whole ? T(-1) : T(1)) * components.fractional;
        --components.whole;
    }

    writeDateTimeText<date_delimeter, time_delimeter, between_date_time_delimiter>(LocalDateTime(components.whole, time_zone), buf);
    if constexpr (cut_trailing_zeros_align_to_groups_of_thousands)
    {
        if (scale > 0 && components.fractional != 0)
        {
            buf.write(fractional_time_delimiter);
            writeDateTime64FractionalText<DateTime64, true>(components.fractional, scale, buf);
        }
    }
    else
    {
        if (scale > 0)
        {
            buf.write(fractional_time_delimiter);
            writeDateTime64FractionalText<DateTime64, false>(components.fractional, scale, buf);
        }
    }
}

inline void writeDateTimeTextCutTrailingZerosAlignToGroupOfThousands(DateTime64 datetime64, UInt32 scale, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    writeDateTimeText<'-', ':', ' ', '.', true>(datetime64, scale, buf, time_zone);
}

/// In the RFC 1123 format: "Tue, 03 Dec 2019 00:11:50 GMT". You must provide GMT DateLUT.
/// This is needed for HTTP requests.
inline void writeDateTimeTextRFC1123(time_t datetime, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    const auto & values = time_zone.getValues(datetime);

    static const char week_days[3 * 8 + 1] = "XXX" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat" "Sun";
    static const char months[3 * 13 + 1] = "XXX" "Jan" "Feb" "Mar" "Apr" "May" "Jun" "Jul" "Aug" "Sep" "Oct" "Nov" "Dec";

    buf.write(&week_days[values.day_of_week * 3], 3);
    buf.write(", ", 2);
    buf.write(&digits100[values.day_of_month * 2], 2);
    buf.write(' ');
    buf.write(&months[values.month * 3], 3);
    buf.write(' ');
    buf.write(&digits100[values.year / 100 * 2], 2);
    buf.write(&digits100[values.year % 100 * 2], 2);
    buf.write(' ');
    buf.write(&digits100[time_zone.toHour(datetime) * 2], 2);
    buf.write(':');
    buf.write(&digits100[time_zone.toMinute(datetime) * 2], 2);
    buf.write(':');
    buf.write(&digits100[time_zone.toSecond(datetime) * 2], 2);
    buf.write(" GMT", 4);
}

inline void writeDateTimeTextISO(time_t datetime, WriteBuffer & buf, const DateLUTImpl & utc_time_zone)
{
    writeDateTimeText<'-', ':', 'T'>(datetime, buf, utc_time_zone);
    buf.write('Z');
}

inline void writeDateTimeTextISO(DateTime64 datetime64, UInt32 scale, WriteBuffer & buf, const DateLUTImpl & utc_time_zone)
{
    writeDateTimeText<'-', ':', 'T'>(datetime64, scale, buf, utc_time_zone);
    buf.write('Z');
}

inline void writeDateTimeUnixTimestamp(DateTime64 datetime64, UInt32 scale, WriteBuffer & buf)
{
    static constexpr UInt32 MaxScale = DecimalUtils::max_precision<DateTime64>;
    scale = scale > MaxScale ? MaxScale : scale;

    auto components = DecimalUtils::split(datetime64, scale);
    writeIntText(components.whole, buf);

    if (scale > 0)
    {
        buf.write('.');
        writeDateTime64FractionalText<DateTime64>(components.fractional, scale, buf);
    }
}

/// Methods for output in binary format.
template <typename T>
requires is_arithmetic_v<T>
inline void writeBinary(const T & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const String & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(StringRef x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(std::string_view x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const Decimal32 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal64 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Decimal256 & x, WriteBuffer & buf) { writePODBinary(x.value, buf); }
inline void writeBinary(const LocalDate & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDateTime & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const IPv4 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const IPv6 & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const UUID & x, WriteBuffer & buf)
{
    writeUUIDBinary(x, buf);
}

inline void writeBinary(const CityHash_v1_0_2::uint128 & x, WriteBuffer & buf)
{
    writePODBinary(x.low64, buf);
    writePODBinary(x.high64, buf);
}

inline void writeBinary(const StackTrace::FramePointers & x, WriteBuffer & buf) { writePODBinary(x, buf); }

/// Methods for outputting the value in text form for a tab-separated format.

inline void writeText(is_integer auto x, WriteBuffer & buf)
{
    if constexpr (std::is_same_v<decltype(x), bool>)
        writeBoolText(x, buf);
    else if constexpr (std::is_same_v<decltype(x), char>)
        writeChar(x, buf);
    else
        writeIntText(x, buf);
}

inline void writeText(is_floating_point auto x, WriteBuffer & buf) { writeFloatText(x, buf); }

inline void writeText(is_enum auto x, WriteBuffer & buf) { writeText(magic_enum::enum_name(x), buf); }

inline void writeText(std::string_view x, WriteBuffer & buf) { writeString(x.data(), x.size(), buf); }

inline void writeText(const DayNum & x, WriteBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance()) { writeDateText(LocalDate(x, time_zone), buf); }
inline void writeText(const LocalDate & x, WriteBuffer & buf) { writeDateText(x, buf); }
inline void writeText(const LocalDateTime & x, WriteBuffer & buf) { writeDateTimeText(x, buf); }
inline void writeText(const UUID & x, WriteBuffer & buf) { writeUUIDText(x, buf); }
inline void writeText(const IPv4 & x, WriteBuffer & buf) { writeIPv4Text(x, buf); }
inline void writeText(const IPv6 & x, WriteBuffer & buf) { writeIPv6Text(x, buf); }

template <typename T>
void writeDecimalFractional(const T & x, UInt32 scale, WriteBuffer & ostr, bool trailing_zeros,
                            bool fixed_fractional_length, UInt32 fractional_length)
{
    /// If it's big integer, but the number of digits is small,
    /// use the implementation for smaller integers for more efficient arithmetic.
    if constexpr (std::is_same_v<T, Int256>)
    {
        if (x <= std::numeric_limits<UInt32>::max())
        {
            writeDecimalFractional(static_cast<UInt32>(x), scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
            return;
        }
        if (x <= std::numeric_limits<UInt64>::max())
        {
            writeDecimalFractional(static_cast<UInt64>(x), scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
            return;
        }
        if (x <= std::numeric_limits<UInt128>::max())
        {
            writeDecimalFractional(static_cast<UInt128>(x), scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
            return;
        }
    }
    else if constexpr (std::is_same_v<T, Int128>)
    {
        if (x <= std::numeric_limits<UInt32>::max())
        {
            writeDecimalFractional(static_cast<UInt32>(x), scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
            return;
        }
        if (x <= std::numeric_limits<UInt64>::max())
        {
            writeDecimalFractional(static_cast<UInt64>(x), scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
            return;
        }
    }

    constexpr size_t max_digits = std::numeric_limits<UInt256>::digits10;
    assert(scale <= max_digits);
    assert(fractional_length <= max_digits);

    char buf[max_digits];
    memset(buf, '0', std::max(scale, fractional_length));

    T value = x;
    Int32 last_nonzero_pos = 0;

    if (fixed_fractional_length && fractional_length < scale)
    {
        T new_value = value / DecimalUtils::scaleMultiplier<Int256>(scale - fractional_length - 1);
        auto round_carry = new_value % 10;
        value = new_value / 10;
        if (round_carry >= 5)
            value += 1;
    }

    for (Int32 pos = fixed_fractional_length ? std::min(scale - 1, fractional_length - 1) : scale - 1; pos >= 0; --pos)
    {
        auto remainder = value % 10;
        value /= 10;

        if (remainder != 0 && last_nonzero_pos == 0)
            last_nonzero_pos = pos;

        buf[pos] += static_cast<char>(remainder);
    }

    writeChar('.', ostr);
    ostr.write(buf, fixed_fractional_length ? fractional_length : (trailing_zeros ? scale : last_nonzero_pos + 1));
}

template <typename T>
void writeText(Decimal<T> x, UInt32 scale, WriteBuffer & ostr, bool trailing_zeros,
               bool fixed_fractional_length = false, UInt32 fractional_length = 0)
{
    T part = DecimalUtils::getWholePart(x, scale);

    if (x.value < 0 && part == 0)
    {
        writeChar('-', ostr); /// avoid crop leading minus when whole part is zero
    }

    writeIntText(part, ostr);

    if (scale || (fixed_fractional_length && fractional_length > 0))
    {
        part = DecimalUtils::getFractionalPart(x, scale);
        if (part || trailing_zeros)
        {
            if (part < 0)
                part *= T(-1);

            writeDecimalFractional(part, scale, ostr, trailing_zeros, fixed_fractional_length, fractional_length);
        }
    }
}

/// String, date, datetime are in single quotes with C-style escaping. Numbers - without.
template <typename T>
requires is_arithmetic_v<T>
inline void writeQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeQuoted(const String & x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(std::string_view x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(StringRef x, WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(const LocalDate & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const LocalDateTime & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateTimeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const UUID & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const IPv4 & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const IPv6 & x, WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeText(x, buf);
    writeChar('\'', buf);
}

/// String, date, datetime are in double quotes with C-style escaping. Numbers - without.
template <typename T>
requires is_arithmetic_v<T>
inline void writeDoubleQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeDoubleQuoted(const String & x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(std::string_view x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(StringRef x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(const LocalDate & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const LocalDateTime & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateTimeText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const UUID & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const IPv4 & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const IPv6 & x, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeText(x, buf);
    writeChar('"', buf);
}

/// String - in double quotes and with CSV-escaping; date, datetime - in double quotes. Numbers - without.
template <typename T>
requires is_arithmetic_v<T>
inline void writeCSV(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeCSV(const String & x, WriteBuffer & buf) { writeCSVString<>(x, buf); }
inline void writeCSV(const LocalDate & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const LocalDateTime & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UUID & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const IPv4 & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const IPv6 & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }

template <typename T>
void writeBinary(const std::vector<T> & x, WriteBuffer & buf)
{
    size_t size = x.size();
    writeVarUInt(size, buf);
    for (size_t i = 0; i < size; ++i)
        writeBinary(x[i], buf);
}

template <typename T>
void writeQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
    writeChar('[', buf);
    for (size_t i = 0, size = x.size(); i < size; ++i)
    {
        if (i != 0)
            writeChar(',', buf);
        writeQuoted(x[i], buf);
    }
    writeChar(']', buf);
}

template <typename T>
void writeDoubleQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
    writeChar('[', buf);
    for (size_t i = 0, size = x.size(); i < size; ++i)
    {
        if (i != 0)
            writeChar(',', buf);
        writeDoubleQuoted(x[i], buf);
    }
    writeChar(']', buf);
}

template <typename T>
void writeText(const std::vector<T> & x, WriteBuffer & buf)
{
    writeQuoted(x, buf);
}


/// Serialize exception (so that it can be transferred over the network)
void writeException(const Exception & e, WriteBuffer & buf, bool with_stack_trace);


/// An easy-to-use method for converting something to a string in text form.
template <typename T>
inline String toString(const T & x)
{
    WriteBufferFromOwnString buf;
    writeText(x, buf);
    return buf.str();
}

inline String toString(const CityHash_v1_0_2::uint128 & hash)
{
    WriteBufferFromOwnString buf;
    writeText(hash.low64, buf);
    writeChar('_', buf);
    writeText(hash.high64, buf);
    return buf.str();
}

template <typename T>
inline String toStringWithFinalSeparator(const std::vector<T> & x, const String & final_sep)
{
    WriteBufferFromOwnString buf;
    for (auto it = x.begin(); it != x.end(); ++it)
    {
        if (it != x.begin())
        {
            if (std::next(it) == x.end())
                writeString(final_sep, buf);
            else
                writeString(", ", buf);
        }
        writeQuoted(*it, buf);
    }

    return buf.str();
}

inline void writeNullTerminatedString(const String & s, WriteBuffer & buffer)
{
    /// c_str is guaranteed to return zero-terminated string
    buffer.write(s.c_str(), s.size() + 1);
}

template <std::endian endian, typename T>
inline void writeBinaryEndian(T x, WriteBuffer & buf)
{
    transformEndianness<endian>(x);
    writeBinary(x, buf);
}

template <typename T>
inline void writeBinaryLittleEndian(T x, WriteBuffer & buf)
{
    writeBinaryEndian<std::endian::little>(x, buf);
}

template <typename T>
inline void writeBinaryBigEndian(T x, WriteBuffer & buf)
{
    writeBinaryEndian<std::endian::big>(x, buf);
}


struct PcgSerializer
{
    static void serializePcg32(const pcg32_fast & rng, WriteBuffer & buf)
    {
        writeText(pcg32_fast::multiplier(), buf);
        writeChar(' ', buf);
        writeText(pcg32_fast::increment(), buf);
        writeChar(' ', buf);
        writeText(rng.state_, buf);
    }
};

void writePointerHex(const void * ptr, WriteBuffer & buf);

String fourSpaceIndent(size_t indent);

bool inline isWritingToTerminal(const WriteBuffer & buf)
{
    const auto * write_buffer_to_descriptor = typeid_cast<const WriteBufferFromFileDescriptor *>(&buf);
    return write_buffer_to_descriptor && write_buffer_to_descriptor->getFD() == STDOUT_FILENO && isatty(STDOUT_FILENO);
}

}

template<>
struct fmt::formatter<DB::UUID>
{
    template<typename ParseContext>
    constexpr auto parse(ParseContext & context)
    {
        return context.begin();
    }

    template<typename FormatContext>
    auto format(const DB::UUID & uuid, FormatContext & context) const
    {
        return fmt::format_to(context.out(), "{}", toString(uuid));
    }
};
