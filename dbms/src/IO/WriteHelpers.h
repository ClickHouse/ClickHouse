#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>
#include <iterator>

#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <common/find_first_symbols.h>

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/UInt128.h>
#include <common/StringRef.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteIntText.h>
#include <IO/VarInt.h>
#include <IO/DoubleConverter.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Helper functions for formatted and binary output.

inline void writeChar(char x, WriteBuffer & buf)
{
    buf.nextIfAtEnd();
    *buf.position() = x;
    ++buf.position();
}


/// Write POD-type in native format. It's recommended to use only with packed (dense) data types.
template <typename T>
inline void writePODBinary(const T & x, WriteBuffer & buf)
{
    buf.write(reinterpret_cast<const char *>(&x), sizeof(x));
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

inline void writeStringBinary(const char * s, WriteBuffer & buf)
{
    writeVarUInt(strlen(s), buf);
    buf.write(s, strlen(s));
}

inline void writeStringBinary(const StringRef & s, WriteBuffer & buf)
{
    writeVarUInt(s.size, buf);
    buf.write(s.data, s.size);
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
inline size_t writeFloatTextFastPath(T x, char * buffer, int len)
{
    using Converter = DoubleConverter<false>;
    double_conversion::StringBuilder builder{buffer, len};

    bool result = false;
    if constexpr (std::is_same_v<T, double>)
        result = Converter::instance().ToShortest(x, &builder);
    else
        result = Converter::instance().ToShortestSingle(x, &builder);

    if (!result)
        throw Exception("Cannot print floating point number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);
    return builder.position();
}

template <typename T>
inline void writeFloatText(T x, WriteBuffer & buf)
{
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>, "Argument for writeFloatText must be float or double");

    using Converter = DoubleConverter<false>;
    if (likely(buf.available() >= Converter::MAX_REPRESENTATION_LENGTH))
    {
        buf.position() += writeFloatTextFastPath(x, buf.position(), Converter::MAX_REPRESENTATION_LENGTH);
        return;
    }

    Converter::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    bool result = false;
    if constexpr (std::is_same_v<T, double>)
        result = Converter::instance().ToShortest(x, &builder);
    else
        result = Converter::instance().ToShortestSingle(x, &builder);

    if (!result)
        throw Exception("Cannot print floating point number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    buf.write(buffer, builder.position());
}


inline void writeString(const String & s, WriteBuffer & buf)
{
    buf.write(s.data(), s.size());
}

inline void writeString(const char * data, size_t size, WriteBuffer & buf)
{
    buf.write(data, size);
}

inline void writeString(const StringRef & ref, WriteBuffer & buf)
{
    writeString(ref.data, ref.size, buf);
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
inline void writeJSONString(const char * begin, const char * end, WriteBuffer & buf)
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


template <char c>
void writeAnyEscapedString(const char * begin, const char * end, WriteBuffer & buf)
{
    const char * pos = begin;
    while (true)
    {
        /// On purpose we will escape more characters than minimally necessary.
        const char * next_pos = find_first_symbols<'\b', '\f', '\n', '\r', '\t', '\0', '\\', c>(pos, end);

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
                    writeChar('\\', buf);
                    writeChar('\\', buf);
                    break;
                case c:
                    writeChar('\\', buf);
                    writeChar(c, buf);
                    break;
                default:
                    writeChar(*pos, buf);
            }
            ++pos;
        }
    }
}


inline void writeJSONString(const String & s, WriteBuffer & buf)
{
    writeJSONString(s.data(), s.data() + s.size(), buf);
}


inline void writeJSONString(const StringRef & ref, WriteBuffer & buf)
{
    writeJSONString(ref.data, ref.data + ref.size, buf);
}


template <char c>
void writeAnyEscapedString(const String & s, WriteBuffer & buf)
{
    writeAnyEscapedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeEscapedString(const char * str, size_t size, WriteBuffer & buf)
{
    writeAnyEscapedString<'\''>(str, str + size, buf);
}


inline void writeEscapedString(const String & s, WriteBuffer & buf)
{
    writeEscapedString(s.data(), s.size(), buf);
}


inline void writeEscapedString(const StringRef & ref, WriteBuffer & buf)
{
    writeEscapedString(ref.data, ref.size, buf);
}


template <char c>
void writeAnyQuotedString(const char * begin, const char * end, WriteBuffer & buf)
{
    writeChar(c, buf);
    writeAnyEscapedString<c>(begin, end, buf);
    writeChar(c, buf);
}



template <char c>
void writeAnyQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<c>(s.data(), s.data() + s.size(), buf);
}


template <char c>
void writeAnyQuotedString(const StringRef & ref, WriteBuffer & buf)
{
    writeAnyQuotedString<c>(ref.data, ref.data + ref.size, buf);
}


inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(s, buf);
}


inline void writeQuotedString(const StringRef & ref, WriteBuffer & buf)
{
    writeAnyQuotedString<'\''>(ref, buf);
}

inline void writeDoubleQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'"'>(s, buf);
}

/// Outputs a string in backquotes, as an identifier in MySQL.
inline void writeBackQuotedString(const String & s, WriteBuffer & buf)
{
    writeAnyQuotedString<'`'>(s, buf);
}


/// The same, but quotes apply only if there are characters that do not match the identifier without quotes.
template <typename F>
inline void writeProbablyQuotedStringImpl(const String & s, WriteBuffer & buf, F && write_quoted_string)
{
    if (s.empty() || !isValidIdentifierBegin(s[0]))
        write_quoted_string(s, buf);
    else
    {
        const char * pos = s.data() + 1;
        const char * end = s.data() + s.size();
        for (; pos < end; ++pos)
            if (!isWordCharASCII(*pos))
                break;
        if (pos != end)
            write_quoted_string(s, buf);
        else
            writeString(s, buf);
    }
}

inline void writeProbablyBackQuotedString(const String & s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const String & s, WriteBuffer & buf) { return writeBackQuotedString(s, buf); });
}

inline void writeProbablyDoubleQuotedString(const String & s, WriteBuffer & buf)
{
    writeProbablyQuotedStringImpl(s, buf, [](const String & s, WriteBuffer & buf) { return writeDoubleQuotedString(s, buf); });
}


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
        else /// Quotation.
        {
            ++next_pos;
            buf.write(pos, next_pos - pos);
            writeChar(quote, buf);
        }

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
void writeCSVString(const StringRef & s, WriteBuffer & buf)
{
    writeCSVString<quote>(s.data, s.data + s.size, buf);
}


/// Writing a string to a text node in XML (not into an attribute - otherwise you need more escaping).
inline void writeXMLString(const char * begin, const char * end, WriteBuffer & buf)
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
        else if (*next_pos == '<')
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

inline void writeXMLString(const String & s, WriteBuffer & buf)
{
    writeXMLString(s.data(), s.data() + s.size(), buf);
}

inline void writeXMLString(const StringRef & s, WriteBuffer & buf)
{
    writeXMLString(s.data, s.data + s.size, buf);
}

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes);
void formatUUID(const UInt8 * src16, UInt8 * dst36);
void formatUUID(std::reverse_iterator<const UInt8 *> dst16, UInt8 * dst36);

inline void writeUUIDText(const UUID & uuid, WriteBuffer & buf)
{
    char s[36];

    formatUUID(std::reverse_iterator<const UInt8 *>(reinterpret_cast<const UInt8 *>(&uuid) + 16), reinterpret_cast<UInt8 *>(s));
    buf.write(s, sizeof(s));
}

/// in YYYY-MM-DD format
template <char delimiter = '-'>
inline void writeDateText(const LocalDate & date, WriteBuffer & buf)
{
    static const char digits[201] =
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

    if (buf.position() + 10 <= buf.buffer().end())
    {
        memcpy(buf.position(), &digits[date.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits[date.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits[date.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits[date.day() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits[date.year() / 100 * 2], 2);
        buf.write(&digits[date.year() % 100 * 2], 2);
        buf.write(delimiter);
        buf.write(&digits[date.month() * 2], 2);
        buf.write(delimiter);
        buf.write(&digits[date.day() * 2], 2);
    }
}

template <char delimiter = '-'>
inline void writeDateText(DayNum date, WriteBuffer & buf)
{
    if (unlikely(!date))
    {
        static const char s[] = {'0', '0', '0', '0', delimiter, '0', '0', delimiter, '0', '0'};
        buf.write(s, sizeof(s));
        return;
    }

    writeDateText<delimiter>(LocalDate(date), buf);
}


/// In the format YYYY-MM-DD HH:MM:SS
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(const LocalDateTime & datetime, WriteBuffer & buf)
{
    static const char digits[201] =
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

    if (buf.position() + 19 <= buf.buffer().end())
    {
        memcpy(buf.position(), &digits[datetime.year() / 100 * 2], 2);
        buf.position() += 2;
        memcpy(buf.position(), &digits[datetime.year() % 100 * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits[datetime.month() * 2], 2);
        buf.position() += 2;
        *buf.position() = date_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits[datetime.day() * 2], 2);
        buf.position() += 2;
        *buf.position() = between_date_time_delimiter;
        ++buf.position();
        memcpy(buf.position(), &digits[datetime.hour() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits[datetime.minute() * 2], 2);
        buf.position() += 2;
        *buf.position() = time_delimeter;
        ++buf.position();
        memcpy(buf.position(), &digits[datetime.second() * 2], 2);
        buf.position() += 2;
    }
    else
    {
        buf.write(&digits[datetime.year() / 100 * 2], 2);
        buf.write(&digits[datetime.year() % 100 * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits[datetime.month() * 2], 2);
        buf.write(date_delimeter);
        buf.write(&digits[datetime.day() * 2], 2);
        buf.write(between_date_time_delimiter);
        buf.write(&digits[datetime.hour() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits[datetime.minute() * 2], 2);
        buf.write(time_delimeter);
        buf.write(&digits[datetime.second() * 2], 2);
    }
}

/// In the format YYYY-MM-DD HH:MM:SS, according to the specified time zone.
template <char date_delimeter = '-', char time_delimeter = ':', char between_date_time_delimiter = ' '>
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    if (unlikely(!datetime))
    {
        static const char s[] =
        {
            '0', '0', '0', '0', date_delimeter, '0', '0', date_delimeter, '0', '0',
            between_date_time_delimiter,
            '0', '0', time_delimeter, '0', '0', time_delimeter, '0', '0'
        };
        buf.write(s, sizeof(s));
        return;
    }

    const auto & values = date_lut.getValues(datetime);
    writeDateTimeText<date_delimeter, time_delimeter, between_date_time_delimiter>(
        LocalDateTime(values.year, values.month, values.day_of_month,
            date_lut.toHour(datetime), date_lut.toMinute(datetime), date_lut.toSecond(datetime)), buf);
}


/// Methods for output in binary format.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void>
writeBinary(const T & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const String & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const StringRef & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const UInt128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDate & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDateTime & x, WriteBuffer & buf) { writePODBinary(x, buf); }


/// Methods for outputting the value in text form for a tab-separated format.
template <typename T>
inline std::enable_if_t<std::is_integral_v<T>, void>
writeText(const T & x, WriteBuffer & buf) { writeIntText(x, buf); }

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, void>
writeText(const T & x, WriteBuffer & buf) { writeFloatText(x, buf); }

inline void writeText(const String & x, WriteBuffer & buf) { writeEscapedString(x, buf); }

/// Implemented as template specialization (not function overload) to avoid preference over templates on arithmetic types above.
template <> inline void writeText<bool>(const bool & x, WriteBuffer & buf) { writeBoolText(x, buf); }

/// unlike the method for std::string
/// assumes here that `x` is a null-terminated string.
inline void writeText(const char * x, WriteBuffer & buf) { writeEscapedString(x, strlen(x), buf); }
inline void writeText(const char * x, size_t size, WriteBuffer & buf) { writeEscapedString(x, size, buf); }

inline void writeText(const LocalDate & x, WriteBuffer & buf) { writeDateText(x, buf); }
inline void writeText(const LocalDateTime & x, WriteBuffer & buf) { writeDateTimeText(x, buf); }
inline void writeText(const UUID & x, WriteBuffer & buf) { writeUUIDText(x, buf); }
inline void writeText(const UInt128 &, WriteBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be write as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// String, date, datetime are in single quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void>
writeQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeQuoted(const String & x, WriteBuffer & buf) { writeQuotedString(x, buf); }

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


/// String, date, datetime are in double quotes with C-style escaping. Numbers - without.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void>
writeDoubleQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeDoubleQuoted(const String & x, WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

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


/// String - in double quotes and with CSV-escaping; date, datetime - in double quotes. Numbers - without.
template <typename T>
inline std::enable_if_t<std::is_arithmetic_v<T>, void>
writeCSV(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeCSV(const String & x, WriteBuffer & buf) { writeCSVString<>(x, buf); }
inline void writeCSV(const LocalDate & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const LocalDateTime & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UUID & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UInt128, WriteBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be write as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

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
void writeException(const Exception & e, WriteBuffer & buf);


/// An easy-to-use method for converting something to a string in text form.
template <typename T>
inline String toString(const T & x)
{
    WriteBufferFromOwnString buf;
    writeText(x, buf);
    return buf.str();
}

}
