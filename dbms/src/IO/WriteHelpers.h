#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <common/find_first_symbols.h>

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/UInt128.h>
#include <common/StringRef.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteIntText.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/DoubleConverter.h>


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


inline void writeFloatText(double x, WriteBuffer & buf)
{
    DoubleConverter<false>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<false>::instance().ToShortest(x, &builder);

    if (!result)
        throw Exception("Cannot print double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

    buf.write(buffer, builder.position());
}

inline void writeFloatText(float x, WriteBuffer & buf)
{
    DoubleConverter<false>::BufferType buffer;
    double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

    const auto result = DoubleConverter<false>::instance().ToShortestSingle(x, &builder);

    if (!result)
        throw Exception("Cannot print float number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

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
 *  - the string is outputted in double quotes
 *  - forward slash character '/' is escaped
 *  - bytes from the range 0x00-0x1F except `\b', '\f', '\n', '\r', '\t' are escaped as \u00XX
 *  - code points U+2028 and U+2029 (byte sequences in UTF-8: e2 80 a8, e2 80 a9) are escaped as \u2028 and \u2029
 *  - it is assumed that string is the UTF-8 encoded, the invalid UTF-8 is not processed
 *  - non-ASCII characters remain as is
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
                if (0x00 <= *it && *it <= 0x1F)
                {
                    char higher_half = (*it) >> 4;
                    char lower_half = (*it) & 0xF;

                    writeCString("\\u00", buf);
                    writeChar('0' + higher_half, buf);

                    if (0 <= lower_half && lower_half <= 9)
                        writeChar('0' + lower_half, buf);
                    else
                        writeChar('A' + lower_half - 10, buf);
                }
                else if (end - it >= 3 && it[0] == '\xE2' && it[1] == '\x80' && (it[2] == '\xA8' || it[2] == '\xA9'))
                {
                    if (it[2] == '\xA8')
                        writeCString("\\u2028", buf);
                    if (it[2] == '\xA9')
                        writeCString("\\u2029", buf);
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

/// The same, but backquotes apply only if there are characters that do not match the identifier without backquotes.
inline void writeProbablyBackQuotedString(const String & s, WriteBuffer & buf)
{
    if (s.empty() || !isValidIdentifierBegin(s[0]))
        writeBackQuotedString(s, buf);
    else
    {
        const char * pos = s.data() + 1;
        const char * end = s.data() + s.size();
        for (; pos < end; ++pos)
            if (!isWordCharASCII(*pos))
                break;
        if (pos != end)
            writeBackQuotedString(s, buf);
        else
            writeString(s, buf);
    }
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
        else                        /// Quotation.
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

void formatHex(const UInt8 * __restrict src, UInt8 * __restrict dst, const size_t num_bytes);
void formatUUID(const UInt8 * src16, UInt8 * dst36);
void formatUUID(const UInt128 & uuid, UInt8 * dst36);

inline void writeUUIDText(const UUID & uuid, WriteBuffer & buf)
{
    char s[36];

    formatUUID(uuid, reinterpret_cast<UInt8 *>(s));
    buf.write(s, sizeof(s));
}

/// in YYYY-MM-DD format
inline void writeDateText(DayNum_t date, WriteBuffer & buf)
{
    char s[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};

    if (unlikely(date > DATE_LUT_MAX_DAY_NUM || date == 0))
    {
        buf.write(s, 10);
        return;
    }

    const auto & values = DateLUT::instance().getValues(date);

    s[0] += values.year / 1000;
    s[1] += (values.year / 100) % 10;
    s[2] += (values.year / 10) % 10;
    s[3] += values.year % 10;
    s[5] += values.month / 10;
    s[6] += values.month % 10;
    s[8] += values.day_of_month / 10;
    s[9] += values.day_of_month % 10;

    buf.write(s, 10);
}

inline void writeDateText(LocalDate date, WriteBuffer & buf)
{
    char s[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};

    s[0] += date.year() / 1000;
    s[1] += (date.year() / 100) % 10;
    s[2] += (date.year() / 10) % 10;
    s[3] += date.year() % 10;
    s[5] += date.month() / 10;
    s[6] += date.month() % 10;
    s[8] += date.day() / 10;
    s[9] += date.day() % 10;

    buf.write(s, 10);
}


/// in the format YYYY-MM-DD HH:MM:SS, according to the current time zone
template <char date_delimeter = '-', char time_delimeter = ':'>
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    char s[19] = {'0', '0', '0', '0', date_delimeter, '0', '0', date_delimeter, '0', '0', ' ', '0', '0', time_delimeter, '0', '0', time_delimeter, '0', '0'};

    if (unlikely(datetime > DATE_LUT_MAX || datetime == 0))
    {
        buf.write(s, 19);
        return;
    }

    const auto & values = date_lut.getValues(datetime);

    s[0] += values.year / 1000;
    s[1] += (values.year / 100) % 10;
    s[2] += (values.year / 10) % 10;
    s[3] += values.year % 10;
    s[5] += values.month / 10;
    s[6] += values.month % 10;
    s[8] += values.day_of_month / 10;
    s[9] += values.day_of_month % 10;

    UInt8 hour = date_lut.toHour(datetime);
    UInt8 minute = date_lut.toMinuteInaccurate(datetime);
    UInt8 second = date_lut.toSecondInaccurate(datetime);

    s[11] += hour / 10;
    s[12] += hour % 10;
    s[14] += minute / 10;
    s[15] += minute % 10;
    s[17] += second / 10;
    s[18] += second % 10;

    buf.write(s, 19);
}

template <char date_delimeter = '-', char time_delimeter = ':'>
inline void writeDateTimeText(LocalDateTime datetime, WriteBuffer & buf)
{
    char s[19] = {'0', '0', '0', '0', date_delimeter, '0', '0', date_delimeter, '0', '0', ' ', '0', '0', time_delimeter, '0', '0', time_delimeter, '0', '0'};

    s[0] += datetime.year() / 1000;
    s[1] += (datetime.year() / 100) % 10;
    s[2] += (datetime.year() / 10) % 10;
    s[3] += datetime.year() % 10;
    s[5] += datetime.month() / 10;
    s[6] += datetime.month() % 10;
    s[8] += datetime.day() / 10;
    s[9] += datetime.day() % 10;

    s[11] += datetime.hour() / 10;
    s[12] += datetime.hour() % 10;
    s[14] += datetime.minute() / 10;
    s[15] += datetime.minute() % 10;
    s[17] += datetime.second() / 10;
    s[18] += datetime.second() % 10;

    buf.write(s, 19);
}


/// Methods of output in binary form
template <typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
writeBinary(const T & x, WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const String & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const StringRef & x, WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const UInt128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDate & x, WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const LocalDateTime & x, WriteBuffer & buf) { writePODBinary(x, buf); }


/// Methods for outputting the value in text form for a tab-separated format.
template <typename T>
inline typename std::enable_if<std::is_integral<T>::value, void>::type
writeText(const T & x, WriteBuffer & buf) { writeIntText(x, buf); }

template <typename T>
inline typename std::enable_if<std::is_floating_point<T>::value, void>::type
writeText(const T & x, WriteBuffer & buf) { writeFloatText(x, buf); }

inline void writeText(const String & x,        WriteBuffer & buf) { writeEscapedString(x, buf); }

/// Implemented as template specialization (not function overload) to avoid preference over templates on arithmetic types above.
template <> inline void writeText<bool>(const bool & x, WriteBuffer & buf) { writeBoolText(x, buf); }

/// unlike the method for std::string
/// assumes here that `x` is a null-terminated string.
inline void writeText(const char * x,         WriteBuffer & buf) { writeEscapedString(x, strlen(x), buf); }
inline void writeText(const char * x, size_t size, WriteBuffer & buf) { writeEscapedString(x, size, buf); }

inline void writeText(const LocalDate & x,        WriteBuffer & buf) { writeDateText(x, buf); }
inline void writeText(const LocalDateTime & x,    WriteBuffer & buf) { writeDateTimeText(x, buf); }
inline void writeText(const UUID & x, WriteBuffer & buf) { writeUUIDText(x, buf); }
inline void writeText(const UInt128 & x, WriteBuffer & buf)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be write as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// String, date, datetime are in single quotes with C-style escaping. Numbers - without.
template <typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
writeQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeQuoted(const String & x,    WriteBuffer & buf) { writeQuotedString(x, buf); }

inline void writeQuoted(const LocalDate & x,        WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateText(x, buf);
    writeChar('\'', buf);
}

inline void writeQuoted(const LocalDateTime & x,    WriteBuffer & buf)
{
    writeChar('\'', buf);
    writeDateTimeText(x, buf);
    writeChar('\'', buf);
}


/// String, date, datetime are in double quotes with C-style escaping. Numbers - without.
template <typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
writeDoubleQuoted(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeDoubleQuoted(const String & x,        WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }

inline void writeDoubleQuoted(const LocalDate & x,        WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateText(x, buf);
    writeChar('"', buf);
}

inline void writeDoubleQuoted(const LocalDateTime & x,    WriteBuffer & buf)
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
inline typename std::enable_if<std::is_arithmetic<T>::value, void>::type
writeCSV(const T & x, WriteBuffer & buf) { writeText(x, buf); }

inline void writeCSV(const String & x,        WriteBuffer & buf) { writeCSVString<>(x, buf); }
inline void writeCSV(const LocalDate & x,    WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const LocalDateTime & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UUID & x, WriteBuffer & buf) { writeDoubleQuoted(x, buf); }
inline void writeCSV(const UInt128, WriteBuffer & buf)
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
    String res;
    {
        WriteBufferFromString buf(res);
        writeText(x, buf);
    }
    return res;
}

}
