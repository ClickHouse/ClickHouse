#pragma once

#include <cmath>
#include <cstring>
#include <string>
#include <string_view>
#include <limits>
#include <algorithm>
#include <iterator>

#include <type_traits>

#include <Common/DateLUT.h>
#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <base/StringRef.h>
#include <base/arithmeticOverflow.h>
#include <base/unit.h>

#include <Core/Types.h>
#include <Core/DecimalFunctions.h>
#include <Core/UUID.h>

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Arena.h>
#include <Common/intExp.h>

#include <Formats/FormatSettings.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/VarInt.h>

#include <DataTypes/DataTypeDateTime.h>

#include <double-conversion/double-conversion.h>

static constexpr auto DEFAULT_MAX_STRING_SIZE = 1_GiB;

namespace DB
{

template <typename Allocator>
struct Memory;

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int INCORRECT_DATA;
}

/// Helper functions for formatted input.

inline char parseEscapeSequence(char c)
{
    switch (c)
    {
        case 'a':
            return '\a';
        case 'b':
            return '\b';
        case 'e':
            return '\x1B';      /// \e escape sequence is non standard for C and C++ but supported by gcc and clang.
        case 'f':
            return '\f';
        case 'n':
            return '\n';
        case 'r':
            return '\r';
        case 't':
            return '\t';
        case 'v':
            return '\v';
        case '0':
            return '\0';
        default:
            return c;
    }
}


/// These functions are located in VarInt.h
/// inline void throwReadAfterEOF()


inline void readChar(char & x, ReadBuffer & buf)
{
    if (!buf.eof())
    {
        x = *buf.position();
        ++buf.position();
    }
    else
        throwReadAfterEOF();
}


/// Read POD-type in native format
template <typename T>
inline void readPODBinary(T & x, ReadBuffer & buf)
{
    buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x)); /// NOLINT
}

template <typename T>
inline void readIntBinary(T & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}

template <typename T>
inline void readFloatBinary(T & x, ReadBuffer & buf)
{
    readPODBinary(x, buf);
}

inline void readStringBinary(std::string & s, ReadBuffer & buf, size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_STRING_SIZE)
        throw Poco::Exception("Too large string size.");

    s.resize(size);
    buf.readStrict(s.data(), size);
}


inline StringRef readStringBinaryInto(Arena & arena, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    char * data = arena.alloc(size);
    buf.readStrict(data, size);

    return StringRef(data, size);
}


template <typename T>
void readVectorBinary(std::vector<T> & v, ReadBuffer & buf, size_t MAX_VECTOR_SIZE = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > MAX_VECTOR_SIZE)
        throw Poco::Exception("Too large vector size.");

    v.resize(size);
    for (size_t i = 0; i < size; ++i)
        readBinary(v[i], buf);
}


void assertString(const char * s, ReadBuffer & buf);
void assertEOF(ReadBuffer & buf);
void assertNotEOF(ReadBuffer & buf);

[[noreturn]] void throwAtAssertionFailed(const char * s, ReadBuffer & buf);

inline bool checkChar(char c, ReadBuffer & buf)  // -V1071
{
    char a;
    if (!buf.peek(a) || a != c)
        return false;
    buf.ignore();
    return true;
}

inline void assertChar(char symbol, ReadBuffer & buf)
{
    if (!checkChar(symbol, buf))
    {
        char err[2] = {symbol, '\0'};
        throwAtAssertionFailed(err, buf);
    }
}

inline bool checkCharCaseInsensitive(char c, ReadBuffer & buf)
{
    char a;
    if (!buf.peek(a) || !equalsCaseInsensitive(a, c))
        return false;
    buf.ignore();
    return true;
}

inline void assertString(const String & s, ReadBuffer & buf)
{
    assertString(s.c_str(), buf);
}

bool checkString(const char * s, ReadBuffer & buf);
inline bool checkString(const String & s, ReadBuffer & buf)
{
    return checkString(s.c_str(), buf);
}

bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline bool checkStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return checkStringCaseInsensitive(s.c_str(), buf);
}

void assertStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline void assertStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return assertStringCaseInsensitive(s.c_str(), buf);
}

/** Check that next character in buf matches first character of s.
  * If true, then check all characters in s and throw exception if it doesn't match.
  * If false, then return false, and leave position in buffer unchanged.
  */
bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf);
bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf);

inline bool checkStringByFirstCharacterAndAssertTheRest(const String & s, ReadBuffer & buf)
{
    return checkStringByFirstCharacterAndAssertTheRest(s.c_str(), buf);
}

inline bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const String & s, ReadBuffer & buf)
{
    return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(s.c_str(), buf);
}


inline void readBoolText(bool & x, ReadBuffer & buf)
{
    char tmp = '0';
    readChar(tmp, buf);
    x = tmp != '0';
}

inline void readBoolTextWord(bool & x, ReadBuffer & buf, bool support_upper_case = false)
{
    if (buf.eof())
        throwReadAfterEOF();

    switch (*buf.position())
    {
        case 't':
            assertString("true", buf);
            x = true;
            break;
        case 'f':
            assertString("false", buf);
            x = false;
            break;
        case 'T':
        {
            if (support_upper_case)
            {
                assertString("TRUE", buf);
                x = true;
                break;
            }
            else
                [[fallthrough]];
        }
        case 'F':
        {
            if (support_upper_case)
            {
                assertString("FALSE", buf);
                x = false;
                break;
            }
            else
                [[fallthrough]];
        }
        default:
            throw ParsingException("Unexpected Bool value", ErrorCodes::CANNOT_PARSE_BOOL);
    }
}

enum class ReadIntTextCheckOverflow
{
    DO_NOT_CHECK_OVERFLOW,
    CHECK_OVERFLOW,
};

template <typename T, typename ReturnType = void, ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW>
ReturnType readIntTextImpl(T & x, ReadBuffer & buf)
{
    using UnsignedT = make_unsigned_t<T>;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    UnsignedT res{};
    if (buf.eof())
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        else
            return ReturnType(false);
    }

    const size_t initial_pos = buf.count();
    bool has_sign = false;
    bool has_number = false;
    while (!buf.eof())
    {
        switch (*buf.position())
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
                        throw ParsingException(
                            "Cannot parse number with multiple sign (+/-) characters",
                            ErrorCodes::CANNOT_PARSE_NUMBER);
                    else
                        return ReturnType(false);
                }

                has_sign = true;
                break;
            }
            case '-':
            {
                if (has_number)
                    goto end;

                if (has_sign)
                {
                    if constexpr (throw_exception)
                        throw ParsingException(
                            "Cannot parse number with multiple sign (+/-) characters",
                            ErrorCodes::CANNOT_PARSE_NUMBER);
                    else
                        return ReturnType(false);
                }

                if constexpr (is_signed_v<T>)
                    negative = true;
                else
                {
                    if constexpr (throw_exception)
                        throw ParsingException("Unsigned type must not contain '-' symbol", ErrorCodes::CANNOT_PARSE_NUMBER);
                    else
                        return ReturnType(false);
                }
                has_sign = true;
                break;
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
                has_number = true;
                if constexpr (check_overflow == ReadIntTextCheckOverflow::CHECK_OVERFLOW && !is_big_int_v<T>)
                {
                    /// Perform relativelly slow overflow check only when
                    /// number of decimal digits so far is close to the max for given type.
                    /// Example: 20 * 10 will overflow Int8.

                    if (buf.count() - initial_pos + 1 >= std::numeric_limits<T>::max_digits10)
                    {
                        if (negative)
                        {
                            T signed_res = -res;
                            if (common::mulOverflow<T>(signed_res, 10, signed_res) ||
                                common::subOverflow<T>(signed_res, (*buf.position() - '0'), signed_res))
                                return ReturnType(false);

                            res = -static_cast<UnsignedT>(signed_res);
                        }
                        else
                        {
                            T signed_res = res;
                            if (common::mulOverflow<T>(signed_res, 10, signed_res) ||
                                common::addOverflow<T>(signed_res, (*buf.position() - '0'), signed_res))
                                return ReturnType(false);

                            res = signed_res;
                        }
                        break;
                    }
                }
                res *= 10;
                res += *buf.position() - '0';
                break;
            }
            default:
                goto end;
        }
        ++buf.position();
    }

end:
    if (has_sign && !has_number)
    {
        if constexpr (throw_exception)
            throw ParsingException(
                "Cannot parse number with a sign character but without any numeric character", ErrorCodes::CANNOT_PARSE_NUMBER);
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
                    return ReturnType(false);
            }
            else
                x = -res;
        }
    }

    return ReturnType(true);
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW, typename T>
void readIntText(T & x, ReadBuffer & buf)
{
    if constexpr (is_decimal<T>)
    {
        readIntText<check_overflow>(x.value, buf);
    }
    else
    {
        readIntTextImpl<T, void, check_overflow>(x, buf);
    }
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryReadIntText(T & x, ReadBuffer & buf)  // -V1071
{
    return readIntTextImpl<T, bool, check_overflow>(x, buf);
}


/** More efficient variant (about 1.5 times on real dataset).
  * Differs in following:
  * - for numbers starting with zero, parsed only zero;
  * - symbol '+' before number is not supported;
  */
template <typename T, bool throw_on_error = true>
void readIntTextUnsafe(T & x, ReadBuffer & buf)
{
    bool negative = false;
    make_unsigned_t<T> res = 0;

    auto on_error = []
    {
        if (throw_on_error)
            throwReadAfterEOF();
    };

    if (unlikely(buf.eof()))
        return on_error();

    if (is_signed_v<T> && *buf.position() == '-')
    {
        ++buf.position();
        negative = true;
        if (unlikely(buf.eof()))
            return on_error();
    }

    if (*buf.position() == '0') /// There are many zeros in real datasets.
    {
        ++buf.position();
        x = 0;
        return;
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
    x = is_signed_v<T> && negative ? -res : res;
}

template <typename T>
void tryReadIntTextUnsafe(T & x, ReadBuffer & buf)
{
    return readIntTextUnsafe<T, false>(x, buf);
}


/// Look at readFloatText.h
template <typename T> void readFloatText(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatText(T & x, ReadBuffer & in);


/// simple: all until '\n' or '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);
void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);
void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readJSONString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);
void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readStringUntilEOF(String & s, ReadBuffer & buf);

// Reads the line until EOL, unescaping backslash escape sequences.
// Buffer pointer is left at EOL, don't forget to advance it.
void readEscapedStringUntilEOL(String & s, ReadBuffer & buf);

/// Only 0x20 as whitespace character
void readStringUntilWhitespace(String & s, ReadBuffer & buf);


/** Read string in CSV format.
  * Parsing rules:
  * - string could be placed in quotes; quotes could be single: ' if FormatSettings::CSV::allow_single_quotes is true
  *   or double: " if FormatSettings::CSV::allow_double_quotes is true;
  * - or string could be unquoted - this is determined by first character;
  * - if string is unquoted, then it is read until next delimiter,
  *   either until end of line (CR or LF),
  *   or until end of stream;
  *   but spaces and tabs at begin and end of unquoted string are consumed but ignored (note that this behaviour differs from RFC).
  * - if string is in quotes, then it will be read until closing quote,
  *   but sequences of two consecutive quotes are parsed as single quote inside string;
  */
void readCSVString(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// Differ from readCSVString in that it doesn't remove quotes around field if any.
void readCSVField(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// Read and append result to array of characters.
template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readNullTerminated(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// ReturnType is either bool or void. If bool, the function will return false instead of throwing an exception.
template <typename Vector, typename ReturnType = void>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
bool tryReadJSONStringInto(Vector & s, ReadBuffer & buf)
{
    return readJSONStringInto<Vector, bool>(s, buf);
}

/// Reads chunk of data between {} in that way,
/// that it has balanced parentheses sequence of {}.
/// So, it may form a JSON object, but it can be incorrenct.
template <typename Vector, typename ReturnType = void>
ReturnType readJSONObjectPossiblyInvalid(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilWhitespaceInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilNewlineInto(Vector & s, ReadBuffer & buf);

/// This could be used as template parameter for functions above, if you want to just skip data.
struct NullOutput
{
    void append(const char *, size_t) {}
    void append(const char *) {}
    void append(const char *, const char *) {}
    void push_back(char) {} /// NOLINT
};

void parseUUID(const UInt8 * src36, UInt8 * dst16);
void parseUUIDWithoutSeparator(const UInt8 * src36, UInt8 * dst16);
void parseUUID(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16);
void parseUUIDWithoutSeparator(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16);


template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf);

/// In YYYY-MM-DD format.
/// For convenience, Month and Day parts can have single digit instead of two digits.
/// Any separators other than '-' are supported.
template <typename ReturnType = void>
inline ReturnType readDateTextImpl(LocalDate & date, ReadBuffer & buf)
{
    /// Optimistic path, when whole value is in buffer.
    if (!buf.eof() && buf.position() + 10 <= buf.buffer().end())
    {
        char * pos = buf.position();

        /// YYYY-MM-DD
        /// YYYY-MM-D
        /// YYYY-M-DD
        /// YYYY-M-D
        /// YYYYMMDD

        /// The delimiters can be arbitrary characters, like YYYY/MM!DD, but obviously not digits.

        UInt16 year = (pos[0] - '0') * 1000 + (pos[1] - '0') * 100 + (pos[2] - '0') * 10 + (pos[3] - '0');
        UInt8 month;
        UInt8 day;
        pos += 5;

        if (isNumericASCII(pos[-1]))
        {
            /// YYYYMMDD
            month = (pos[-1] - '0') * 10 + (pos[0] - '0');
            day = (pos[1] - '0') * 10 + (pos[2] - '0');
            pos += 3;
        }
        else
        {
            month = pos[0] - '0';
            if (isNumericASCII(pos[1]))
            {
                month = month * 10 + pos[1] - '0';
                pos += 3;
            }
            else
                pos += 2;

            if (isNumericASCII(pos[-1]))
                return ReturnType(false);

            day = pos[0] - '0';
            if (isNumericASCII(pos[1]))
            {
                day = day * 10 + pos[1] - '0';
                pos += 2;
            }
            else
                pos += 1;
        }

        buf.position() = pos;
        date = LocalDate(year, month, day);
        return ReturnType(true);
    }
    else
        return readDateTextFallback<ReturnType>(date, buf);
}

inline void convertToDayNum(DayNum & date, ExtendedDayNum & from)
{
    if (unlikely(from < 0))
        date = 0;
    else if (unlikely(from > 0xFFFF))
        date = 0xFFFF;
    else
        date = from;
}

template <typename ReturnType = void>
inline ReturnType readDateTextImpl(DayNum & date, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    LocalDate local_date;

    if constexpr (throw_exception)
        readDateTextImpl<ReturnType>(local_date, buf);
    else if (!readDateTextImpl<ReturnType>(local_date, buf))
        return false;

    ExtendedDayNum ret = DateLUT::instance().makeDayNum(local_date.year(), local_date.month(), local_date.day());
    convertToDayNum(date,ret);
    return ReturnType(true);
}

template <typename ReturnType = void>
inline ReturnType readDateTextImpl(ExtendedDayNum & date, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    LocalDate local_date;

    if constexpr (throw_exception)
        readDateTextImpl<ReturnType>(local_date, buf);
    else if (!readDateTextImpl<ReturnType>(local_date, buf))
        return false;
    /// When the parameter is out of rule or out of range, Date32 uses 1925-01-01 as the default value (-DateLUT::instance().getDayNumOffsetEpoch(), -16436) and Date uses 1970-01-01.
    date = DateLUT::instance().makeDayNum(local_date.year(), local_date.month(), local_date.day(), -static_cast<Int32>(DateLUT::instance().getDayNumOffsetEpoch()));
    return ReturnType(true);
}


inline void readDateText(LocalDate & date, ReadBuffer & buf)
{
    readDateTextImpl<void>(date, buf);
}

inline void readDateText(DayNum & date, ReadBuffer & buf)
{
    readDateTextImpl<void>(date, buf);
}

inline void readDateText(ExtendedDayNum & date, ReadBuffer & buf)
{
    readDateTextImpl<void>(date, buf);
}

inline bool tryReadDateText(LocalDate & date, ReadBuffer & buf)
{
    return readDateTextImpl<bool>(date, buf);
}

inline bool tryReadDateText(DayNum & date, ReadBuffer & buf)
{
    return readDateTextImpl<bool>(date, buf);
}

inline bool tryReadDateText(ExtendedDayNum & date, ReadBuffer & buf)
{
    return readDateTextImpl<bool>(date, buf);
}

template <typename ReturnType = void>
inline ReturnType readUUIDTextImpl(UUID & uuid, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    char s[36];
    size_t size = buf.read(s, 32);

    if (size == 32)
    {
        if (s[8] == '-')
        {
            size += buf.read(&s[32], 4);

            if (size != 36)
            {
                s[size] = 0;

                if constexpr (throw_exception)
                {
                    throw ParsingException(std::string("Cannot parse uuid ") + s, ErrorCodes::CANNOT_PARSE_UUID);
                }
                else
                {
                    return ReturnType(false);
                }
            }

            parseUUID(reinterpret_cast<const UInt8 *>(s), std::reverse_iterator<UInt8 *>(reinterpret_cast<UInt8 *>(&uuid) + 16));
        }
        else
            parseUUIDWithoutSeparator(reinterpret_cast<const UInt8 *>(s), std::reverse_iterator<UInt8 *>(reinterpret_cast<UInt8 *>(&uuid) + 16));

        return ReturnType(true);
    }
    else
    {
        s[size] = 0;

        if constexpr (throw_exception)
        {
            throw ParsingException(std::string("Cannot parse uuid ") + s, ErrorCodes::CANNOT_PARSE_UUID);
        }
        else
        {
            return ReturnType(false);
        }
    }
}

inline void readUUIDText(UUID & uuid, ReadBuffer & buf)
{
    return readUUIDTextImpl<void>(uuid, buf);
}

inline bool tryReadUUIDText(UUID & uuid, ReadBuffer & buf)
{
    return readUUIDTextImpl<bool>(uuid, buf);
}


template <typename T>
inline T parse(const char * data, size_t size);

template <typename T>
inline T parseFromString(std::string_view str)
{
    return parse<T>(str.data(), str.size());
}


template <typename ReturnType = void>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut);

/** In YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format, according to specified time zone.
  * As an exception, also supported parsing of unix timestamp in form of decimal number.
  */
template <typename ReturnType = void>
inline ReturnType readDateTimeTextImpl(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    /// Optimistic path, when whole value is in buffer.
    const char * s = buf.position();

    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto DateTimeStringInputSize = 19;
    ///YYYY-MM-DD
    static constexpr auto DateStringInputSize = 10;
    bool optimistic_path_for_date_time_input = s + DateTimeStringInputSize <= buf.buffer().end();

    if (optimistic_path_for_date_time_input)
    {
        if (s[4] < '0' || s[4] > '9')
        {
            UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
            UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
            UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

            UInt8 hour = 0;
            UInt8 minute = 0;
            UInt8 second = 0;
            ///simply determine whether it is YYYY-MM-DD hh:mm:ss or YYYY-MM-DD by the content of the tenth character in an optimistic scenario
            bool dt_long = (s[10] == ' ' || s[10] == 'T');
            if (dt_long)
            {
                hour = (s[11] - '0') * 10 + (s[12] - '0');
                minute = (s[14] - '0') * 10 + (s[15] - '0');
                second = (s[17] - '0') * 10 + (s[18] - '0');
            }

            if (unlikely(year == 0))
                datetime = 0;
            else
                datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);

            if (dt_long)
                buf.position() += DateTimeStringInputSize;
            else
                buf.position() += DateStringInputSize;
            return ReturnType(true);
        }
        else
            /// Why not readIntTextUnsafe? Because for needs of AdFox, parsing of unix timestamp with leading zeros is supported: 000...NNNN.
            return readIntTextImpl<time_t, ReturnType, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(datetime, buf);
    }
    else
        return readDateTimeTextFallback<ReturnType>(datetime, buf, date_lut);
}

template <typename ReturnType>
inline ReturnType readDateTimeTextImpl(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    time_t whole;
    if (!readDateTimeTextImpl<bool>(whole, buf, date_lut))
    {
        return ReturnType(false);
    }

    int negative_multiplier = 1;

    DB::DecimalUtils::DecimalComponents<DateTime64> components{static_cast<DateTime64::NativeType>(whole), 0};

    if (!buf.eof() && *buf.position() == '.')
    {
        ++buf.position();

        /// Read digits, up to 'scale' positions.
        for (size_t i = 0; i < scale; ++i)
        {
            if (!buf.eof() && isNumericASCII(*buf.position()))
            {
                components.fractional *= 10;
                components.fractional += *buf.position() - '0';
                ++buf.position();
            }
            else
            {
                /// Adjust to scale.
                components.fractional *= 10;
            }
        }

        /// Ignore digits that are out of precision.
        while (!buf.eof() && isNumericASCII(*buf.position()))
            ++buf.position();

        /// Fractional part (subseconds) is treated as positive by users
        /// (as DateTime64 itself is a positive, although underlying decimal is negative)
        /// setting fractional part to be negative when whole is 0 results in wrong value,
        /// so we multiply result by -1.
        if (components.whole < 0 && components.fractional != 0)
        {
            const auto scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale);
            ++components.whole;
            components.fractional = scale_multiplier - components.fractional;
            if (!components.whole)
            {
                negative_multiplier = -1;
            }
        }
    }
    /// 9908870400 is time_t value for 2184-01-01 UTC (a bit over the last year supported by DateTime64)
    else if (whole >= 9908870400LL)
    {
        /// Unix timestamp with subsecond precision, already scaled to integer.
        /// For disambiguation we support only time since 2001-09-09 01:46:40 UTC and less than 30 000 years in future.
        components.fractional =  components.whole % common::exp10_i32(scale);
        components.whole = components.whole / common::exp10_i32(scale);
    }

    datetime64 = negative_multiplier * DecimalUtils::decimalFromComponents<DateTime64>(components, scale);

    return ReturnType(true);
}

inline void readDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime, buf, time_zone);
}

inline void readDateTime64Text(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime64, scale, buf, date_lut);
}

inline bool tryReadDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    return readDateTimeTextImpl<bool>(datetime, buf, time_zone);
}

inline bool tryReadDateTime64Text(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    return readDateTimeTextImpl<bool>(datetime64, scale, buf, date_lut);
}

inline void readDateTimeText(LocalDateTime & datetime, ReadBuffer & buf)
{
    char s[19];
    size_t size = buf.read(s, 19);
    if (19 != size)
    {
        s[size] = 0;
        throw ParsingException(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
    }

    datetime.year((s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0'));
    datetime.month((s[5] - '0') * 10 + (s[6] - '0'));
    datetime.day((s[8] - '0') * 10 + (s[9] - '0'));

    datetime.hour((s[11] - '0') * 10 + (s[12] - '0'));
    datetime.minute((s[14] - '0') * 10 + (s[15] - '0'));
    datetime.second((s[17] - '0') * 10 + (s[18] - '0'));
}


/// Generic methods to read value in native binary format.
template <typename T>
requires is_arithmetic_v<T>
inline void readBinary(T & x, ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(String & x, ReadBuffer & buf) { readStringBinary(x, buf); }
inline void readBinary(Int128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Int256 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt256 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal32 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal64 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal256 & x, ReadBuffer & buf) { readPODBinary(x.value, buf); }
inline void readBinary(LocalDate & x, ReadBuffer & buf) { readPODBinary(x, buf); }


template <typename T>
requires is_arithmetic_v<T> && (sizeof(T) <= 8)
inline void readBinaryBigEndian(T & x, ReadBuffer & buf)    /// Assuming little endian architecture.
{
    readPODBinary(x, buf);

    if constexpr (sizeof(x) == 1)
        return;
    else if constexpr (sizeof(x) == 2)
        x = __builtin_bswap16(x);
    else if constexpr (sizeof(x) == 4)
        x = __builtin_bswap32(x);
    else if constexpr (sizeof(x) == 8)
        x = __builtin_bswap64(x);
}

template <typename T>
requires is_big_int_v<T>
inline void readBinaryBigEndian(T & x, ReadBuffer & buf)    /// Assuming little endian architecture.
{
    for (size_t i = 0; i != std::size(x.items); ++i)
    {
        auto & item = x.items[std::size(x.items) - i - 1];
        readBinaryBigEndian(item, buf);
    }
}


/// Generic methods to read value in text tab-separated format.

inline void readText(is_integer auto & x, ReadBuffer & buf)
{
    if constexpr (std::is_same_v<decltype(x), bool &>)
        readBoolText(x, buf);
    else
        readIntText(x, buf);
}

inline bool tryReadText(is_integer auto & x, ReadBuffer & buf)
{
    return tryReadIntText(x, buf);
}

inline bool tryReadText(UUID & x, ReadBuffer & buf) { return tryReadUUIDText(x, buf); }

inline void readText(is_floating_point auto & x, ReadBuffer & buf) { readFloatText(x, buf); }

inline void readText(String & x, ReadBuffer & buf) { readEscapedString(x, buf); }
inline void readText(LocalDate & x, ReadBuffer & buf) { readDateText(x, buf); }
inline void readText(LocalDateTime & x, ReadBuffer & buf) { readDateTimeText(x, buf); }
inline void readText(UUID & x, ReadBuffer & buf) { readUUIDText(x, buf); }

/// Generic methods to read value in text format,
///  possibly in single quotes (only for data types that use quotes in VALUES format of INSERT statement in SQL).
template <typename T>
requires is_arithmetic_v<T>
inline void readQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

inline void readQuoted(String & x, ReadBuffer & buf) { readQuotedString(x, buf); }

inline void readQuoted(LocalDate & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readDateText(x, buf);
    assertChar('\'', buf);
}

inline void readQuoted(LocalDateTime & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readDateTimeText(x, buf);
    assertChar('\'', buf);
}

inline void readQuoted(UUID & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readUUIDText(x, buf);
    assertChar('\'', buf);
}


/// Same as above, but in double quotes.
template <typename T>
requires is_arithmetic_v<T>
inline void readDoubleQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

inline void readDoubleQuoted(String & x, ReadBuffer & buf) { readDoubleQuotedString(x, buf); }

inline void readDoubleQuoted(LocalDate & x, ReadBuffer & buf)
{
    assertChar('"', buf);
    readDateText(x, buf);
    assertChar('"', buf);
}

inline void readDoubleQuoted(LocalDateTime & x, ReadBuffer & buf)
{
    assertChar('"', buf);
    readDateTimeText(x, buf);
    assertChar('"', buf);
}

/// CSV, for numbers, dates: quotes are optional, no special escaping rules.
template <typename T>
inline void readCSVSimple(T & x, ReadBuffer & buf)
{
    if (buf.eof())
        throwReadAfterEOF();

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    readText(x, buf);

    if (maybe_quote == '\'' || maybe_quote == '\"')
        assertChar(maybe_quote, buf);
}

template <typename T>
requires is_arithmetic_v<T>
inline void readCSV(T & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}

inline void readCSV(String & x, ReadBuffer & buf, const FormatSettings::CSV & settings) { readCSVString(x, buf, settings); }
inline void readCSV(LocalDate & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(LocalDateTime & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UUID & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UInt128 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int128 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UInt256 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int256 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }

template <typename T>
void readBinary(std::vector<T> & x, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Poco::Exception("Too large vector size.");

    x.resize(size);
    for (size_t i = 0; i < size; ++i)
        readBinary(x[i], buf);
}

template <typename T>
void readQuoted(std::vector<T> & x, ReadBuffer & buf)
{
    bool first = true;
    assertChar('[', buf);
    while (!buf.eof() && *buf.position() != ']')
    {
        if (!first)
        {
            if (*buf.position() == ',')
                ++buf.position();
            else
                throw ParsingException("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
        }

        first = false;

        x.push_back(T());
        readQuoted(x.back(), buf);
    }
    assertChar(']', buf);
}

template <typename T>
void readDoubleQuoted(std::vector<T> & x, ReadBuffer & buf)
{
    bool first = true;
    assertChar('[', buf);
    while (!buf.eof() && *buf.position() != ']')
    {
        if (!first)
        {
            if (*buf.position() == ',')
                ++buf.position();
            else
                throw ParsingException("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
        }

        first = false;

        x.push_back(T());
        readDoubleQuoted(x.back(), buf);
    }
    assertChar(']', buf);
}

template <typename T>
void readText(std::vector<T> & x, ReadBuffer & buf)
{
    readQuoted(x, buf);
}


/// Skip whitespace characters.
inline void skipWhitespaceIfAny(ReadBuffer & buf, bool one_line = false)
{
    if (!one_line)
        while (!buf.eof() && isWhitespaceASCII(*buf.position()))
            ++buf.position();
    else
        while (!buf.eof() && isWhitespaceASCIIOneLine(*buf.position()))
            ++buf.position();
}

/// Skips json value.
void skipJSONField(ReadBuffer & buf, const StringRef & name_of_field);


/** Read serialized exception.
  * During serialization/deserialization some information is lost
  * (type is cut to base class, 'message' replaced by 'displayText', and stack trace is appended to 'message')
  * Some additional message could be appended to exception (example: you could add information about from where it was received).
  */
Exception readException(ReadBuffer & buf, const String & additional_message = "", bool remote_exception = false);
void readAndThrowException(ReadBuffer & buf, const String & additional_message = "");


/** Helper function for implementation.
  */
template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
static inline const char * tryReadIntText(T & x, const char * pos, const char * end)
{
    ReadBufferFromMemory in(pos, end - pos);
    tryReadIntText<check_overflow>(x, in);
    return pos + in.count();
}


/// Convenient methods for reading something from string in text format.
template <typename T>
inline T parse(const char * data, size_t size)
{
    T res;
    ReadBufferFromMemory buf(data, size);
    readText(res, buf);
    return res;
}

template <typename T>
inline bool tryParse(T & res, const char * data, size_t size)
{
    ReadBufferFromMemory buf(data, size);
    return tryReadText(res, buf);
}

template <typename T>
inline void readTextWithSizeSuffix(T & x, ReadBuffer & buf) { readText(x, buf); }

template <is_integer T>
inline void readTextWithSizeSuffix(T & x, ReadBuffer & buf)
{
    readIntText(x, buf);
    if (buf.eof())
        return;

    /// Updates x depending on the suffix
    auto finish = [&buf, &x] (UInt64 base, int power_of_two) mutable
    {
        ++buf.position();
        if (buf.eof())
        {
            x *= base; /// For decimal suffixes, such as k, M, G etc.
        }
        else if (*buf.position() == 'i')
        {
            x = (x << power_of_two); // NOLINT /// For binary suffixes, such as ki, Mi, Gi, etc.
            ++buf.position();
        }
        return;
    };

    switch (*buf.position())
    {
        case 'k': [[fallthrough]];
        case 'K':
            finish(1000, 10);
            break;
        case 'M':
            finish(1000000, 20);
            break;
        case 'G':
            finish(1000000000, 30);
            break;
        case 'T':
            finish(1000000000000ULL, 40);
            break;
        default:
            return;
    }
}

/// Read something from text format and trying to parse the suffix.
/// If the suffix is not valid gives an error
/// For example: 723145 -- ok, 213MB -- not ok, but 213Mi -- ok
template <typename T>
inline T parseWithSizeSuffix(const char * data, size_t size)
{
    T res;
    ReadBufferFromMemory buf(data, size);
    readTextWithSizeSuffix(res, buf);
    assertEOF(buf);
    return res;
}

template <typename T>
inline T parseWithSizeSuffix(std::string_view s)
{
    return parseWithSizeSuffix<T>(s.data(), s.size());
}

template <typename T>
inline T parseWithSizeSuffix(const char * data)
{
    return parseWithSizeSuffix<T>(data, strlen(data));
}

template <typename T>
inline T parse(const char * data)
{
    return parse<T>(data, strlen(data));
}

template <typename T>
inline T parse(const String & s)
{
    return parse<T>(s.data(), s.size());
}

template <typename T>
inline T parse(std::string_view s)
{
    return parse<T>(s.data(), s.size());
}

template <typename T>
inline bool tryParse(T & res, const char * data)
{
    return tryParse(res, data, strlen(data));
}

template <typename T>
inline bool tryParse(T & res, const String & s)
{
    return tryParse(res, s.data(), s.size());
}

template <typename T>
inline bool tryParse(T & res, std::string_view s)
{
    return tryParse(res, s.data(), s.size());
}


/** Skip UTF-8 BOM if it is under cursor.
  * As BOM is usually located at start of stream, and buffer size is usually larger than three bytes,
  *  the function expects, that all three bytes of BOM is fully in buffer (otherwise it don't skip anything).
  */
inline void skipBOMIfExists(ReadBuffer & buf)
{
    if (!buf.eof()
        && buf.position() + 3 < buf.buffer().end()
        && buf.position()[0] == '\xEF'
        && buf.position()[1] == '\xBB'
        && buf.position()[2] == '\xBF')
    {
        buf.position() += 3;
    }
}


/// Skip to next character after next \n. If no \n in stream, skip to end.
void skipToNextLineOrEOF(ReadBuffer & buf);

/// Skip to next character after next \r. If no \r in stream, skip to end.
void skipToCarriageReturnOrEOF(ReadBuffer & buf);

/// Skip to next character after next unescaped \n. If no \n in stream, skip to end. Does not throw on invalid escape sequences.
void skipToUnescapedNextLineOrEOF(ReadBuffer & buf);


/** This function just copies the data from buffer's internal position (in.position())
  * to current position (from arguments) into memory.
  */
void saveUpToPosition(ReadBuffer & in, Memory<Allocator<false>> & memory, char * current);

/** This function is negative to eof().
  * In fact it returns whether the data was loaded to internal ReadBuffers's buffer or not.
  * And saves data from buffer's position to current if there is no pending data in buffer.
  * Why we have to use this strange function? Consider we have buffer's internal position in the middle
  * of our buffer and the current cursor in the end of the buffer. When we call eof() it calls next().
  * And this function can fill the buffer with new data, so we will lose the data from previous buffer state.
  */
bool loadAtPosition(ReadBuffer & in, Memory<Allocator<false>> & memory, char * & current);

/// Skip data until start of the next row or eof (the end of row is determined by two delimiters:
/// row_after_delimiter and row_between_delimiter).
void skipToNextRowOrEof(PeekableReadBuffer & buf, const String & row_after_delimiter, const String & row_between_delimiter, bool skip_spaces);

struct PcgDeserializer
{
    static void deserializePcg32(pcg32_fast & rng, ReadBuffer & buf)
    {
        decltype(rng.state_) multiplier, increment, state;
        readText(multiplier, buf);
        assertChar(' ', buf);
        readText(increment, buf);
        assertChar(' ', buf);
        readText(state, buf);

        if (multiplier != rng.multiplier())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect multiplier in pcg32: expected {}, got {}", rng.multiplier(), multiplier);
        if (increment != rng.increment())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect increment in pcg32: expected {}, got {}", rng.increment(), increment);

        rng.state_ = state;
    }
};

template <typename Vector>
void readQuotedFieldInto(Vector & s, ReadBuffer & buf);

void readQuotedField(String & s, ReadBuffer & buf);

void readJSONField(String & s, ReadBuffer & buf);

}
