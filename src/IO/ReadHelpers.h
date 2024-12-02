#pragma once

#include <cmath>
#include <cstring>
#include <string>
#include <string_view>
#include <limits>
#include <algorithm>
#include <iterator>
#include <bit>
#include <span>

#include <type_traits>

#include <Common/StackTrace.h>
#include <Common/formatIPv6.h>
#include <Common/DateLUT.h>
#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/transformEndianness.h>
#include <base/StringRef.h>
#include <base/arithmeticOverflow.h>
#include <base/sort.h>
#include <base/unit.h>

#include <Core/Types.h>
#include <Core/DecimalFunctions.h>
#include <Core/UUID.h>
#include <base/IPv4andIPv6.h>

#include <Common/Allocator.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <Common/intExp.h>

#include <Formats/FormatSettings.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>

static constexpr auto DEFAULT_MAX_STRING_SIZE = 1_GiB;

namespace DB
{

template <typename Allocator>
struct Memory;
class PeekableReadBuffer;

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
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


/// Function throwReadAfterEOF is located in VarInt.h


inline void readChar(char & x, ReadBuffer & buf)
{
    if (buf.eof()) [[unlikely]]
        throwReadAfterEOF();
    x = *buf.position();
    ++buf.position();
}


/// Read POD-type in native format
template <typename T>
inline void readPODBinary(T & x, ReadBuffer & buf)
{
    buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x)); /// NOLINT
}

inline void readUUIDBinary(UUID & x, ReadBuffer & buf)
{
    auto & uuid = x.toUnderType();
    readPODBinary(uuid.items[0], buf);
    readPODBinary(uuid.items[1], buf);
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

inline void readStringBinary(std::string & s, ReadBuffer & buf, size_t max_string_size = DEFAULT_MAX_STRING_SIZE)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > max_string_size)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large string size.");

    s.resize(size);
    buf.readStrict(s.data(), size);
}

/// For historical reasons we store IPv6 as a String
inline void readIPv6Binary(IPv6 & ip, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size != sizeof(IPv6::UnderlyingType))
        throw Exception(
            ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH,
            "Size of the string {} doesn't match size of binary IPv6 {}",
            size,
            sizeof(IPv6::UnderlyingType));

    buf.readStrict(reinterpret_cast<char*>(&ip.toUnderType()), size);
}

template <typename T>
void readVectorBinary(std::vector<T> & v, ReadBuffer & buf)
{
    size_t size = 0;
    readVarUInt(size, buf);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                        "Too large array size (maximum: {})", DEFAULT_MAX_STRING_SIZE);

    v.resize(size);
    for (size_t i = 0; i < size; ++i)
        readBinary(v[i], buf);
}


void assertString(const char * s, ReadBuffer & buf);
void assertEOF(ReadBuffer & buf);
void assertNotEOF(ReadBuffer & buf);

[[noreturn]] void throwAtAssertionFailed(const char * s, ReadBuffer & buf);

inline bool checkChar(char c, ReadBuffer & buf)
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
    assertStringCaseInsensitive(s.c_str(), buf);
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

    if (!buf.eof() && isAlphaASCII(tmp))
    {
        if (tmp == 't' || tmp == 'T')
        {
            assertStringCaseInsensitive("rue", buf);
            x = true;
        }
        else if (tmp == 'f' || tmp == 'F')
        {
            assertStringCaseInsensitive("alse", buf);
            x = false;
        }
    }
}

template <typename ReturnType = void>
inline ReturnType readBoolTextWord(bool & x, ReadBuffer & buf, bool support_upper_case = false)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (buf.eof()) [[unlikely]]
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        else
            return ReturnType(false);
    }

    switch (*buf.position())
    {
        case 't':
            if constexpr (throw_exception)
                assertString("true", buf);
            else if (!checkString("true", buf))
                return ReturnType(false);
            x = true;
            break;
        case 'f':
            if constexpr (throw_exception)
                assertString("false", buf);
            else if (!checkString("false", buf))
                return ReturnType(false);
            x = false;
            break;
        case 'T':
        {
            if (support_upper_case)
            {
                if constexpr (throw_exception)
                    assertString("TRUE", buf);
                else if (!checkString("TRUE", buf))
                    return ReturnType(false);
                x = true;
                break;
            }
            [[fallthrough]];
        }
        case 'F':
        {
            if (support_upper_case)
            {
                if constexpr (throw_exception)
                    assertString("FALSE", buf);
                else if (!checkString("FALSE", buf))
                    return ReturnType(false);
                x = false;
                break;
            }
            [[fallthrough]];
        }
        default:
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_BOOL, "Unexpected Bool value");
            else
                return ReturnType(false);
        }
    }

    return ReturnType(true);
}

enum class ReadIntTextCheckOverflow : uint8_t
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
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER,
                            "Cannot parse number with multiple sign (+/-) characters");
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
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER,
                            "Cannot parse number with multiple sign (+/-) characters");
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
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER,
                "Cannot parse number with a sign character but without any numeric character");
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
bool tryReadIntText(T & x, ReadBuffer & buf)
{
    if constexpr (is_decimal<T>)
        return tryReadIntText<check_overflow>(x.value, buf);
    else
        return readIntTextImpl<T, bool, check_overflow>(x, buf);
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
    x = is_signed_v<T> && negative ? -res : res;
    return ReturnType(true);
}

template <typename T>
bool tryReadIntTextUnsafe(T & x, ReadBuffer & buf)
{
    return readIntTextUnsafe<T, bool>(x, buf);
}


/// Look at readFloatText.h
template <typename T> void readFloatText(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatText(T & x, ReadBuffer & in);

template <typename T> void readFloatTextPrecise(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatTextPrecise(T & x, ReadBuffer & in);
template <typename T> void readFloatTextFast(T & x, ReadBuffer & in);
template <typename T> bool tryReadFloatTextFast(T & x, ReadBuffer & in);


/// simple: all until '\n' or '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readEscapedStringCRLF(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);
void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

bool tryReadQuotedString(String & s, ReadBuffer & buf);
bool tryReadQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);
void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

bool tryReadDoubleQuotedString(String & s, ReadBuffer & buf);
bool tryReadDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readJSONString(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings);

void readBackQuotedString(String & s, ReadBuffer & buf);
bool tryReadBackQuotedString(String & s, ReadBuffer & buf);
void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf);

void readStringUntilEOF(String & s, ReadBuffer & buf);

// Reads the line until EOL, unescaping backslash escape sequences.
// Buffer pointer is left at EOL, don't forget to advance it.
void readEscapedStringUntilEOL(String & s, ReadBuffer & buf);

/// Only 0x20 as whitespace character
void readStringUntilWhitespace(String & s, ReadBuffer & buf);

void readStringUntilAmpersand(String & s, ReadBuffer & buf);
void readStringUntilEquals(String & s, ReadBuffer & buf);


/** Read string in CSV format.
  * Parsing rules:
  * - string could be placed in quotes; quotes could be single: ' if FormatSettings::CSV::allow_single_quotes is true
  *   or double: " if FormatSettings::CSV::allow_double_quotes is true;
  * - or string could be unquoted - this is determined by first character;
  * - if string is unquoted, then:
  *     - If settings.custom_delimiter is not specified, it is read until next settings.delimiter, either until end of line (CR or LF) or until end of stream;
  *     - If settings.custom_delimiter is specified it reads until first occurrences of settings.custom_delimiter in buffer.
  *       This works only if provided buffer is PeekableReadBuffer.
  *   but spaces and tabs at begin and end of unquoted string are consumed but ignored (note that this behaviour differs from RFC).
  * - if string is in quotes, then it will be read until closing quote,
  *   but sequences of two consecutive quotes are parsed as single quote inside string;
  */
void readCSVString(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// Differ from readCSVString in that it doesn't remove quotes around field if any.
void readCSVField(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// Read string in CSV format until the first occurrence of first_delimiter or second_delimiter.
/// Similar to readCSVString if string is in quotes, we read only data in quotes.
String readCSVStringWithTwoPossibleDelimiters(PeekableReadBuffer & buf, const FormatSettings::CSV & settings, const String & first_delimiter, const String & second_delimiter);

/// Same as above but includes quotes in the result if any.
String readCSVFieldWithTwoPossibleDelimiters(PeekableReadBuffer & buf, const FormatSettings::CSV & settings, const String & first_delimiter, const String & second_delimiter);

/// Read and append result to array of characters.
template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readNullTerminated(Vector & s, ReadBuffer & buf);

template <typename Vector, bool support_crlf>
void readEscapedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf);

template <typename Vector, bool include_quotes = false, bool allow_throw = true>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings);

/// ReturnType is either bool or void. If bool, the function will return false instead of throwing an exception.
template <typename Vector, typename ReturnType = void>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::JSON & settings);

template <typename Vector>
bool tryReadJSONStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::JSON & settings)
{
    return readJSONStringInto<Vector, bool>(s, buf, settings);
}

template <bool enable_sql_style_quoting, typename Vector>
bool tryReadQuotedStringInto(Vector & s, ReadBuffer & buf);

/// Reads chunk of data between {} in that way,
/// that it has balanced parentheses sequence of {}.
/// So, it may form a JSON object, but it can be incorrenct.
template <typename Vector, typename ReturnType = void>
ReturnType readJSONObjectPossiblyInvalid(Vector & s, ReadBuffer & buf);

template <typename Vector, typename ReturnType = void>
ReturnType readJSONArrayInto(Vector & s, ReadBuffer & buf);

/// Similar to readJSONObjectPossiblyInvalid but avoids copying the data if JSON object fits into current read buffer
/// If copying is unavoidable, it copies data into provided object_buffer and returns string_view to it.
std::string_view readJSONObjectAsViewPossiblyInvalid(ReadBuffer & buf, String & object_buffer);

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

template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf, const char * allowed_delimiters);

inline bool isSymbolIn(char symbol, const char * symbols)
{
    if (symbols == nullptr)
        return true;

    const char * pos = symbols;
    while (*pos)
    {
        if (*pos == symbol)
            return true;
        ++pos;
    }
    return false;
}

/// In YYYY-MM-DD format.
/// For convenience, Month and Day parts can have single digit instead of two digits.
/// Any separators other than '-' are supported.
template <typename ReturnType = void>
inline ReturnType readDateTextImpl(LocalDate & date, ReadBuffer & buf, const char * allowed_delimiters = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// Optimistic path, when whole value is in buffer.
    if (!buf.eof() && buf.position() + 10 <= buf.buffer().end())
    {
        char * pos = buf.position();

        auto error = [&]
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATE, "Cannot parse date here: {}", String(buf.position(), 10));
            return ReturnType(false);
        };

        /// YYYY-MM-DD
        /// YYYY-MM-D
        /// YYYY-M-DD
        /// YYYY-M-D
        /// YYYYMMDD

        /// The delimiters can be arbitrary characters, like YYYY/MM!DD, but obviously not digits.

        if (!isNumericASCII(pos[0]) || !isNumericASCII(pos[1]) || !isNumericASCII(pos[2]) || !isNumericASCII(pos[3]))
            return error();

        UInt16 year = (pos[0] - '0') * 1000 + (pos[1] - '0') * 100 + (pos[2] - '0') * 10 + (pos[3] - '0');
        UInt8 month;
        UInt8 day;
        pos += 5;

        if (isNumericASCII(pos[-1]))
        {
            /// YYYYMMDD
            if (!isNumericASCII(pos[0]) || !isNumericASCII(pos[1]) || !isNumericASCII(pos[2]))
                return error();

            month = (pos[-1] - '0') * 10 + (pos[0] - '0');
            day = (pos[1] - '0') * 10 + (pos[2] - '0');
            pos += 3;
        }
        else
        {
            if (!isSymbolIn(pos[-1], allowed_delimiters))
                return error();

            if (!isNumericASCII(pos[0]))
                return error();

            month = pos[0] - '0';
            if (isNumericASCII(pos[1]))
            {
                month = month * 10 + pos[1] - '0';
                pos += 3;
            }
            else
                pos += 2;

            if (isNumericASCII(pos[-1]) || !isNumericASCII(pos[0]))
                return error();

            if (!isSymbolIn(pos[-1], allowed_delimiters))
                return error();

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
    return readDateTextFallback<ReturnType>(date, buf, allowed_delimiters);
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
inline ReturnType readDateTextImpl(DayNum & date, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_delimiters = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    LocalDate local_date;

    if constexpr (throw_exception)
        readDateTextImpl<ReturnType>(local_date, buf, allowed_delimiters);
    else if (!readDateTextImpl<ReturnType>(local_date, buf, allowed_delimiters))
        return false;

    ExtendedDayNum ret = date_lut.makeDayNum(local_date.year(), local_date.month(), local_date.day());
    convertToDayNum(date, ret);
    return ReturnType(true);
}

template <typename ReturnType = void>
inline ReturnType readDateTextImpl(ExtendedDayNum & date, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_delimiters = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    LocalDate local_date;

    if constexpr (throw_exception)
        readDateTextImpl<ReturnType>(local_date, buf, allowed_delimiters);
    else if (!readDateTextImpl<ReturnType>(local_date, buf, allowed_delimiters))
        return false;

    /// When the parameter is out of rule or out of range, Date32 uses 1925-01-01 as the default value (-DateLUT::instance().getDayNumOffsetEpoch(), -16436) and Date uses 1970-01-01.
    date = date_lut.makeDayNum(local_date.year(), local_date.month(), local_date.day(), -static_cast<Int32>(DateLUTImpl::getDayNumOffsetEpoch()));
    return ReturnType(true);
}


inline void readDateText(LocalDate & date, ReadBuffer & buf)
{
    readDateTextImpl<void>(date, buf);
}

inline void readDateText(DayNum & date, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTextImpl<void>(date, buf, date_lut);
}

inline void readDateText(ExtendedDayNum & date, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTextImpl<void>(date, buf, date_lut);
}

inline bool tryReadDateText(LocalDate & date, ReadBuffer & buf, const char * allowed_delimiters = nullptr)
{
    return readDateTextImpl<bool>(date, buf, allowed_delimiters);
}

inline bool tryReadDateText(DayNum & date, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance(), const char * allowed_delimiters = nullptr)
{
    return readDateTextImpl<bool>(date, buf, time_zone, allowed_delimiters);
}

inline bool tryReadDateText(ExtendedDayNum & date, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance(), const char * allowed_delimiters = nullptr)
{
    return readDateTextImpl<bool>(date, buf, time_zone, allowed_delimiters);
}

UUID parseUUID(std::span<const UInt8> src);

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
                    throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Cannot parse uuid {}", s);
                }
                else
                {
                    return ReturnType(false);
                }
            }
        }

        uuid = parseUUID({reinterpret_cast<const UInt8 *>(s), size});
        return ReturnType(true);
    }

    s[size] = 0;

    if constexpr (throw_exception)
    {
        throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Cannot parse uuid {}", s);
    }
    else
    {
        return ReturnType(false);
    }
}

inline void readUUIDText(UUID & uuid, ReadBuffer & buf)
{
    readUUIDTextImpl<void>(uuid, buf);
}

inline bool tryReadUUIDText(UUID & uuid, ReadBuffer & buf)
{
    return readUUIDTextImpl<bool>(uuid, buf);
}

template <typename ReturnType = void>
inline ReturnType readIPv4TextImpl(IPv4 & ip, ReadBuffer & buf)
{
    if (parseIPv4(buf.position(), [&buf](){ return buf.eof(); }, reinterpret_cast<unsigned char *>(&ip.toUnderType())))
        return ReturnType(true);

    if constexpr (std::is_same_v<ReturnType, void>)
        throw Exception(ErrorCodes::CANNOT_PARSE_IPV4, "Cannot parse IPv4 {}", std::string_view(buf.position(), buf.available()));
    else
        return ReturnType(false);
}

inline void readIPv4Text(IPv4 & ip, ReadBuffer & buf)
{
    readIPv4TextImpl<void>(ip, buf);
}

inline bool tryReadIPv4Text(IPv4 & ip, ReadBuffer & buf)
{
    return readIPv4TextImpl<bool>(ip, buf);
}

template <typename ReturnType = void>
inline ReturnType readIPv6TextImpl(IPv6 & ip, ReadBuffer & buf)
{
    if (parseIPv6orIPv4(buf.position(), [&buf](){ return buf.eof(); }, reinterpret_cast<unsigned char *>(ip.toUnderType().items)))
        return ReturnType(true);

    if constexpr (std::is_same_v<ReturnType, void>)
        throw Exception(ErrorCodes::CANNOT_PARSE_IPV6, "Cannot parse IPv6 {}", std::string_view(buf.position(), buf.available()));
    else
        return ReturnType(false);
}

inline void readIPv6Text(IPv6 & ip, ReadBuffer & buf)
{
    readIPv6TextImpl<void>(ip, buf);
}

inline bool tryReadIPv6Text(IPv6 & ip, ReadBuffer & buf)
{
    return readIPv6TextImpl<bool>(ip, buf);
}

template <typename T>
inline T parse(const char * data, size_t size);

template <typename T>
inline T parseFromString(std::string_view str)
{
    return parse<T>(str.data(), str.size());
}


template <typename ReturnType = void, bool dt64_mode = false>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_date_delimiters = nullptr, const char * allowed_time_delimiters = nullptr);

/** In YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format, according to specified time zone.
  * As an exception, also supported parsing of unix timestamp in form of decimal number.
  */
template <typename ReturnType = void, bool dt64_mode = false>
inline ReturnType readDateTimeTextImpl(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_date_delimiters = nullptr, const char * allowed_time_delimiters = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if constexpr (!dt64_mode)
    {
        if (!buf.eof() && !isNumericASCII(*buf.position()))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse datetime");
            else
                return false;
        }
    }

    /// Optimistic path, when whole value is in buffer.
    const char * s = buf.position();

    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto date_time_broken_down_length = 19;
    /// YYYY-MM-DD
    static constexpr auto date_broken_down_length = 10;
    bool optimistic_path_for_date_time_input = s + date_time_broken_down_length <= buf.buffer().end();

    if (optimistic_path_for_date_time_input)
    {
        if (s[4] < '0' || s[4] > '9')
        {
            if constexpr (!throw_exception)
            {
                if (!isNumericASCII(s[0]) || !isNumericASCII(s[1]) || !isNumericASCII(s[2]) || !isNumericASCII(s[3])
                    || !isNumericASCII(s[5]) || !isNumericASCII(s[6]) || !isNumericASCII(s[8]) || !isNumericASCII(s[9]))
                    return ReturnType(false);

                if (!isSymbolIn(s[4], allowed_date_delimiters) || !isSymbolIn(s[7], allowed_date_delimiters))
                    return ReturnType(false);
            }

            UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
            UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
            UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

            UInt8 hour = 0;
            UInt8 minute = 0;
            UInt8 second = 0;

            /// Simply determine whether it is YYYY-MM-DD hh:mm:ss or YYYY-MM-DD by the content of the tenth character in an optimistic scenario
            bool dt_long = (s[10] == ' ' || s[10] == 'T');
            if (dt_long)
            {
                if constexpr (!throw_exception)
                {
                    if (!isNumericASCII(s[11]) || !isNumericASCII(s[12]) || !isNumericASCII(s[14]) || !isNumericASCII(s[15])
                        || !isNumericASCII(s[17]) || !isNumericASCII(s[18]))
                        return ReturnType(false);

                    if (!isSymbolIn(s[13], allowed_time_delimiters) || !isSymbolIn(s[16], allowed_time_delimiters))
                        return ReturnType(false);
                }

                hour = (s[11] - '0') * 10 + (s[12] - '0');
                minute = (s[14] - '0') * 10 + (s[15] - '0');
                second = (s[17] - '0') * 10 + (s[18] - '0');
            }

            if (unlikely(year == 0))
                datetime = 0;
            else
                datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);

            if (dt_long)
                buf.position() += date_time_broken_down_length;
            else
                buf.position() += date_broken_down_length;

            return ReturnType(true);
        }
        /// Why not readIntTextUnsafe? Because for needs of AdFox, parsing of unix timestamp with leading zeros is supported: 000...NNNN.
        return readIntTextImpl<time_t, ReturnType, ReadIntTextCheckOverflow::CHECK_OVERFLOW>(datetime, buf);
    }
    return readDateTimeTextFallback<ReturnType, dt64_mode>(datetime, buf, date_lut, allowed_date_delimiters, allowed_time_delimiters);
}

template <typename ReturnType>
inline ReturnType readDateTimeTextImpl(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_date_delimiters = nullptr, const char * allowed_time_delimiters = nullptr)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    time_t whole = 0;
    bool is_negative_timestamp = (!buf.eof() && *buf.position() == '-');
    bool is_empty = buf.eof();

    if (!is_empty)
    {
        if constexpr (throw_exception)
        {
            try
            {
                readDateTimeTextImpl<ReturnType, true>(whole, buf, date_lut, allowed_date_delimiters, allowed_time_delimiters);
            }
            catch (const DB::Exception &)
            {
                if (buf.eof() || *buf.position() != '.')
                    throw;
            }
        }
        else
        {
            auto ok = readDateTimeTextImpl<ReturnType, true>(whole, buf, date_lut, allowed_date_delimiters, allowed_time_delimiters);
            if (!ok && (buf.eof() || *buf.position() != '.'))
                return ReturnType(false);
        }
    }

    int negative_fraction_multiplier = 1;

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

        /// Fractional part (subseconds) is treated as positive by users, but represented as a negative number.
        /// E.g. `1925-12-12 13:14:15.123` is represented internally as timestamp `-1390214744.877`.
        /// Thus need to convert <negative_timestamp>.<fractional> to <negative_timestamp+1>.<1-0.<fractional>>
        /// Also, setting fractional part to be negative when whole is 0 results in wrong value, in this case multiply result by -1.
        if (!is_negative_timestamp && components.whole < 0 && components.fractional != 0)
        {
            const auto scale_multiplier = DecimalUtils::scaleMultiplier<DateTime64::NativeType>(scale);
            ++components.whole;
            components.fractional = scale_multiplier - components.fractional;
            if (!components.whole)
            {
                negative_fraction_multiplier = -1;
            }
        }
    }
    /// 10413792000 is time_t value for 2300-01-01 UTC (a bit over the last year supported by DateTime64)
    else if (whole >= 10413792000LL)
    {
        /// Unix timestamp with subsecond precision, already scaled to integer.
        /// For disambiguation we support only time since 2001-09-09 01:46:40 UTC and less than 30 000 years in future.
        components.fractional =  components.whole % common::exp10_i32(scale);
        components.whole = components.whole / common::exp10_i32(scale);
    }

    bool is_ok = true;
    if constexpr (std::is_same_v<ReturnType, void>)
    {
        datetime64 = DecimalUtils::decimalFromComponents<DateTime64>(components, scale) * negative_fraction_multiplier;
    }
    else
    {
        is_ok = DecimalUtils::tryGetDecimalFromComponents<DateTime64>(components, scale, datetime64);
        if (is_ok)
            datetime64 *= negative_fraction_multiplier;
    }

    return ReturnType(is_ok);
}

inline void readDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime, buf, time_zone);
}

inline void readDateTime64Text(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime64, scale, buf, date_lut);
}

inline bool tryReadDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance(), const char * allowed_date_delimiters = nullptr, const char * allowed_time_delimiters = nullptr)
{
    return readDateTimeTextImpl<bool>(datetime, buf, time_zone, allowed_date_delimiters, allowed_time_delimiters);
}

inline bool tryReadDateTime64Text(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance(), const char * allowed_date_delimiters = nullptr, const char * allowed_time_delimiters = nullptr)
{
    return readDateTimeTextImpl<bool>(datetime64, scale, buf, date_lut, allowed_date_delimiters, allowed_time_delimiters);
}

inline void readDateTimeText(LocalDateTime & datetime, ReadBuffer & buf)
{
    char s[10];
    size_t size = buf.read(s, 10);
    if (10 != size)
    {
        s[size] = 0;
        throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime {}", s);
    }

    datetime.year((s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0'));
    datetime.month((s[5] - '0') * 10 + (s[6] - '0'));
    datetime.day((s[8] - '0') * 10 + (s[9] - '0'));

    /// Allow to read Date as DateTime
    if (buf.eof() || !(*buf.position() == ' ' || *buf.position() == 'T'))
        return;

    ++buf.position();
    size = buf.read(s, 8);
    if (8 != size)
    {
        s[size] = 0;
        throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse time component of DateTime {}", s);
    }

    datetime.hour((s[0] - '0') * 10 + (s[1] - '0'));
    datetime.minute((s[3] - '0') * 10 + (s[4] - '0'));
    datetime.second((s[6] - '0') * 10 + (s[7] - '0'));
}

/// In h*:mm:ss format.
template <typename ReturnType = void>
inline ReturnType readTimeTextImpl(time_t & time, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    int16_t hours;
    int16_t minutes;
    int16_t seconds;

    readIntText(hours, buf);

    int negative_multiplier = hours < 0 ? -1 : 1;

    // :mm:ss
    const size_t remaining_time_size = 6;

    char s[remaining_time_size];

    size_t size = buf.read(s, remaining_time_size);
    if (size != remaining_time_size)
    {
        s[size] = 0;

        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime {}", s);
        else
            return false;
    }

    minutes = (s[1] - '0') * 10 + (s[2] - '0');
    seconds = (s[4] - '0') * 10 + (s[5] - '0');

    time = hours * 3600 + (minutes * 60 + seconds) * negative_multiplier;

    return ReturnType(true);
}

template <typename ReturnType>
inline ReturnType readTimeTextImpl(Decimal64 & time64, UInt32 scale, ReadBuffer & buf)
{
    time_t whole;
    if (!readTimeTextImpl<bool>(whole, buf))
    {
        return ReturnType(false);
    }

    DB::DecimalUtils::DecimalComponents<Decimal64> components{static_cast<Decimal64::NativeType>(whole), 0};

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
    }

    bool is_ok = true;
    if constexpr (std::is_same_v<ReturnType, void>)
    {
        time64 = DecimalUtils::decimalFromComponents<Decimal64>(components, scale);
    }
    else
    {
        is_ok = DecimalUtils::tryGetDecimalFromComponents<Decimal64>(components, scale, time64);
    }

    return ReturnType(is_ok);
}

inline void readTime64Text(Decimal64 & time64, UInt32 scale, ReadBuffer & buf)
{
    readTimeTextImpl<void>(time64, scale, buf);
}

/// Generic methods to read value in native binary format.
template <typename T>
requires is_arithmetic_v<T>
inline void readBinary(T & x, ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(bool & x, ReadBuffer & buf)
{
    /// When deserializing a bool it might trigger UBSAN if the input is not 0 or 1, so it's better to treat it as an Int8
    static_assert(sizeof(bool) == sizeof(Int8));
    Int8 flag = 0;
    readBinary(flag, buf);
    x = (flag != 0);
}

inline void readBinary(String & x, ReadBuffer & buf) { readStringBinary(x, buf); }
inline void readBinary(Decimal32 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal64 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal256 & x, ReadBuffer & buf) { readPODBinary(x.value, buf); }
inline void readBinary(LocalDate & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(IPv4 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(IPv6 & x, ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(UUID & x, ReadBuffer & buf)
{
    readUUIDBinary(x, buf);
}

inline void readBinary(CityHash_v1_0_2::uint128 & x, ReadBuffer & buf)
{
    readPODBinary(x.low64, buf);
    readPODBinary(x.high64, buf);
}

inline void readBinary(StackTrace::FramePointers & x, ReadBuffer & buf) { readPODBinary(x, buf); }

template <std::endian endian, typename T>
inline void readBinaryEndian(T & x, ReadBuffer & buf)
{
    readBinary(x, buf);
    transformEndianness<endian>(x);
}

template <typename T>
inline void readBinaryLittleEndian(T & x, ReadBuffer & buf)
{
    readBinaryEndian<std::endian::little>(x, buf);
}

template <typename T>
inline void readBinaryBigEndian(T & x, ReadBuffer & buf)
{
    readBinaryEndian<std::endian::big>(x, buf);
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

inline bool tryReadText(is_floating_point auto & x, ReadBuffer & buf)
{
    return tryReadFloatText(x, buf);
}

inline bool tryReadText(UUID & x, ReadBuffer & buf) { return tryReadUUIDText(x, buf); }
inline bool tryReadText(IPv4 & x, ReadBuffer & buf) { return tryReadIPv4Text(x, buf); }
inline bool tryReadText(IPv6 & x, ReadBuffer & buf) { return tryReadIPv6Text(x, buf); }

template <typename T>
requires is_floating_point<T>
inline void readText(T & x, ReadBuffer & buf) { readFloatText(x, buf); }

inline void readText(String & x, ReadBuffer & buf) { readEscapedString(x, buf); }

inline void readText(DayNum & x, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance()) { readDateText(x, buf, time_zone); }
inline bool tryReadText(DayNum & x, ReadBuffer & buf, const DateLUTImpl & time_zone = DateLUT::instance()) { return tryReadDateText(x, buf, time_zone); }

inline void readText(LocalDate & x, ReadBuffer & buf) { readDateText(x, buf); }
inline bool tryReadText(LocalDate & x, ReadBuffer & buf) { return tryReadDateText(x, buf); }
inline void readText(LocalDateTime & x, ReadBuffer & buf) { readDateTimeText(x, buf); }
inline bool tryReadText(LocalDateTime & x, ReadBuffer & buf)
{
    time_t time;
    if (!tryReadDateTimeText(time, buf))
        return false;
    x = LocalDateTime(time, DateLUT::instance());
    return true;
}

inline void readText(UUID & x, ReadBuffer & buf) { readUUIDText(x, buf); }
inline void readText(IPv4 & x, ReadBuffer & buf) { readIPv4Text(x, buf); }
inline void readText(IPv6 & x, ReadBuffer & buf) { readIPv6Text(x, buf); }

/// Generic methods to read value in text format,
///  possibly in single quotes (only for data types that use quotes in VALUES format of INSERT statement in SQL).
template <typename T>
requires is_arithmetic_v<T>
inline void readQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

template <typename T>
requires is_arithmetic_v<T>
inline void readQuoted(T & x, ReadBuffer & buf, const DateLUTImpl & time_zone) { readText(x, buf, time_zone); }

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

inline void readQuoted(IPv4 & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readIPv4Text(x, buf);
    assertChar('\'', buf);
}

inline void readQuoted(IPv6 & x, ReadBuffer & buf)
{
    assertChar('\'', buf);
    readIPv6Text(x, buf);
    assertChar('\'', buf);
}

/// Same as above, but in double quotes.
template <typename T>
requires is_arithmetic_v<T>
inline void readDoubleQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

template <typename T>
requires is_arithmetic_v<T>
inline void readDoubleQuoted(T & x, ReadBuffer & buf, const DateLUTImpl & time_zone) { readText(x, buf, time_zone); }

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

/// CSV for numbers: quotes are optional, no special escaping rules.
template <typename T, typename ReturnType = void>
inline ReturnType readCSVSimple(T & x, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (buf.eof()) [[unlikely]]
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        return ReturnType(false);
    }

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    if constexpr (throw_exception)
        readText(x, buf);
    else if (!tryReadText(x, buf))
        return ReturnType(false);

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        if constexpr (throw_exception)
            assertChar(maybe_quote, buf);
        else if (!checkChar(maybe_quote, buf))
            return ReturnType(false);
    }

    return ReturnType(true);
}

// standalone overload for dates: to avoid instantiating DateLUTs while parsing other types
template <typename T, typename ReturnType = void>
inline ReturnType readCSVSimple(T & x, ReadBuffer & buf, const DateLUTImpl & time_zone)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (buf.eof()) [[unlikely]]
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        return ReturnType(false);
    }

    char maybe_quote = *buf.position();

    if (maybe_quote == '\'' || maybe_quote == '\"')
        ++buf.position();

    if constexpr (throw_exception)
        readText(x, buf, time_zone);
    else if (!tryReadText(x, buf, time_zone))
        return ReturnType(false);

    if (maybe_quote == '\'' || maybe_quote == '\"')
    {
        if constexpr (throw_exception)
            assertChar(maybe_quote, buf);
        else if (!checkChar(maybe_quote, buf))
            return ReturnType(false);
    }

    return ReturnType(true);
}

template <typename T>
requires is_arithmetic_v<T>
inline void readCSV(T & x, ReadBuffer & buf)
{
    readCSVSimple(x, buf);
}

template <typename T>
requires is_arithmetic_v<T>
inline bool tryReadCSV(T & x, ReadBuffer & buf)
{
    return readCSVSimple<T, bool>(x, buf);
}

inline void readCSV(String & x, ReadBuffer & buf, const FormatSettings::CSV & settings) { readCSVString(x, buf, settings); }
inline bool tryReadCSV(String & x, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    x.clear();
    readCSVStringInto<String, false, false>(x, buf, settings);
    return true;
}

inline void readCSV(LocalDate & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(LocalDate & x, ReadBuffer & buf) { return readCSVSimple<LocalDate, bool>(x, buf); }

inline void readCSV(DayNum & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(DayNum & x, ReadBuffer & buf) { return readCSVSimple<DayNum, bool>(x, buf); }
inline void readCSV(DayNum & x, ReadBuffer & buf, const DateLUTImpl & time_zone) { readCSVSimple(x, buf, time_zone); }
inline bool tryReadCSV(DayNum & x, ReadBuffer & buf, const DateLUTImpl & time_zone) { return readCSVSimple<DayNum, bool>(x, buf, time_zone); }

inline void readCSV(LocalDateTime & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(LocalDateTime & x, ReadBuffer & buf) { return readCSVSimple<LocalDateTime, bool>(x, buf); }

inline void readCSV(UUID & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(UUID & x, ReadBuffer & buf) { return readCSVSimple<UUID, bool>(x, buf); }

inline void readCSV(IPv4 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(IPv4 & x, ReadBuffer & buf) { return readCSVSimple<IPv4, bool>(x, buf); }

inline void readCSV(IPv6 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(IPv6 & x, ReadBuffer & buf) { return readCSVSimple<IPv6, bool>(x, buf); }

inline void readCSV(UInt128 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(UInt128 & x, ReadBuffer & buf) { return readCSVSimple<UInt128, bool>(x, buf); }

inline void readCSV(Int128 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(Int128 & x, ReadBuffer & buf) { return readCSVSimple<Int128, bool>(x, buf); }

inline void readCSV(UInt256 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(UInt256 & x, ReadBuffer & buf) { return readCSVSimple<UInt256, bool>(x, buf); }

inline void readCSV(Int256 & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline bool tryReadCSV(Int256 & x, ReadBuffer & buf) { return readCSVSimple<Int256, bool>(x, buf); }

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
                throw Exception(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT, "Cannot read array from text");
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
                throw Exception(ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT, "Cannot read array from text");
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
void skipJSONField(ReadBuffer & buf, StringRef name_of_field, const FormatSettings::JSON & settings);
bool trySkipJSONField(ReadBuffer & buf, StringRef name_of_field, const FormatSettings::JSON & settings);


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
    assertEOF(buf);
    return res;
}

template <typename T>
inline bool tryParse(T & res, const char * data, size_t size)
{
    ReadBufferFromMemory buf(data, size);
    if (!tryReadText(res, buf))
        return false;
    return buf.eof();
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

/// Skip to next character after next \0. If no \0 in stream, skip to end.
void skipNullTerminated(ReadBuffer & buf);

/** This function just copies the data from buffer's position (in.position())
  * to current position (from arguments) appending into memory.
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

void readParsedValueIntoString(String & s, ReadBuffer & buf, std::function<void(ReadBuffer &)> parse_func);

template <typename ReturnType = void, typename Vector>
ReturnType readQuotedFieldInto(Vector & s, ReadBuffer & buf);

void readQuotedField(String & s, ReadBuffer & buf);
bool tryReadQuotedField(String & s, ReadBuffer & buf);

void readJSONField(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings);
bool tryReadJSONField(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings);

void readTSVField(String & s, ReadBuffer & buf);
void readTSVFieldCRLF(String & s, ReadBuffer & buf);

/** Parse the escape sequence, which can be simple (one character after backslash) or more complex (multiple characters).
  * It is assumed that the cursor is located on the `\` symbol
  */
bool parseComplexEscapeSequence(String & s, ReadBuffer & buf);

}
