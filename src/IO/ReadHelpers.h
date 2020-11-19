#pragma once

#include <cmath>
#include <cstring>
#include <limits>
#include <algorithm>
#include <iterator>

#include <type_traits>

#include <common/DateLUT.h>
#include <common/LocalDate.h>
#include <common/LocalDateTime.h>
#include <common/StringRef.h>
#include <common/arithmeticOverflow.h>

#include <Core/Types.h>
#include <Core/DecimalFunctions.h>
#include <Core/UUID.h>
#include <Core/BigInt.h>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Arena.h>
#include <Common/UInt128.h>
#include <Common/intExp.h>

#include <Formats/FormatSettings.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/VarInt.h>

#include <DataTypes/DataTypeDateTime.h>

#include <double-conversion/double-conversion.h>


/// 1 GiB
#define DEFAULT_MAX_STRING_SIZE (1ULL << 30)


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_UUID;
    extern const int CANNOT_READ_ARRAY_FROM_TEXT;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
    buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x));
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

template <typename T>
void readBigIntBinary(T & x, ReadBuffer & buf)
{
    static const constexpr size_t bytesize = BigInt<T>::size;
    char bytes[bytesize];

    buf.readStrict(bytes, bytesize);

    x = BigInt<T>::deserialize(bytes);
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

[[noreturn]] void throwAtAssertionFailed(const char * s, ReadBuffer & buf);

inline void assertChar(char symbol, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != symbol)
    {
        char err[2] = {symbol, '\0'};
        throwAtAssertionFailed(err, buf);
    }
    ++buf.position();
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

inline bool checkChar(char c, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != c)
        return false;
    ++buf.position();
    return true;
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

inline void readBoolTextWord(bool & x, ReadBuffer & buf)
{
    if (buf.eof())
        throwReadAfterEOF();

    if (*buf.position() == 't')
    {
        assertString("true", buf);
        x = true;
    }
    else
    {
        assertString("false", buf);
        x = false;
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
    /// TODO: disabled for big ints cause of 127 vs 128 bit conversion
    using UnsignedT = std::conditional_t<is_big_int_v<T>, T, make_unsigned_t<T>>;

    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    bool negative = false;
    UnsignedT res = 0;
    if (buf.eof())
    {
        if constexpr (throw_exception)
            throwReadAfterEOF();
        else
            return ReturnType(false);
    }

    const size_t initial_pos = buf.count();
    while (!buf.eof())
    {
        switch (*buf.position())
        {
            case '+':
            {
                break;
            }
            case '-':
            {
                if constexpr (is_signed_v<T>)
                    negative = true;
                else
                {
                    if constexpr (throw_exception)
                        throw Exception("Unsigned type must not contain '-' symbol", ErrorCodes::CANNOT_PARSE_NUMBER);
                    else
                        return ReturnType(false);
                }
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
                if constexpr (check_overflow == ReadIntTextCheckOverflow::CHECK_OVERFLOW)
                {
                    /// Perform relativelly slow overflow check only when
                    /// number of decimal digits so far is close to the max for given type.
                    /// Example: 20 * 10 will overflow Int8.

                    if (buf.count() - initial_pos + 1 >= std::numeric_limits<T>::max_digits10)
                    {
                        T signed_res = res;
                        if (common::mulOverflow<T>(signed_res, 10, signed_res)
                            || common::addOverflow<T>(signed_res, (*buf.position() - '0'), signed_res))
                            return ReturnType(false);

                        /// Cannot assign signed to unsigned for big ints. Ignore fast path.
                        if constexpr (!is_big_int_v<T>)
                        {
                            res = signed_res;
                            break;
                        }
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
    x = res;
    if constexpr (is_signed_v<T>)
    {
        if (negative)
        {
            if constexpr (check_overflow == ReadIntTextCheckOverflow::CHECK_OVERFLOW)
            {
                if (common::mulOverflow<T>(x, -1, x))
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
    readIntTextImpl<T, void, check_overflow>(x, buf);
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::CHECK_OVERFLOW, typename T>
bool tryReadIntText(T & x, ReadBuffer & buf)
{
    return readIntTextImpl<T, bool, check_overflow>(x, buf);
}

template <ReadIntTextCheckOverflow check_overflow = ReadIntTextCheckOverflow::DO_NOT_CHECK_OVERFLOW, typename T>
void readIntText(Decimal<T> & x, ReadBuffer & buf)
{
    readIntText<check_overflow>(x.value, buf);
}

/** More efficient variant (about 1.5 times on real dataset).
  * Differs in following:
  * - for numbers starting with zero, parsed only zero;
  * - symbol '+' before number is not supported;
  * - symbols :;<=>? are parsed as some numbers.
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
        /// This check is suddenly faster than
        ///  unsigned char c = *buf.position() - '0';
        ///  if (c < 10)
        /// for unknown reason on Xeon E5645.

        if ((*buf.position() & 0xF0) == 0x30) /// It makes sense to have this condition inside loop.
        {
            res *= 10;
            res += *buf.position() & 0x0F;
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

/// This could be used as template parameter for functions above, if you want to just skip data.
struct NullOutput
{
    void append(const char *, size_t) {}
    void push_back(char) {}
};

void parseUUID(const UInt8 * src36, UInt8 * dst16);
void parseUUIDWithoutSeparator(const UInt8 * src36, UInt8 * dst16);
void parseUUID(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16);
void parseUUIDWithoutSeparator(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16);

template <typename IteratorSrc, typename IteratorDst>
void formatHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes);


template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf);

/// In YYYY-MM-DD format.
/// For convenience, Month and Day parts can have single digit instead of two digits.
/// Any separators other than '-' are supported.
template <typename ReturnType = void>
inline ReturnType readDateTextImpl(LocalDate & date, ReadBuffer & buf)
{
    /// Optimistic path, when whole value is in buffer.
    if (buf.position() + 10 <= buf.buffer().end())
    {
        UInt16 year = (buf.position()[0] - '0') * 1000 + (buf.position()[1] - '0') * 100 + (buf.position()[2] - '0') * 10 + (buf.position()[3] - '0');
        buf.position() += 5;

        UInt8 month = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            month = month * 10 + buf.position()[1] - '0';
            buf.position() += 3;
        }
        else
            buf.position() += 2;

        UInt8 day = buf.position()[0] - '0';
        if (isNumericASCII(buf.position()[1]))
        {
            day = day * 10 + buf.position()[1] - '0';
            buf.position() += 2;
        }
        else
            buf.position() += 1;

        date = LocalDate(year, month, day);
        return ReturnType(true);
    }
    else
        return readDateTextFallback<ReturnType>(date, buf);
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

    date = DateLUT::instance().makeDayNum(local_date.year(), local_date.month(), local_date.day());
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

inline bool tryReadDateText(LocalDate & date, ReadBuffer & buf)
{
    return readDateTextImpl<bool>(date, buf);
}

inline bool tryReadDateText(DayNum & date, ReadBuffer & buf)
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
                    throw Exception(std::string("Cannot parse uuid ") + s, ErrorCodes::CANNOT_PARSE_UUID);
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
            throw Exception(std::string("Cannot parse uuid ") + s, ErrorCodes::CANNOT_PARSE_UUID);
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
inline T parseFromString(const std::string_view & str)
{
    return parse<T>(str.data(), str.size());
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wredundant-decls"
// Just dont mess with it. If the redundant redeclaration is removed then ReaderHelpers.h should be included.
// This leads to Arena.h inclusion which has a problem with ASAN stuff included properly and messing macro definition
// which intefrers with... You dont want to know, really.
UInt128 stringToUUID(const String & str);
#pragma GCC diagnostic pop

template <typename ReturnType = void>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut);

/** In YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format, according to specified time zone.
  * As an exception, also supported parsing of unix timestamp in form of decimal number.
  */
template <typename ReturnType = void>
inline ReturnType readDateTimeTextImpl(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    /** Read 10 characters, that could represent unix timestamp.
      * Only unix timestamp of 5-10 characters is supported.
      * Then look at 5th character. If it is a number - treat whole as unix timestamp.
      * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format.
      */

    /// Optimistic path, when whole value is in buffer.
    const char * s = buf.position();

    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto DateTimeStringInputSize = 19;
    bool optimistic_path_for_date_time_input = s + DateTimeStringInputSize <= buf.buffer().end();

    if (optimistic_path_for_date_time_input)
    {
        if (s[4] < '0' || s[4] > '9')
        {
            UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
            UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
            UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

            UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
            UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
            UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

            if (unlikely(year == 0))
                datetime = 0;
            else
                datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);

            buf.position() += DateTimeStringInputSize;
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

    DB::DecimalUtils::DecimalComponents<DateTime64::NativeType> components{static_cast<DateTime64::NativeType>(whole), 0};

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

    datetime64 = DecimalUtils::decimalFromComponents<DateTime64>(components, scale);

    return ReturnType(true);
}

inline void readDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime, buf, date_lut);
}

inline void readDateTime64Text(DateTime64 & datetime64, UInt32 scale, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    readDateTimeTextImpl<void>(datetime64, scale, buf, date_lut);
}

inline bool tryReadDateTimeText(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut = DateLUT::instance())
{
    return readDateTimeTextImpl<bool>(datetime, buf, date_lut);
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
        throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
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
inline std::enable_if_t<is_arithmetic_v<T>, void>
readBinary(T & x, ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(String & x, ReadBuffer & buf) { readStringBinary(x, buf); }
inline void readBinary(Int128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(DummyUInt256 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal32 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal64 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Decimal256 & x, ReadBuffer & buf) { readBigIntBinary(x.value, buf); }
inline void readBinary(LocalDate & x, ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(UInt256 & x, ReadBuffer & buf) { readBigIntBinary(x, buf); }
inline void readBinary(Int256 & x, ReadBuffer & buf) { readBigIntBinary(x, buf); }

template <typename T>
inline std::enable_if_t<is_arithmetic_v<T> && (sizeof(T) <= 8), void>
readBinaryBigEndian(T & x, ReadBuffer & buf)    /// Assuming little endian architecture.
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


/// Generic methods to read value in text tab-separated format.
template <typename T>
inline std::enable_if_t<is_integer_v<T>, void>
readText(T & x, ReadBuffer & buf) { readIntText(x, buf); }

template <typename T>
inline std::enable_if_t<is_integer_v<T>, bool>
tryReadText(T & x, ReadBuffer & buf) { return tryReadIntText(x, buf); }

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, void>
readText(T & x, ReadBuffer & buf) { readFloatText(x, buf); }

template <typename T>
inline std::enable_if_t<std::is_floating_point_v<T>, bool>
tryReadText(T & x, ReadBuffer & buf) { return tryReadFloatText(x, buf); }

inline void readText(bool & x, ReadBuffer & buf) { readBoolText(x, buf); }
inline void readText(String & x, ReadBuffer & buf) { readEscapedString(x, buf); }
inline void readText(LocalDate & x, ReadBuffer & buf) { readDateText(x, buf); }
inline void readText(LocalDateTime & x, ReadBuffer & buf) { readDateTimeText(x, buf); }
inline void readText(UUID & x, ReadBuffer & buf) { readUUIDText(x, buf); }
[[noreturn]] inline void readText(UInt128 &, ReadBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be read as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

/// Generic methods to read value in text format,
///  possibly in single quotes (only for data types that use quotes in VALUES format of INSERT statement in SQL).
template <typename T>
inline std::enable_if_t<is_arithmetic_v<T>, void>
readQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

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
inline std::enable_if_t<is_arithmetic_v<T>, void>
readDoubleQuoted(T & x, ReadBuffer & buf) { readText(x, buf); }

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
inline std::enable_if_t<is_arithmetic_v<T>, void>
readCSV(T & x, ReadBuffer & buf) { readCSVSimple(x, buf); }

inline void readCSV(String & x, ReadBuffer & buf, const FormatSettings::CSV & settings) { readCSVString(x, buf, settings); }
inline void readCSV(LocalDate & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(LocalDateTime & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UUID & x, ReadBuffer & buf) { readCSVSimple(x, buf); }
[[noreturn]] inline void readCSV(UInt128 &, ReadBuffer &)
{
    /** Because UInt128 isn't a natural type, without arithmetic operator and only use as an intermediary type -for UUID-
     *  it should never arrive here. But because we used the DataTypeNumber class we should have at least a definition of it.
     */
    throw Exception("UInt128 cannot be read as a text", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}
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
                throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
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
                throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
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
inline void skipWhitespaceIfAny(ReadBuffer & buf)
{
    while (!buf.eof() && isWhitespaceASCII(*buf.position()))
        ++buf.position();
}

/// Skips json value.
void skipJSONField(ReadBuffer & buf, const StringRef & name_of_field);


/** Read serialized exception.
  * During serialization/deserialization some information is lost
  * (type is cut to base class, 'message' replaced by 'displayText', and stack trace is appended to 'message')
  * Some additional message could be appended to exception (example: you could add information about from where it was received).
  */
Exception readException(ReadBuffer & buf, const String & additional_message = "");
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
inline std::enable_if_t<!is_integer_v<T>, void>
readTextWithSizeSuffix(T & x, ReadBuffer & buf) { readText(x, buf); }

template <typename T>
inline std::enable_if_t<is_integer_v<T>, void>
readTextWithSizeSuffix(T & x, ReadBuffer & buf)
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
    return;
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
inline T parseWithSizeSuffix(const std::string_view & s)
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
inline bool tryParse(T & res, const char * data)
{
    return tryParse(res, data, strlen(data));
}

template <typename T>
inline bool tryParse(T & res, const String & s)
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

/// Skip to next character after next unescaped \n. If no \n in stream, skip to end. Does not throw on invalid escape sequences.
void skipToUnescapedNextLineOrEOF(ReadBuffer & buf);


/** This function just copies the data from buffer's internal position (in.position())
  * to current position (from arguments) into memory.
  */
void saveUpToPosition(ReadBuffer & in, Memory<> & memory, char * current);

/** This function is negative to eof().
  * In fact it returns whether the data was loaded to internal ReadBuffers's buffer or not.
  * And saves data from buffer's position to current if there is no pending data in buffer.
  * Why we have to use this strange function? Consider we have buffer's internal position in the middle
  * of our buffer and the current cursor in the end of the buffer. When we call eof() it calls next().
  * And this function can fill the buffer with new data, so we will lose the data from previous buffer state.
  */
bool loadAtPosition(ReadBuffer & in, Memory<> & memory, char * & current);


struct PcgDeserializer
{
    static void deserializePcg32(const pcg32_fast & rng, ReadBuffer & buf)
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
    }
};

}
