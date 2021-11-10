#include <Core/Defines.h>
#include <Common/hex.h>
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/memcpySmall.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/readFloatText.h>
#include <IO/Operators.h>
#include <common/find_symbols.h>
#include <stdlib.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_PARSE_ESCAPE_SEQUENCE;
    extern const int CANNOT_PARSE_QUOTED_STRING;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_DATE;
    extern const int INCORRECT_DATA;
}

template <typename IteratorSrc, typename IteratorDst>
void parseHex(IteratorSrc src, IteratorDst dst, const size_t num_bytes)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; dst_pos < num_bytes; ++dst_pos, src_pos += 2)
        dst[dst_pos] = unhex2(reinterpret_cast<const char *>(&src[src_pos]));
}

void parseUUID(const UInt8 * src36, UInt8 * dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    parseHex(&src36[0], &dst16[0], 4);
    parseHex(&src36[9], &dst16[4], 2);
    parseHex(&src36[14], &dst16[6], 2);
    parseHex(&src36[19], &dst16[8], 2);
    parseHex(&src36[24], &dst16[10], 6);
}

void parseUUIDWithoutSeparator(const UInt8 * src36, UInt8 * dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    parseHex(&src36[0], &dst16[0], 16);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void parseUUID(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    /// FIXME This code looks like trash.
    parseHex(&src36[0], dst16 + 8, 4);
    parseHex(&src36[9], dst16 + 12, 2);
    parseHex(&src36[14], dst16 + 14, 2);
    parseHex(&src36[19], dst16, 2);
    parseHex(&src36[24], dst16 + 2, 6);
}

/** Function used when byte ordering is important when parsing uuid
 *  ex: When we create an UUID type
 */
void parseUUIDWithoutSeparator(const UInt8 * src36, std::reverse_iterator<UInt8 *> dst16)
{
    /// If string is not like UUID - implementation specific behaviour.

    parseHex(&src36[0], dst16 + 8, 8);
    parseHex(&src36[16], dst16, 8);
}

void NO_INLINE throwAtAssertionFailed(const char * s, ReadBuffer & buf)
{
    WriteBufferFromOwnString out;
    out << "Cannot parse input: expected " << quote << s;

    if (buf.eof())
        out << " at end of stream.";
    else
        out << " before: " << quote << String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position()));

    throw ParsingException(out.str(), ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
}


bool checkString(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof() || *buf.position() != *s)
            return false;
        ++buf.position();
    }
    return true;
}


bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    for (; *s; ++s)
    {
        if (buf.eof())
            return false;

        char c = *buf.position();
        if (!equalsCaseInsensitive(*s, c))
            return false;

        ++buf.position();
    }
    return true;
}


void assertString(const char * s, ReadBuffer & buf)
{
    if (!checkString(s, buf))
        throwAtAssertionFailed(s, buf);
}


void assertEOF(ReadBuffer & buf)
{
    if (!buf.eof())
        throwAtAssertionFailed("eof", buf);
}


void assertStringCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (!checkStringCaseInsensitive(s, buf))
        throwAtAssertionFailed(s, buf);
}


bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != *s)
        return false;

    assertString(s, buf);
    return true;
}

bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf)
{
    if (buf.eof())
        return false;

    char c = *buf.position();
    if (!equalsCaseInsensitive(*s, c))
        return false;

    assertStringCaseInsensitive(s, buf);
    return true;
}


template <typename T>
static void appendToStringOrVector(T & s, ReadBuffer & rb, const char * end)
{
    s.append(rb.position(), end - rb.position());
}

template <>
inline void appendToStringOrVector(PaddedPODArray<UInt8> & s, ReadBuffer & rb, const char * end)
{
    if (rb.isPadded())
        s.insertSmallAllowReadWriteOverflow15(rb.position(), end);
    else
        s.insert(rb.position(), end);
}

template <>
inline void appendToStringOrVector(PODArray<char> & s, ReadBuffer & rb, const char * end)
{
    s.insert(rb.position(), end);
}

template <char... chars, typename Vector>
void readStringUntilCharsInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<chars...>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (buf.hasPendingData())
            return;
    }
}

template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<'\t', '\n'>(s, buf);
}

template <typename Vector>
void readStringUntilWhitespaceInto(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<' '>(s, buf);
}

template <typename Vector>
void readNullTerminated(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<'\0'>(s, buf);
    buf.ignore();
}

void readStringUntilWhitespace(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilWhitespaceInto(s, buf);
}

template void readNullTerminated<PODArray<char>>(PODArray<char> & s, ReadBuffer & buf);
template void readNullTerminated<String>(String & s, ReadBuffer & buf);

void readString(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringInto(s, buf);
}

template void readStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        appendToStringOrVector(s, buf, buf.buffer().end());
        buf.position() = buf.buffer().end();
    }
}


void readStringUntilEOF(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilEOFInto(s, buf);
}

template <typename Vector>
void readEscapedStringUntilEOLInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }
}


void readEscapedStringUntilEOL(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringUntilEOLInto(s, buf);
}

template void readStringUntilEOFInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);


/** Parse the escape sequence, which can be simple (one character after backslash) or more complex (multiple characters).
  * It is assumed that the cursor is located on the `\` symbol
  */
template <typename Vector>
static void parseComplexEscapeSequence(Vector & s, ReadBuffer & buf)
{
    ++buf.position();
    if (buf.eof())
        throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

    char char_after_backslash = *buf.position();

    if (char_after_backslash == 'x')
    {
        ++buf.position();
        /// escape sequence of the form \xAA
        char hex_code[2];
        readPODBinary(hex_code, buf);
        s.push_back(unhex2(hex_code));
    }
    else if (char_after_backslash == 'N')
    {
        /// Support for NULLs: \N sequence must be parsed as empty string.
        ++buf.position();
    }
    else
    {
        /// The usual escape sequence of a single character.
        char decoded_char = parseEscapeSequence(char_after_backslash);

        /// For convenience using LIKE and regular expressions,
        /// we leave backslash when user write something like 'Hello 100\%':
        /// it is parsed like Hello 100\% instead of Hello 100%
        if (decoded_char != '\\'
            && decoded_char != '\''
            && decoded_char != '"'
            && decoded_char != '`'  /// MySQL style identifiers
            && decoded_char != '/'  /// JavaScript in HTML
            && !isControlASCII(decoded_char))
        {
            s.push_back('\\');
        }

        s.push_back(decoded_char);
        ++buf.position();
    }
}


template <typename Vector, typename ReturnType>
static ReturnType parseJSONEscapeSequence(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw Exception(message, code);
        return ReturnType(false);
    };

    ++buf.position();

    if (buf.eof())
        return error("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

    assert(buf.hasPendingData());

    switch (*buf.position())
    {
        case '"':
            s.push_back('"');
            break;
        case '\\':
            s.push_back('\\');
            break;
        case '/':
            s.push_back('/');
            break;
        case 'b':
            s.push_back('\b');
            break;
        case 'f':
            s.push_back('\f');
            break;
        case 'n':
            s.push_back('\n');
            break;
        case 'r':
            s.push_back('\r');
            break;
        case 't':
            s.push_back('\t');
            break;
        case 'u':
        {
            ++buf.position();

            char hex_code[4];
            if (4 != buf.read(hex_code, 4))
                return error("Cannot parse escape sequence: less than four bytes after \\u", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

            /// \u0000 - special case
            if (0 == memcmp(hex_code, "0000", 4))
            {
                s.push_back(0);
                return ReturnType(true);
            }

            UInt16 code_point = unhex4(hex_code);

            if (code_point <= 0x7F)
            {
                s.push_back(code_point);
            }
            else if (code_point <= 0x07FF)
            {
                s.push_back(((code_point >> 6) & 0x1F) | 0xC0);
                s.push_back((code_point & 0x3F) | 0x80);
            }
            else
            {
                /// Surrogate pair.
                if (code_point >= 0xD800 && code_point <= 0xDBFF)
                {
                    if (!checkString("\\u", buf))
                        return error("Cannot parse escape sequence: missing second part of surrogate pair", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

                    char second_hex_code[4];
                    if (4 != buf.read(second_hex_code, 4))
                        return error("Cannot parse escape sequence: less than four bytes after \\u of second part of surrogate pair",
                            ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

                    UInt16 second_code_point = unhex4(second_hex_code);

                    if (second_code_point >= 0xDC00 && second_code_point <= 0xDFFF)
                    {
                        UInt32 full_code_point = 0x10000 + (code_point - 0xD800) * 1024 + (second_code_point - 0xDC00);

                        s.push_back(((full_code_point >> 18) & 0x07) | 0xF0);
                        s.push_back(((full_code_point >> 12) & 0x3F) | 0x80);
                        s.push_back(((full_code_point >> 6) & 0x3F) | 0x80);
                        s.push_back((full_code_point & 0x3F) | 0x80);
                    }
                    else
                        return error("Incorrect surrogate pair of unicode escape sequences in JSON", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
                }
                else
                {
                    s.push_back(((code_point >> 12) & 0x0F) | 0xE0);
                    s.push_back(((code_point >> 6) & 0x3F) | 0x80);
                    s.push_back((code_point & 0x3F) | 0x80);
                }
            }

            return ReturnType(true);
        }
        default:
            s.push_back(*buf.position());
            break;
    }

    ++buf.position();
    return ReturnType(true);
}


template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\t', '\n', '\\'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }
}

void readEscapedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringInto(s, buf);
}

template void readEscapedStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readEscapedStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf);


/** If enable_sql_style_quoting == true,
  *  strings like 'abc''def' will be parsed as abc'def.
  * Please note, that even with SQL style quoting enabled,
  *  backslash escape sequences are also parsed,
  *  that could be slightly confusing.
  */
template <char quote, bool enable_sql_style_quoting, typename Vector>
static void readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    if (buf.eof() || *buf.position() != quote)
    {
        throw ParsingException(ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
            "Cannot parse quoted string: expected opening quote '{}', got '{}'",
            std::string{quote}, buf.eof() ? "EOF" : std::string{*buf.position()});
    }

    ++buf.position();

    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', quote>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == quote)
        {
            ++buf.position();

            if (enable_sql_style_quoting && !buf.eof() && *buf.position() == quote)
            {
                s.push_back(quote);
                ++buf.position();
                continue;
            }

            return;
        }

        if (*buf.position() == '\\')
            parseComplexEscapeSequence(s, buf);
    }

    throw ParsingException("Cannot parse quoted string: expected closing quote",
        ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'\'', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'"', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'`', enable_sql_style_quoting>(s, buf);
}


void readQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<false>(s, buf);
}

void readQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedStringInto<true>(s, buf);
}


template void readQuotedStringInto<true>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readDoubleQuotedStringInto<false>(NullOutput & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<false>(s, buf);
}

void readDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readDoubleQuotedStringInto<true>(s, buf);
}

void readBackQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<false>(s, buf);
}

void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<true>(s, buf);
}


template <typename Vector>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    if (buf.eof())
        throwReadAfterEOF();

    const char delimiter = settings.delimiter;
    const char maybe_quote = *buf.position();

    /// Emptiness and not even in quotation marks.
    if (maybe_quote == delimiter)
        return;

    if ((settings.allow_single_quotes && maybe_quote == '\'') || (settings.allow_double_quotes && maybe_quote == '"'))
    {
        ++buf.position();

        /// The quoted case. We are looking for the next quotation mark.
        while (!buf.eof())
        {
            char * next_pos = reinterpret_cast<char *>(memchr(buf.position(), maybe_quote, buf.buffer().end() - buf.position()));

            if (nullptr == next_pos)
                next_pos = buf.buffer().end();

            appendToStringOrVector(s, buf, next_pos);
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            /// Now there is a quotation mark under the cursor. Is there any following?
            ++buf.position();
            if (buf.eof())
                return;

            if (*buf.position() == maybe_quote)
            {
                s.push_back(maybe_quote);
                ++buf.position();
                continue;
            }

            return;
        }
    }
    else
    {
        /// Unquoted case. Look for delimiter or \r or \n.
        while (!buf.eof())
        {
            char * next_pos = buf.position();

            [&]()
            {
#ifdef __SSE2__
                auto rc = _mm_set1_epi8('\r');
                auto nc = _mm_set1_epi8('\n');
                auto dc = _mm_set1_epi8(delimiter);
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(next_pos));
                    auto eq = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(bytes, rc), _mm_cmpeq_epi8(bytes, nc)), _mm_cmpeq_epi8(bytes, dc));
                    uint16_t bit_mask = _mm_movemask_epi8(eq);
                    if (bit_mask)
                    {
                        next_pos += __builtin_ctz(bit_mask);
                        return;
                    }
                }
#endif
                while (next_pos < buf.buffer().end()
                    && *next_pos != delimiter && *next_pos != '\r' && *next_pos != '\n')
                    ++next_pos;
            }();

            appendToStringOrVector(s, buf, next_pos);
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            /** CSV format can contain insignificant spaces and tabs.
              * Usually the task of skipping them is for the calling code.
              * But in this case, it will be difficult to do this, so remove the trailing whitespace by ourself.
              */
            size_t size = s.size();
            while (size > 0
                && (s[size - 1] == ' ' || s[size - 1] == '\t'))
                --size;

            s.resize(size);
            return;
        }
    }
}

void readCSVString(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    s.clear();
    readCSVStringInto(s, buf, settings);
}

template void readCSVStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf, const FormatSettings::CSV & settings);


template <typename Vector, typename ReturnType>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw ParsingException(message, code);
        return ReturnType(false);
    };

    if (buf.eof() || *buf.position() != '"')
        return error("Cannot parse JSON string: expected opening quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
    ++buf.position();

    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', '"'>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '"')
        {
            ++buf.position();
            return ReturnType(true);
        }

        if (*buf.position() == '\\')
            parseJSONEscapeSequence<Vector, ReturnType>(s, buf);
    }

    return error("Cannot parse JSON string: expected closing quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

void readJSONString(String & s, ReadBuffer & buf)
{
    s.clear();
    readJSONStringInto(s, buf);
}

template void readJSONStringInto<PaddedPODArray<UInt8>, void>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template bool readJSONStringInto<PaddedPODArray<UInt8>, bool>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readJSONStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf);
template void readJSONStringInto<String>(String & s, ReadBuffer & buf);


template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = []
    {
        if constexpr (throw_exception)
            throw Exception("Cannot parse date: value is too short", ErrorCodes::CANNOT_PARSE_DATE);
        return ReturnType(false);
    };

    auto ignore_delimiter = [&]
    {
        if (!buf.eof() && !isNumericASCII(*buf.position()))
        {
            ++buf.position();
            return true;
        }
        else
            return false;
    };

    auto append_digit = [&](auto & x)
    {
        if (!buf.eof() && isNumericASCII(*buf.position()))
        {
            x = x * 10 + (*buf.position() - '0');
            ++buf.position();
            return true;
        }
        else
            return false;
    };

    UInt16 year = 0;
    if (!append_digit(year)
        || !append_digit(year) // NOLINT
        || !append_digit(year) // NOLINT
        || !append_digit(year)) // NOLINT
        return error();

    if (!ignore_delimiter())
        return error();

    UInt8 month = 0;
    if (!append_digit(month))
        return error();
    append_digit(month);

    if (!ignore_delimiter())
        return error();

    UInt8 day = 0;
    if (!append_digit(day))
        return error();
    append_digit(day);

    date = LocalDate(year, month, day);
    return ReturnType(true);
}

template void readDateTextFallback<void>(LocalDate &, ReadBuffer &);
template bool readDateTextFallback<bool>(LocalDate &, ReadBuffer &);


template <typename ReturnType>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto date_time_broken_down_length = 19;
    /// YYYY-MM-DD
    static constexpr auto date_broken_down_length = 10;

    char s[date_time_broken_down_length];
    char * s_pos = s;

    /** Read characters, that could represent unix timestamp.
      * Only unix timestamp of at least 5 characters is supported.
      * Then look at 5th character. If it is a number - treat whole as unix timestamp.
      * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format.
      */

    /// A piece similar to unix timestamp, maybe scaled to subsecond precision.
    while (s_pos < s + date_time_broken_down_length && !buf.eof() && isNumericASCII(*buf.position()))
    {
        *s_pos = *buf.position();
        ++s_pos;
        ++buf.position();
    }

    /// 2015-01-01 01:02:03 or 2015-01-01
    if (s_pos == s + 4 && !buf.eof() && !isNumericASCII(*buf.position()))
    {
        const auto already_read_length = s_pos - s;
        const size_t remaining_date_time_size = date_time_broken_down_length - already_read_length;
        const size_t remaining_date_size = date_broken_down_length - already_read_length;

        size_t size = buf.read(s_pos, remaining_date_time_size);
        if (size != remaining_date_time_size && size != remaining_date_size)
        {
            s_pos[size] = 0;

            if constexpr (throw_exception)
                throw ParsingException(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
            else
                return false;
        }

        UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
        UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

        UInt8 hour = 0;
        UInt8 minute = 0;
        UInt8 second = 0;

        if (size == remaining_date_time_size)
        {
            hour = (s[11] - '0') * 10 + (s[12] - '0');
            minute = (s[14] - '0') * 10 + (s[15] - '0');
            second = (s[17] - '0') * 10 + (s[18] - '0');
        }

        if (unlikely(year == 0))
            datetime = 0;
        else
            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
    }
    else
    {
        if (s_pos - s >= 5)
        {
            /// Not very efficient.
            datetime = 0;
            for (const char * digit_pos = s; digit_pos < s_pos; ++digit_pos)
                datetime = datetime * 10 + *digit_pos - '0';
        }
        else
        {
            if constexpr (throw_exception)
                throw ParsingException("Cannot parse datetime", ErrorCodes::CANNOT_PARSE_DATETIME);
            else
                return false;
        }
    }

    return ReturnType(true);
}

template void readDateTimeTextFallback<void>(time_t &, ReadBuffer &, const DateLUTImpl &);
template bool readDateTimeTextFallback<bool>(time_t &, ReadBuffer &, const DateLUTImpl &);


void skipJSONField(ReadBuffer & buf, const StringRef & name_of_field)
{
    if (buf.eof())
        throw Exception("Unexpected EOF for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
    else if (*buf.position() == '"') /// skip double-quoted string
    {
        NullOutput sink;
        readJSONStringInto(sink, buf);
    }
    else if (isNumericASCII(*buf.position()) || *buf.position() == '-' || *buf.position() == '+' || *buf.position() == '.') /// skip number
    {
        if (*buf.position() == '+')
            ++buf.position();

        double v;
        if (!tryReadFloatText(v, buf))
            throw Exception("Expected a number field for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
    }
    else if (*buf.position() == 'n') /// skip null
    {
        assertString("null", buf);
    }
    else if (*buf.position() == 't') /// skip true
    {
        assertString("true", buf);
    }
    else if (*buf.position() == 'f') /// skip false
    {
        assertString("false", buf);
    }
    else if (*buf.position() == '[')
    {
        ++buf.position();
        skipWhitespaceIfAny(buf);

        if (!buf.eof() && *buf.position() == ']') /// skip empty array
        {
            ++buf.position();
            return;
        }

        while (true)
        {
            skipJSONField(buf, name_of_field);
            skipWhitespaceIfAny(buf);

            if (!buf.eof() && *buf.position() == ',')
            {
                ++buf.position();
                skipWhitespaceIfAny(buf);
            }
            else if (!buf.eof() && *buf.position() == ']')
            {
                ++buf.position();
                break;
            }
            else
                throw Exception("Unexpected symbol for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
        }
    }
    else if (*buf.position() == '{') /// skip whole object
    {
        ++buf.position();
        skipWhitespaceIfAny(buf);

        while (!buf.eof() && *buf.position() != '}')
        {
            // field name
            if (*buf.position() == '"')
            {
                NullOutput sink;
                readJSONStringInto(sink, buf);
            }
            else
                throw Exception("Unexpected symbol for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);

            // ':'
            skipWhitespaceIfAny(buf);
            if (buf.eof() || !(*buf.position() == ':'))
                throw Exception("Unexpected symbol for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
            ++buf.position();
            skipWhitespaceIfAny(buf);

            skipJSONField(buf, name_of_field);
            skipWhitespaceIfAny(buf);

            // optional ','
            if (!buf.eof() && *buf.position() == ',')
            {
                ++buf.position();
                skipWhitespaceIfAny(buf);
            }
        }

        if (buf.eof())
            throw Exception("Unexpected EOF for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
        ++buf.position();
    }
    else
    {
        throw Exception("Unexpected symbol '" + std::string(*buf.position(), 1) + "' for key '" + name_of_field.toString() + "'", ErrorCodes::INCORRECT_DATA);
    }
}


Exception readException(ReadBuffer & buf, const String & additional_message, bool remote_exception)
{
    int code = 0;
    String name;
    String message;
    String stack_trace;
    bool has_nested = false;    /// Obsolete

    readBinary(code, buf);
    readBinary(name, buf);
    readBinary(message, buf);
    readBinary(stack_trace, buf);
    readBinary(has_nested, buf);

    WriteBufferFromOwnString out;

    if (!additional_message.empty())
        out << additional_message << ". ";

    if (name != "DB::Exception")
        out << name << ". ";

    out << message << ".";

    if (!stack_trace.empty())
        out << " Stack trace:\n\n" << stack_trace;

    return Exception(out.str(), code, remote_exception);
}

void readAndThrowException(ReadBuffer & buf, const String & additional_message)
{
    readException(buf, additional_message).rethrow();
}


void skipToCarriageReturnOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\r'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\r')
        {
            ++buf.position();
            return;
        }
    }
}


void skipToNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


void skipToUnescapedNextLineOrEOF(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n', '\\'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }

        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (buf.eof())
                return;

            /// Skip escaped character. We do not consider escape sequences with more than one character after backslash (\x01).
            /// It's ok for the purpose of this function, because we are interested only in \n and \\.
            ++buf.position();
            continue;
        }
    }
}

void saveUpToPosition(ReadBuffer & in, DB::Memory<> & memory, char * current)
{
    assert(current >= in.position());
    assert(current <= in.buffer().end());

    const size_t old_bytes = memory.size();
    const size_t additional_bytes = current - in.position();
    const size_t new_bytes = old_bytes + additional_bytes;

    /// There are no new bytes to add to memory.
    /// No need to do extra stuff.
    if (new_bytes == 0)
        return;

    assert(in.position() + additional_bytes <= in.buffer().end());
    memory.resize(new_bytes);
    memcpy(memory.data() + old_bytes, in.position(), additional_bytes);
    in.position() = current;
}

bool loadAtPosition(ReadBuffer & in, DB::Memory<> & memory, char * & current)
{
    assert(current <= in.buffer().end());

    if (current < in.buffer().end())
        return true;

    saveUpToPosition(in, memory, current);

    bool loaded_more = !in.eof();
    // A sanity check. Buffer position may be in the beginning of the buffer
    // (normal case), or have some offset from it (AIO).
    assert(in.position() >= in.buffer().begin());
    assert(in.position() <= in.buffer().end());
    current = in.position();

    return loaded_more;
}

}
