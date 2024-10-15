#include <Core/Defines.h>
#include <base/hex.h>
#include <Common/PODArray.h>
#include <Common/StringUtils.h>
#include <Common/memcpySmall.h>
#include <Common/checkStackSize.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/readFloatText.h>
#include <IO/Operators.h>
#include <cstdlib>
#include <bit>
#include <utility>

#include <base/simd.h>

#ifdef __SSE2__
    #include <emmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
#    include <arm_neon.h>
#      pragma clang diagnostic ignored "-Wreserved-identifier"
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
    extern const int CANNOT_PARSE_UUID;
    extern const int INCORRECT_DATA;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int TOO_DEEP_RECURSION;
}

template <size_t num_bytes, typename IteratorSrc, typename IteratorDst>
inline void parseHex(IteratorSrc src, IteratorDst dst)
{
    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; dst_pos < num_bytes; ++dst_pos, src_pos += 2)
        dst[dst_pos] = unhex2(reinterpret_cast<const char *>(&src[src_pos]));
}

UUID parseUUID(std::span<const UInt8> src)
{
    UUID uuid;
    const auto * src_ptr = src.data();
    const auto size = src.size();

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    const std::reverse_iterator dst(reinterpret_cast<UInt8 *>(&uuid) + sizeof(UUID));
#else
    auto * dst = reinterpret_cast<UInt8 *>(&uuid);
#endif
    if (size == 36)
    {
        parseHex<4>(src_ptr, dst + 8);
        parseHex<2>(src_ptr + 9, dst + 12);
        parseHex<2>(src_ptr + 14, dst + 14);
        parseHex<2>(src_ptr + 19, dst);
        parseHex<6>(src_ptr + 24, dst + 2);
    }
    else if (size == 32)
    {
        parseHex<8>(src_ptr, dst + 8);
        parseHex<8>(src_ptr + 16, dst);
    }
    else
        throw Exception(ErrorCodes::CANNOT_PARSE_UUID, "Unexpected length when trying to parse UUID ({})", size);

    return uuid;
}

void NO_INLINE throwAtAssertionFailed(const char * s, ReadBuffer & buf)
{
    WriteBufferFromOwnString out;
    out << quote << s;

    if (buf.eof())
        out << " at end of stream.";
    else
        out << " before: " << quote << String(buf.position(), std::min(SHOW_CHARS_ON_SYNTAX_ERROR, buf.buffer().end() - buf.position()));

    throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot parse input: expected {}", out.str());
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

void assertNotEOF(ReadBuffer & buf)
{
    if (buf.eof())
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after EOF");
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
void readStringUntilNewlineInto(Vector & s, ReadBuffer & buf)
{
    readStringUntilCharsInto<'\n'>(s, buf);
}

template void readStringUntilNewlineInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readStringUntilNewlineInto<String>(String & s, ReadBuffer & buf);

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

void readStringUntilAmpersand(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilCharsInto<'&'>(s, buf);
}

void readStringUntilEquals(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringUntilCharsInto<'='>(s, buf);
}

template void readNullTerminated<PODArray<char>>(PODArray<char> & s, ReadBuffer & buf);
template void readNullTerminated<String>(String & s, ReadBuffer & buf);

void readString(String & s, ReadBuffer & buf)
{
    s.clear();
    readStringInto(s, buf);
}

template void readStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readStringInto<String>(String & s, ReadBuffer & buf);
template void readStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf);

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
template <typename Vector, typename ReturnType = void>
static ReturnType parseComplexEscapeSequence(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw Exception::createDeprecated(message, code);
        return ReturnType(false);
    };

    ++buf.position();

    if (buf.eof())
    {
        return error("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
    }

    char char_after_backslash = *buf.position();

    if (char_after_backslash == 'x')
    {
        ++buf.position();
        /// escape sequence of the form \xAA
        char hex_code[2];

        auto bytes_read = buf.read(hex_code, sizeof(hex_code));

        if (bytes_read != sizeof(hex_code))
        {
            return error("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
        }
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
            && decoded_char != '='  /// TSKV format invented somewhere
            && !isControlASCII(decoded_char))
        {
            s.push_back('\\');
        }

        s.push_back(decoded_char);
        ++buf.position();
    }

    return ReturnType(true);
}

bool parseComplexEscapeSequence(String & s, ReadBuffer & buf)
{
    return parseComplexEscapeSequence<String, bool>(s, buf);
}

template <typename Vector, typename ReturnType>
static ReturnType parseJSONEscapeSequence(Vector & s, ReadBuffer & buf, bool keep_bad_sequences)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](const char * message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw Exception::createDeprecated(message, code);
        return ReturnType(false);
    };

    ++buf.position();

    if (buf.eof())
    {
        if (keep_bad_sequences)
            return ReturnType(true);
        return error("Cannot parse escape sequence: unexpected eof", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
    }

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
            if (keep_bad_sequences)
            {
                for (size_t i = 0; i != 4; ++i)
                {
                    if (buf.eof() || *buf.position() == '"')
                    {
                        /// Save initial data without parsing of escape sequence.
                        s.push_back('\\');
                        s.push_back('u');
                        for (size_t j = 0; j != i; ++j)
                            s.push_back(hex_code[j]);
                        return ReturnType(true);
                    }

                    hex_code[i] = *buf.position();
                    ++buf.position();
                }
            }
            else
            {
                if (4 != buf.read(hex_code, 4))
                    return error(
                        "Cannot parse escape sequence: less than four bytes after \\u. In JSON input formats you can disable setting "
                        "input_format_json_throw_on_bad_escape_sequence to save bad escape sequences as is",
                        ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
            }

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
                    auto restore_first_unicode = [&]()
                    {
                        s.push_back('\\');
                        s.push_back('u');
                        for (char & c : hex_code)
                            s.push_back(c);
                    };

                    if (keep_bad_sequences)
                    {
                        if (buf.eof() || *buf.position() != '\\')
                        {
                            restore_first_unicode();
                            return ReturnType(true);
                        }

                        ++buf.position();
                        if (buf.eof() || *buf.position() != 'u')
                        {
                            restore_first_unicode();
                            s.push_back('\\');
                            return ReturnType(true);
                        }

                        ++buf.position();
                    }
                    else
                    {
                        if (!checkString("\\u", buf))
                            return error(
                                "Cannot parse escape sequence: missing second part of surrogate pair. In JSON input formats you can "
                                "disable setting input_format_json_throw_on_bad_escape_sequence to save bad escape sequences as is",
                                ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
                    }

                    char second_hex_code[4];
                    if (keep_bad_sequences)
                    {
                        for (size_t i = 0; i != 4; ++i)
                        {
                            if (buf.eof() || *buf.position() == '"')
                            {
                                /// Save initial data without parsing of escape sequence.
                                restore_first_unicode();
                                s.push_back('\\');
                                s.push_back('u');
                                for (size_t j = 0; j != i; ++j)
                                    s.push_back(second_hex_code[j]);
                                return ReturnType(true);
                            }

                            second_hex_code[i] = *buf.position();
                            ++buf.position();
                        }
                    }
                    else
                    {
                        if (4 != buf.read(second_hex_code, 4))
                            return error(
                                "Cannot parse escape sequence: less than four bytes after \\u of second part of surrogate pair. In JSON "
                                "input formats you can disable setting input_format_json_throw_on_bad_escape_sequence to save bad escape "
                                "sequences as is",
                                ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
                    }

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
                    {
                        if (!keep_bad_sequences)
                            return error(
                                "Incorrect surrogate pair of unicode escape sequences in JSON. In JSON input formats you can disable "
                                "setting input_format_json_throw_on_bad_escape_sequence to save bad escape sequences as is",
                                ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);

                        /// Save initial data without parsing of escape sequence.
                        restore_first_unicode();
                        s.push_back('\\');
                        s.push_back('u');
                        for (char & c : second_hex_code)
                            s.push_back(c);
                        return ReturnType(true);
                    }
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


template <typename Vector, bool parse_complex_escape_sequence, bool support_crlf>
void readEscapedStringIntoImpl(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos;
        if constexpr (support_crlf)
        {
            next_pos = find_first_symbols<'\t', '\n', '\\','\r'>(buf.position(), buf.buffer().end());
        }
        else
        {
            next_pos = find_first_symbols<'\t', '\n', '\\'>(buf.position(), buf.buffer().end());
        }
        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n')
            return;

        if (*buf.position() == '\\')
        {
            if constexpr (parse_complex_escape_sequence)
            {
                parseComplexEscapeSequence(s, buf);
            }
            else
            {
                s.push_back(*buf.position());
                ++buf.position();
                if (!buf.eof())
                {
                    s.push_back(*buf.position());
                    ++buf.position();
                }
            }
        }

        if constexpr (support_crlf)
        {
            if (*buf.position() == '\r')
            {
                ++buf.position();
                if (!buf.eof() && *buf.position() != '\n')
                {
                    s.push_back('\r');
                    continue;
                }
                return;
            }
        }
    }
}

template <typename Vector, bool support_crlf>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
    readEscapedStringIntoImpl<Vector, true, support_crlf>(s, buf);
}


void readEscapedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringInto<String,false>(s, buf);
}

void readEscapedStringCRLF(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringInto<String,true>(s, buf);
}

template void readEscapedStringInto<PaddedPODArray<UInt8>,false>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readEscapedStringInto<NullOutput,false>(NullOutput & s, ReadBuffer & buf);
template void readEscapedStringInto<PaddedPODArray<UInt8>,true>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readEscapedStringInto<NullOutput,true>(NullOutput & s, ReadBuffer & buf);

/** If enable_sql_style_quoting == true,
  *  strings like 'abc''def' will be parsed as abc'def.
  * Please note, that even with SQL style quoting enabled,
  *  backslash escape sequences are also parsed,
  *  that could be slightly confusing.
  */
template <char quote, bool enable_sql_style_quoting, typename Vector, typename ReturnType = void>
static ReturnType readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    if (buf.eof() || *buf.position() != quote)
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
                "Cannot parse quoted string: expected opening quote '{}', got '{}'",
                std::string{quote}, buf.eof() ? "EOF" : std::string{*buf.position()});
        else
            return ReturnType(false);
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

            return ReturnType(true);
        }

        if (*buf.position() == '\\')
        {
            if constexpr (throw_exception)
                parseComplexEscapeSequence<Vector, ReturnType>(s, buf);
            else
            {
                if (!parseComplexEscapeSequence<Vector, ReturnType>(s, buf))
                    return ReturnType(false);
            }
        }
    }

    if constexpr (throw_exception)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUOTED_STRING, "Cannot parse quoted string: expected closing quote");
    else
        return ReturnType(false);
}

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'\'', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
bool tryReadQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    return readAnyQuotedStringInto<'\'', enable_sql_style_quoting, Vector, bool>(s, buf);
}

template bool tryReadQuotedStringInto<true, String>(String & s, ReadBuffer & buf);
template bool tryReadQuotedStringInto<false, String>(String & s, ReadBuffer & buf);
template bool tryReadQuotedStringInto<true, PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template bool tryReadQuotedStringInto<false, PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);

template <bool enable_sql_style_quoting, typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'"', enable_sql_style_quoting>(s, buf);
}

template <bool enable_sql_style_quoting, typename Vector>
bool tryReadDoubleQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    return readAnyQuotedStringInto<'"', enable_sql_style_quoting, Vector, bool>(s, buf);
}

template bool tryReadDoubleQuotedStringInto<true, String>(String & s, ReadBuffer & buf);
template bool tryReadDoubleQuotedStringInto<false, String>(String & s, ReadBuffer & buf);


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

bool tryReadQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    return tryReadQuotedStringInto<false>(s, buf);
}

bool tryReadQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    return tryReadQuotedStringInto<true>(s, buf);
}


template void readQuotedStringInto<true>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template void readQuotedStringInto<true>(String & s, ReadBuffer & buf);
template void readQuotedStringInto<false>(String & s, ReadBuffer & buf);
template void readDoubleQuotedStringInto<false>(NullOutput & s, ReadBuffer & buf);
template void readDoubleQuotedStringInto<false>(String & s, ReadBuffer & buf);
template void readBackQuotedStringInto<false>(String & s, ReadBuffer & buf);

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

bool tryReadDoubleQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    return tryReadDoubleQuotedStringInto<false>(s, buf);
}

bool tryReadDoubleQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    return tryReadDoubleQuotedStringInto<true>(s, buf);
}

void readBackQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<false>(s, buf);
}

bool tryReadBackQuotedString(String & s, ReadBuffer & buf)
{
    s.clear();
    return readAnyQuotedStringInto<'`', false, String, bool>(s, buf);
}

void readBackQuotedStringWithSQLStyle(String & s, ReadBuffer & buf)
{
    s.clear();
    readBackQuotedStringInto<true>(s, buf);
}

template<typename T>
concept WithResize = requires (T value)
{
    { value.resize(1) };
    { value.size() } -> std::integral<>;
};

template <typename Vector, bool include_quotes, bool allow_throw>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    /// Empty string
    if (buf.eof())
        return;

    const char delimiter = settings.delimiter;
    const char maybe_quote = *buf.position();
    const String & custom_delimiter = settings.custom_delimiter;

    /// Emptiness and not even in quotation marks.
    if (custom_delimiter.empty() && maybe_quote == delimiter)
        return;

    if ((settings.allow_single_quotes && maybe_quote == '\'') || (settings.allow_double_quotes && maybe_quote == '"'))
    {
        if constexpr (include_quotes)
            s.push_back(maybe_quote);

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

            if constexpr (include_quotes)
                s.push_back(maybe_quote);

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
        /// If custom_delimiter is specified, we should read until first occurrences of
        /// custom_delimiter in buffer.
        if (!custom_delimiter.empty())
        {
            PeekableReadBuffer * peekable_buf = dynamic_cast<PeekableReadBuffer *>(&buf);
            if (!peekable_buf)
            {
                if constexpr (allow_throw)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading CSV string with custom delimiter is allowed only when using PeekableReadBuffer");
                return;
            }

            while (true)
            {
                if (peekable_buf->eof())
                {
                    if constexpr (allow_throw)
                        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading CSV string, expected custom delimiter \"{}\"", custom_delimiter);
                    return;
                }

                char * next_pos = reinterpret_cast<char *>(memchr(peekable_buf->position(), custom_delimiter[0], peekable_buf->available()));
                if (!next_pos)
                    next_pos = peekable_buf->buffer().end();

                appendToStringOrVector(s, *peekable_buf, next_pos);
                peekable_buf->position() = next_pos;

                if (!buf.hasPendingData())
                    continue;

                {
                    PeekableReadBufferCheckpoint checkpoint{*peekable_buf, true};
                    if (checkString(custom_delimiter, *peekable_buf))
                        return;
                }

                s.push_back(*peekable_buf->position());
                ++peekable_buf->position();
            }

            return;
        }

        /// Unquoted case. Look for delimiter or \r (followed by '\n') or \n.
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
                        next_pos += std::countr_zero(bit_mask);
                        return;
                    }
                }
#elif defined(__aarch64__) && defined(__ARM_NEON)
                auto rc = vdupq_n_u8('\r');
                auto nc = vdupq_n_u8('\n');
                auto dc = vdupq_n_u8(delimiter);
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    uint8x16_t bytes = vld1q_u8(reinterpret_cast<const uint8_t *>(next_pos));
                    auto eq = vorrq_u8(vorrq_u8(vceqq_u8(bytes, rc), vceqq_u8(bytes, nc)), vceqq_u8(bytes, dc));
                    uint64_t bit_mask = getNibbleMask(eq);
                    if (bit_mask)
                    {
                        next_pos += std::countr_zero(bit_mask) >> 2;
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

            /// Check for single '\r' not followed by '\n'
            /// We should not stop in this case.
            if (*buf.position() == '\r' && !settings.allow_cr_end_of_line)
            {
                ++buf.position();
                if (!buf.eof() && *buf.position() != '\n')
                {
                    s.push_back('\r');
                    continue;
                }
            }

            if constexpr (WithResize<Vector>)
            {
                if (settings.trim_whitespaces) [[likely]]
                {
                    /** CSV format can contain insignificant spaces and tabs.
                    * Usually the task of skipping them is for the calling code.
                    * But in this case, it will be difficult to do this, so remove the trailing whitespace by ourself.
                    */
                    size_t size = s.size();
                    while (size > 0 && (s[size - 1] == ' ' || s[size - 1] == '\t'))
                        --size;

                    s.resize(size);
                }
            }
            return;
        }
    }
}

void readCSVString(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    s.clear();
    readCSVStringInto(s, buf, settings);
}

void readCSVField(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    s.clear();
    readCSVStringInto<String, true>(s, buf, settings);
}

void readCSVWithTwoPossibleDelimitersImpl(String & s, PeekableReadBuffer & buf, const String & first_delimiter, const String & second_delimiter)
{
    /// Check that delimiters are not empty.
    if (first_delimiter.empty() || second_delimiter.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Cannot read CSV field with two possible delimiters, one "
                        "of delimiters '{}' and '{}' is empty", first_delimiter, second_delimiter);

    /// Read all data until first_delimiter or second_delimiter
    while (true)
    {
        if (buf.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, R"(Unexpected EOF while reading CSV string, expected on "
                            "of delimiters "{}" or "{}")", first_delimiter, second_delimiter);

        char * next_pos = buf.position();
        while (next_pos != buf.buffer().end() && *next_pos != first_delimiter[0] && *next_pos != second_delimiter[0])
            ++next_pos;

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;
        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == first_delimiter[0])
        {
            PeekableReadBufferCheckpoint checkpoint(buf, true);
            if (checkString(first_delimiter, buf))
                return;
        }

        if (*buf.position() == second_delimiter[0])
        {
            PeekableReadBufferCheckpoint checkpoint(buf, true);
            if (checkString(second_delimiter, buf))
                return;
        }

        s.push_back(*buf.position());
        ++buf.position();
    }
}

String readCSVStringWithTwoPossibleDelimiters(PeekableReadBuffer & buf, const FormatSettings::CSV & settings, const String & first_delimiter, const String & second_delimiter)
{
    String res;

    /// If value is quoted, use regular CSV reading since we need to read only data inside quotes.
    if (!buf.eof() && ((settings.allow_single_quotes && *buf.position() == '\'') || (settings.allow_double_quotes && *buf.position() == '"')))
        readCSVStringInto(res, buf, settings);
    else
        readCSVWithTwoPossibleDelimitersImpl(res, buf, first_delimiter, second_delimiter);

    return res;
}

String readCSVFieldWithTwoPossibleDelimiters(PeekableReadBuffer & buf, const FormatSettings::CSV & settings, const String & first_delimiter, const String & second_delimiter)
{
    String res;

    /// If value is quoted, use regular CSV reading since we need to read only data inside quotes.
    if (!buf.eof() && ((settings.allow_single_quotes && *buf.position() == '\'') || (settings.allow_double_quotes && *buf.position() == '"')))
         readCSVField(res, buf, settings);
    else
        readCSVWithTwoPossibleDelimitersImpl(res, buf, first_delimiter, second_delimiter);

    return res;
}

template void readCSVStringInto<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf, const FormatSettings::CSV & settings);
template void readCSVStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf, const FormatSettings::CSV & settings);
template void readCSVStringInto<String, false, false>(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);
template void readCSVStringInto<String, true, false>(String & s, ReadBuffer & buf, const FormatSettings::CSV & settings);
template void readCSVStringInto<PaddedPODArray<UInt8>, false, false>(PaddedPODArray<UInt8> & s, ReadBuffer & buf, const FormatSettings::CSV & settings);


template <typename Vector, typename ReturnType>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf, const FormatSettings::JSON & settings)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](FormatStringHelper<> message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw Exception(code, std::move(message));
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
            parseJSONEscapeSequence<Vector, ReturnType>(s, buf, !settings.throw_on_bad_escape_sequence);
    }

    return error("Cannot parse JSON string: expected closing quote", ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}

void readJSONString(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings)
{
    s.clear();
    readJSONStringInto(s, buf, settings);
}

template void readJSONStringInto<PaddedPODArray<UInt8>, void>(PaddedPODArray<UInt8> & s, ReadBuffer & buf, const FormatSettings::JSON & settings);
template bool readJSONStringInto<PaddedPODArray<UInt8>, bool>(PaddedPODArray<UInt8> & s, ReadBuffer & buf, const FormatSettings::JSON & settings);
template void readJSONStringInto<NullOutput>(NullOutput & s, ReadBuffer & buf, const FormatSettings::JSON & settings);
template void readJSONStringInto<String>(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings);
template bool readJSONStringInto<String, bool>(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings);

template <typename Vector, typename ReturnType, char opening_bracket, char closing_bracket>
ReturnType readJSONObjectOrArrayPossiblyInvalid(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](FormatStringHelper<> message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw Exception(code, std::move(message));
        return ReturnType(false);
    };

    if (buf.eof() || *buf.position() != opening_bracket)
        return error("JSON object/array should start with corresponding opening bracket", ErrorCodes::INCORRECT_DATA);

    s.push_back(*buf.position());
    ++buf.position();

    Int64 balance = 1;
    bool quotes = false;

    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', opening_bracket, closing_bracket, '"'>(buf.position(), buf.buffer().end());
        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        s.push_back(*buf.position());

        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (!buf.eof())
            {
                s.push_back(*buf.position());
                ++buf.position();
            }

            continue;
        }

        if (*buf.position() == '"')
            quotes = !quotes;
        else if (!quotes) // can be only opening_bracket or closing_bracket
            balance += *buf.position() == opening_bracket ? 1 : -1;

        ++buf.position();

        if (balance == 0)
            return ReturnType(true);

        if (balance < 0)
            break;
    }

    return error("JSON object/array should have equal number of opening and closing brackets", ErrorCodes::INCORRECT_DATA);
}

template <typename Vector, typename ReturnType>
ReturnType readJSONObjectPossiblyInvalid(Vector & s, ReadBuffer & buf)
{
    return readJSONObjectOrArrayPossiblyInvalid<Vector, ReturnType, '{', '}'>(s, buf);
}

template void readJSONObjectPossiblyInvalid<String>(String & s, ReadBuffer & buf);
template bool readJSONObjectPossiblyInvalid<String, bool>(String & s, ReadBuffer & buf);
template void readJSONObjectPossiblyInvalid<PaddedPODArray<UInt8>>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template bool readJSONObjectPossiblyInvalid<PaddedPODArray<UInt8>, bool>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);

template <typename Vector, typename ReturnType>
ReturnType readJSONArrayInto(Vector & s, ReadBuffer & buf)
{
    return readJSONObjectOrArrayPossiblyInvalid<Vector, ReturnType, '[', ']'>(s, buf);
}

template void readJSONArrayInto<PaddedPODArray<UInt8>, void>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);
template bool readJSONArrayInto<PaddedPODArray<UInt8>, bool>(PaddedPODArray<UInt8> & s, ReadBuffer & buf);

std::string_view readJSONObjectAsViewPossiblyInvalid(ReadBuffer & buf, String & object_buffer)
{
    if (buf.eof() || *buf.position() != '{')
        throw Exception(ErrorCodes::INCORRECT_DATA, "JSON object should start with '{{'");

    char * start = buf.position();
    bool use_object_buffer = false;
    object_buffer.clear();

    ++buf.position();
    Int64 balance = 1;
    bool quotes = false;

    while (true)
    {
        if (!buf.hasPendingData() && !use_object_buffer)
        {
            use_object_buffer = true;
            object_buffer.append(start, buf.position() - start);
        }

        if (buf.eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON object");

        char * next_pos = find_first_symbols<'\\', '{', '}', '"'>(buf.position(), buf.buffer().end());
        if (use_object_buffer)
            object_buffer.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (use_object_buffer)
            object_buffer.push_back(*buf.position());

        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (!buf.hasPendingData() && !use_object_buffer)
            {
                use_object_buffer = true;
                object_buffer.append(start, buf.position() - start);
            }

            if (buf.eof())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading JSON object");

            if (use_object_buffer)
                object_buffer.push_back(*buf.position());
            ++buf.position();

            continue;
        }

        if (*buf.position() == '"')
            quotes = !quotes;
        else if (!quotes) // can be only opening_bracket or closing_bracket
            balance += *buf.position() == '{' ? 1 : -1;

        ++buf.position();

        if (balance == 0)
        {
            if (use_object_buffer)
                return object_buffer;
            return {start, buf.position()};
        }

        if (balance < 0)
            break;
    }

    throw Exception(ErrorCodes::INCORRECT_DATA, "JSON object should have equal number of opening and closing brackets");
}

template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf, const char * allowed_delimiters)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = []
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::CANNOT_PARSE_DATE, "Cannot parse date: value is too short");
        return ReturnType(false);
    };

    auto append_digit = [&](auto & x)
    {
        if (!buf.eof() && isNumericASCII(*buf.position()))
        {
            x = x * 10 + (*buf.position() - '0');
            ++buf.position();
            return true;
        }
        return false;
    };

    UInt16 year = 0;
    UInt8 month = 0;
    UInt8 day = 0;

    if (!append_digit(year)
        || !append_digit(year) // NOLINT
        || !append_digit(year) // NOLINT
        || !append_digit(year)) // NOLINT
        return error();

    if (buf.eof())
        return error();

    if (isNumericASCII(*buf.position()))
    {
        /// YYYYMMDD
        if (!append_digit(month)
            || !append_digit(month) // NOLINT
            || !append_digit(day)
            || !append_digit(day)) // NOLINT
            return error();
    }
    else
    {
        if (!isSymbolIn(*buf.position(), allowed_delimiters))
            return error();

        ++buf.position();

        if (!append_digit(month))
            return error();
        append_digit(month);

        if (!buf.eof() && !isNumericASCII(*buf.position()))
        {
            if (!isSymbolIn(*buf.position(), allowed_delimiters))
                return error();
            ++buf.position();
        }
        else
            return error();

        if (!append_digit(day))
            return error();
        append_digit(day);
    }

    date = LocalDate(year, month, day);
    return ReturnType(true);
}

template void readDateTextFallback<void>(LocalDate &, ReadBuffer &, const char * allowed_delimiters);
template bool readDateTextFallback<bool>(LocalDate &, ReadBuffer &, const char * allowed_delimiters);


template <typename ReturnType, bool dt64_mode>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut, const char * allowed_date_delimiters, const char * allowed_time_delimiters)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    /// YYYY-MM-DD
    static constexpr auto date_broken_down_length = 10;
    /// hh:mm:ss
    static constexpr auto time_broken_down_length = 8;
    /// YYYY-MM-DD hh:mm:ss
    static constexpr auto date_time_broken_down_length = date_broken_down_length + 1 + time_broken_down_length;

    char s[date_time_broken_down_length];
    char * s_pos = s;

    /** Read characters, that could represent unix timestamp.
      * Only unix timestamp of at least 5 characters is supported by default, exception is thrown for a shorter one
      * (unless parsing a string like '1.23' or '-12': there is no ambiguity, it is a DT64 timestamp).
      * Then look at 5th character. If it is a number - treat whole as unix timestamp.
      * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss or YYYY-MM-DD format.
      */

    int negative_multiplier = 1;

    if (!buf.eof() && *buf.position() == '-')
    {
        if constexpr (dt64_mode)
        {
            negative_multiplier = -1;
            ++buf.position();
        }
        else
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime");
            else
                return false;
        }
    }

    /// A piece similar to unix timestamp, maybe scaled to subsecond precision.
    while (s_pos < s + date_time_broken_down_length && !buf.eof() && isNumericASCII(*buf.position()))
    {
        *s_pos = *buf.position();
        ++s_pos;
        ++buf.position();
    }

    /// 2015-01-01 01:02:03 or 2015-01-01
    /// if negative, it is a timestamp with no ambiguity
    if (negative_multiplier == 1 && s_pos == s + 4 && !buf.eof() && !isNumericASCII(*buf.position()))
    {
        const auto already_read_length = s_pos - s;
        const size_t remaining_date_size = date_broken_down_length - already_read_length;

        size_t size = buf.read(s_pos, remaining_date_size);
        if (size != remaining_date_size)
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime {}", std::string_view(s, already_read_length + size));
            else
                return false;
        }

        if constexpr (!throw_exception)
        {
            if (!isNumericASCII(s[0]) || !isNumericASCII(s[1]) || !isNumericASCII(s[2]) || !isNumericASCII(s[3])
                || !isNumericASCII(s[5]) || !isNumericASCII(s[6]) || !isNumericASCII(s[8]) || !isNumericASCII(s[9]))
                return false;

            if (!isSymbolIn(s[4], allowed_date_delimiters) || !isSymbolIn(s[7], allowed_date_delimiters))
                return false;
        }

        UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
        UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
        UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

        UInt8 hour = 0;
        UInt8 minute = 0;
        UInt8 second = 0;

        if (!buf.eof() && (*buf.position() == ' ' || *buf.position() == 'T'))
        {
            ++buf.position();
            size = buf.read(s, time_broken_down_length);

            if (size != time_broken_down_length)
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse time component of DateTime {}", std::string_view(s, size));
                else
                    return false;
            }

            if constexpr (!throw_exception)
            {
                if (!isNumericASCII(s[0]) || !isNumericASCII(s[1]) || !isNumericASCII(s[3]) || !isNumericASCII(s[4])
                    || !isNumericASCII(s[6]) || !isNumericASCII(s[7]))
                    return false;

                if (!isSymbolIn(s[2], allowed_time_delimiters) || !isSymbolIn(s[5], allowed_time_delimiters))
                    return false;
            }

            hour = (s[0] - '0') * 10 + (s[1] - '0');
            minute = (s[3] - '0') * 10 + (s[4] - '0');
            second = (s[6] - '0') * 10 + (s[7] - '0');
        }

        if (unlikely(year == 0))
            datetime = 0;
        else
            datetime = date_lut.makeDateTime(year, month, day, hour, minute, second);
    }
    else
    {
        datetime = 0;
        bool too_short = s_pos - s <= 4;

        if (!too_short || dt64_mode)
        {
            /// Not very efficient.
            for (const char * digit_pos = s; digit_pos < s_pos; ++digit_pos)
            {
                if constexpr (!throw_exception)
                {
                    if (!isNumericASCII(*digit_pos))
                        return false;
                }
                datetime = datetime * 10 + *digit_pos - '0';
            }
        }
        datetime *= negative_multiplier;

        if (too_short && negative_multiplier != -1)
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime");
            else
                return false;
        }

    }

    return ReturnType(true);
}

template void readDateTimeTextFallback<void, false>(time_t &, ReadBuffer &, const DateLUTImpl &, const char *, const char *);
template void readDateTimeTextFallback<void, true>(time_t &, ReadBuffer &, const DateLUTImpl &, const char *, const char *);
template bool readDateTimeTextFallback<bool, false>(time_t &, ReadBuffer &, const DateLUTImpl &, const char *, const char *);
template bool readDateTimeTextFallback<bool, true>(time_t &, ReadBuffer &, const DateLUTImpl &, const char *, const char *);


template <typename ReturnType>
ReturnType skipJSONFieldImpl(ReadBuffer & buf, StringRef name_of_field, const FormatSettings::JSON & settings, size_t current_depth)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (unlikely(current_depth > settings.max_depth))
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "JSON is too deep for key '{}'", name_of_field.toString());
        return ReturnType(false);
    }

    if (unlikely(current_depth > 0 && current_depth % 1024 == 0))
        checkStackSize();

    if (buf.eof())
    {
        if constexpr (throw_exception)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF for key '{}'", name_of_field.toString());
        return ReturnType(false);
    }
    if (*buf.position() == '"') /// skip double-quoted string
    {
        NullOutput sink;
        if constexpr (throw_exception)
            readJSONStringInto(sink, buf, settings);
        else if (!tryReadJSONStringInto(sink, buf, settings))
            return ReturnType(false);
    }
    else if (isNumericASCII(*buf.position()) || *buf.position() == '-' || *buf.position() == '+' || *buf.position() == '.') /// skip number
    {
        if (*buf.position() == '+')
            ++buf.position();

        double v;
        if (!tryReadFloatText(v, buf))
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a number field for key '{}'", name_of_field.toString());
            return ReturnType(false);
        }
    }
    else if (*buf.position() == 'n') /// skip null
    {
        if constexpr (throw_exception)
            assertString("null", buf);
        else if (!checkString("null", buf))
            return ReturnType(false);
    }
    else if (*buf.position() == 't') /// skip true
    {
        if constexpr (throw_exception)
            assertString("true", buf);
        else if (!checkString("true", buf))
            return ReturnType(false);
    }
    else if (*buf.position() == 'f') /// skip false
    {
        if constexpr (throw_exception)
            assertString("false", buf);
        else if (!checkString("false", buf))
            return ReturnType(false);
    }
    else if (*buf.position() == '[')
    {
        ++buf.position();
        skipWhitespaceIfAny(buf);

        if (!buf.eof() && *buf.position() == ']') /// skip empty array
        {
            ++buf.position();
            return ReturnType(true);
        }

        while (true)
        {
            if constexpr (throw_exception)
                skipJSONFieldImpl<ReturnType>(buf, name_of_field, settings, current_depth + 1);
            else if (!skipJSONFieldImpl<ReturnType>(buf, name_of_field, settings, current_depth + 1))
                return ReturnType(false);

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
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());
                return ReturnType(false);
            }
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
                if constexpr (throw_exception)
                    readJSONStringInto(sink, buf, settings);
                else if (!tryReadJSONStringInto(sink, buf, settings))
                    return ReturnType(false);
            }
            else
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());
                return ReturnType(false);
            }

            // ':'
            skipWhitespaceIfAny(buf);
            if (buf.eof() || !(*buf.position() == ':'))
            {
                if constexpr (throw_exception)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());
                return ReturnType(false);
            }
            ++buf.position();
            skipWhitespaceIfAny(buf);

            if constexpr (throw_exception)
                skipJSONFieldImpl<ReturnType>(buf, name_of_field, settings, current_depth + 1);
            else if (!skipJSONFieldImpl<ReturnType>(buf, name_of_field, settings, current_depth + 1))
                return ReturnType(false);

            skipWhitespaceIfAny(buf);

            // optional ','
            if (!buf.eof() && *buf.position() == ',')
            {
                ++buf.position();
                skipWhitespaceIfAny(buf);
            }
        }

        if (buf.eof())
        {
            if constexpr (throw_exception)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF for key '{}'", name_of_field.toString());
            return ReturnType(false);
        }
        ++buf.position();
    }
    else
    {
        if constexpr (throw_exception)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot read JSON field here: '{}'. Unexpected symbol '{}'{}",
                String(buf.position(), std::min(buf.available(), size_t(10))),
                std::string(1, *buf.position()),
                name_of_field.empty() ? "" : " for key " + name_of_field.toString());

        return ReturnType(false);
    }

    return ReturnType(true);
}

void skipJSONField(ReadBuffer & buf, StringRef name_of_field, const FormatSettings::JSON & settings)
{
    skipJSONFieldImpl<void>(buf, name_of_field, settings, 0);
}

bool trySkipJSONField(ReadBuffer & buf, StringRef name_of_field, const FormatSettings::JSON & settings)
{
    return skipJSONFieldImpl<bool>(buf, name_of_field, settings, 0);
}


Exception readException(ReadBuffer & buf, const String & additional_message, bool remote_exception)
{
    int code = 0;
    String name;
    String message;
    String stack_trace;
    bool has_nested = false;    /// Obsolete

    readBinaryLittleEndian(code, buf);
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

    return Exception::createDeprecated(out.str(), code, remote_exception);
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

void skipNullTerminated(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\0'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\0')
        {
            ++buf.position();
            return;
        }
    }
}


void saveUpToPosition(ReadBuffer & in, Memory<> & memory, char * current)
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

bool loadAtPosition(ReadBuffer & in, Memory<> & memory, char * & current)
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

/// Searches for delimiter in input stream and sets buffer position after delimiter (if found) or EOF (if not)
static void findAndSkipNextDelimiter(PeekableReadBuffer & buf, const String & delimiter)
{
    if (delimiter.empty())
        return;

    while (!buf.eof())
    {
        void * pos = memchr(buf.position(), delimiter[0], buf.available());
        if (!pos)
        {
            buf.position() += buf.available();
            continue;
        }

        buf.position() = static_cast<ReadBuffer::Position>(pos);

        PeekableReadBufferCheckpoint checkpoint{buf};
        if (checkString(delimiter, buf))
            return;

        buf.rollbackToCheckpoint();
        ++buf.position();
    }
}

void skipToNextRowOrEof(PeekableReadBuffer & buf, const String & row_after_delimiter, const String & row_between_delimiter, bool skip_spaces)
{
    if (row_after_delimiter.empty())
    {
        findAndSkipNextDelimiter(buf, row_between_delimiter);
        return;
    }

    while (true)
    {
        findAndSkipNextDelimiter(buf, row_after_delimiter);

        if (skip_spaces)
            skipWhitespaceIfAny(buf);

        if (buf.eof() || checkString(row_between_delimiter, buf))
            break;
    }
}

// Use PeekableReadBuffer to copy field to string after parsing.
template <typename ReturnType, typename Vector, typename ParseFunc>
static ReturnType readParsedValueInto(Vector & s, ReadBuffer & buf, ParseFunc parse_func)
{
    PeekableReadBuffer peekable_buf(buf);
    peekable_buf.setCheckpoint();
    if constexpr (std::is_same_v<ReturnType, void>)
        parse_func(peekable_buf);
    else if (!parse_func(peekable_buf))
        return ReturnType(false);
    peekable_buf.makeContinuousMemoryFromCheckpointToPos();
    auto * end = peekable_buf.position();
    peekable_buf.rollbackToCheckpoint();
    s.append(peekable_buf.position(), end);
    peekable_buf.position() = end;
    return ReturnType(true);
}

void readParsedValueIntoString(String & s, ReadBuffer & buf, std::function<void(ReadBuffer &)> parse_func)
{
    readParsedValueInto<void>(s, buf, std::move(parse_func));
}

template <typename ReturnType = void, typename Vector>
static ReturnType readQuotedStringFieldInto(Vector & s, ReadBuffer & buf)
{
    if constexpr (std::is_same_v<ReturnType, void>)
        assertChar('\'', buf);
    else if (!checkChar('\'', buf))
        return ReturnType(false);

    s.push_back('\'');
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', '\''>(buf.position(), buf.buffer().end());

        s.append(buf.position(), next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\'')
            break;

        s.push_back(*buf.position());
        if (*buf.position() == '\\')
        {
            ++buf.position();
            if (!buf.eof())
            {
                s.push_back(*buf.position());
                ++buf.position();
            }
        }
    }

    if (buf.eof())
        return ReturnType(false);

    ++buf.position();
    s.push_back('\'');
    return ReturnType(true);
}

template <typename ReturnType = void, char opening_bracket, char closing_bracket, typename Vector>
static ReturnType readQuotedFieldInBracketsInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if constexpr (throw_exception)
        assertChar(opening_bracket, buf);
    else if (!checkChar(opening_bracket, buf))
        return ReturnType(false);

    s.push_back(opening_bracket);

    size_t balance = 1;

    while (!buf.eof() && balance)
    {
        char * next_pos = find_first_symbols<'\'', opening_bracket, closing_bracket>(buf.position(), buf.buffer().end());
        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\'')
        {
            if constexpr (throw_exception)
                readQuotedStringFieldInto<void>(s, buf);
            else if (!readQuotedStringFieldInto<bool>(s, buf))
                return ReturnType(false);
        }
        else if (*buf.position() == opening_bracket)
        {
            s.push_back(opening_bracket);
            ++balance;
            ++buf.position();
        }
        else if (*buf.position() == closing_bracket)
        {
            s.push_back(closing_bracket);
            --balance;
            ++buf.position();
        }
    }

    if (balance)
        return ReturnType(false);

    return ReturnType(true);
}

template <typename ReturnType, typename Vector>
ReturnType readQuotedFieldInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    if (buf.eof())
        return ReturnType(false);

    /// Possible values in 'Quoted' field:
    /// - Strings: '...'
    /// - Arrays: [...]
    /// - Tuples: (...)
    /// - Maps: {...}
    /// - NULL
    /// - Bool: true/false
    /// - Number: integer, float, decimal.

    if (*buf.position() == '\'')
        return readQuotedStringFieldInto<ReturnType>(s, buf);
    if (*buf.position() == '[')
        return readQuotedFieldInBracketsInto<ReturnType, '[', ']'>(s, buf);
    if (*buf.position() == '(')
        return readQuotedFieldInBracketsInto<ReturnType, '(', ')'>(s, buf);
    if (*buf.position() == '{')
        return readQuotedFieldInBracketsInto<ReturnType, '{', '}'>(s, buf);
    if (checkCharCaseInsensitive('n', buf))
    {
        /// NULL or NaN
        if (checkCharCaseInsensitive('u', buf))
        {
            if constexpr (throw_exception)
                assertStringCaseInsensitive("ll", buf);
            else if (!checkStringCaseInsensitive("ll", buf))
                return ReturnType(false);
            s.append("NULL");
        }
        else
        {
            if constexpr (throw_exception)
                assertStringCaseInsensitive("an", buf);
            else if (!checkStringCaseInsensitive("an", buf))
                return ReturnType(false);
            s.append("NaN");
        }
    }
    else if (checkCharCaseInsensitive('t', buf))
    {
        if constexpr (throw_exception)
            assertStringCaseInsensitive("rue", buf);
        else if (!checkStringCaseInsensitive("rue", buf))
            return ReturnType(false);
        s.append("true");
    }
    else if (checkCharCaseInsensitive('f', buf))
    {
        if constexpr (throw_exception)
            assertStringCaseInsensitive("alse", buf);
        else if (!checkStringCaseInsensitive("alse", buf))
            return ReturnType(false);
        s.append("false");
    }
    else
    {
        /// It's an integer, float or decimal. They all can be parsed as float.
        auto parse_func = [](ReadBuffer & in)
        {
            Float64 tmp;
            if constexpr (throw_exception)
                readFloatText(tmp, in);
            else
                return tryReadFloatText(tmp, in);
        };

        return readParsedValueInto<ReturnType>(s, buf, parse_func);
    }

    return ReturnType(true);
}

template void readQuotedFieldInto<void, NullOutput>(NullOutput & s, ReadBuffer & buf);

void readQuotedField(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedFieldInto(s, buf);
}

bool tryReadQuotedField(String & s, ReadBuffer & buf)
{
    s.clear();
    return readQuotedFieldInto<bool>(s, buf);
}

void readJSONField(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings)
{
    s.clear();
    auto parse_func = [&settings](ReadBuffer & in) { skipJSONField(in, "", settings); };
    readParsedValueInto<void>(s, buf, parse_func);
}

bool tryReadJSONField(String & s, ReadBuffer & buf, const FormatSettings::JSON & settings)
{
    s.clear();
    auto parse_func = [&settings](ReadBuffer & in) { return trySkipJSONField(in, "", settings); };
    return readParsedValueInto<bool>(s, buf, parse_func);
}

void readTSVField(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringIntoImpl<String, false, false>(s, buf);
}

void readTSVFieldCRLF(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringIntoImpl<String, false, true>(s, buf);
}


}
