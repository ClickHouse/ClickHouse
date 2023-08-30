#include <Core/Defines.h>
#include <base/hex.h>
#include <Common/PODArray.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/memcpySmall.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/readFloatText.h>
#include <IO/Operators.h>
#include <base/find_symbols.h>
#include <cstdlib>
#include <bit>

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

    throw ParsingException(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot parse input: expected {}", out.str());
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
static ReturnType parseJSONEscapeSequence(Vector & s, ReadBuffer & buf)
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


template <typename Vector, bool parse_complex_escape_sequence>
void readEscapedStringIntoImpl(Vector & s, ReadBuffer & buf)
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
    }
}

template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf)
{
    readEscapedStringIntoImpl<Vector, true>(s, buf);
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
template <char quote, bool enable_sql_style_quoting, typename Vector, typename ReturnType = void>
static ReturnType readAnyQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;
    if (buf.eof() || *buf.position() != quote)
    {
        if constexpr (throw_exception)
            throw ParsingException(ErrorCodes::CANNOT_PARSE_QUOTED_STRING,
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
        throw ParsingException(ErrorCodes::CANNOT_PARSE_QUOTED_STRING, "Cannot parse quoted string: expected closing quote");
    else
        return ReturnType(false);
}

template <bool enable_sql_style_quoting, typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    readAnyQuotedStringInto<'\'', enable_sql_style_quoting>(s, buf);
}

template <typename Vector>
bool tryReadQuotedStringInto(Vector & s, ReadBuffer & buf)
{
    return readAnyQuotedStringInto<'\'', false, Vector, bool>(s, buf);
}

template bool tryReadQuotedStringInto(String & s, ReadBuffer & buf);

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

template<typename T>
concept WithResize = requires (T value)
{
    { value.resize(1) };
    { value.size() } -> std::integral<>;
};

template <typename Vector, bool include_quotes>
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading CSV string with custom delimiter is allowed only when using PeekableReadBuffer");

            while (true)
            {
                if (peekable_buf->eof())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading CSV string, expected custom delimiter \"{}\"", custom_delimiter);

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


template <typename Vector, typename ReturnType>
ReturnType readJSONStringInto(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](FormatStringHelper<> message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw ParsingException(code, std::move(message));
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
template bool readJSONStringInto<String, bool>(String & s, ReadBuffer & buf);

template <typename Vector, typename ReturnType>
ReturnType readJSONObjectPossiblyInvalid(Vector & s, ReadBuffer & buf)
{
    static constexpr bool throw_exception = std::is_same_v<ReturnType, void>;

    auto error = [](FormatStringHelper<> message [[maybe_unused]], int code [[maybe_unused]])
    {
        if constexpr (throw_exception)
            throw ParsingException(code, std::move(message));
        return ReturnType(false);
    };

    if (buf.eof() || *buf.position() != '{')
        return error("JSON should start from opening curly bracket", ErrorCodes::INCORRECT_DATA);

    s.push_back(*buf.position());
    ++buf.position();

    Int64 balance = 1;
    bool quotes = false;

    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\\', '{', '}', '"'>(buf.position(), buf.buffer().end());
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
        else if (!quotes) // can be only '{' or '}'
            balance += *buf.position() == '{' ? 1 : -1;

        ++buf.position();

        if (balance == 0)
            return ReturnType(true);

        if (balance <    0)
            break;
    }

    return error("JSON should have equal number of opening and closing brackets", ErrorCodes::INCORRECT_DATA);
}

template void readJSONObjectPossiblyInvalid<String>(String & s, ReadBuffer & buf);

template <typename ReturnType>
ReturnType readDateTextFallback(LocalDate & date, ReadBuffer & buf)
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
        else
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
        ++buf.position();

        if (!append_digit(month))
            return error();
        append_digit(month);

        if (!buf.eof() && !isNumericASCII(*buf.position()))
            ++buf.position();
        else
            return error();

        if (!append_digit(day))
            return error();
        append_digit(day);
    }

    date = LocalDate(year, month, day);
    return ReturnType(true);
}

template void readDateTextFallback<void>(LocalDate &, ReadBuffer &);
template bool readDateTextFallback<bool>(LocalDate &, ReadBuffer &);


template <typename ReturnType>
ReturnType readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf, const DateLUTImpl & date_lut)
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
        const size_t remaining_date_size = date_broken_down_length - already_read_length;

        size_t size = buf.read(s_pos, remaining_date_size);
        if (size != remaining_date_size)
        {
            s_pos[size] = 0;

            if constexpr (throw_exception)
                throw ParsingException(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse DateTime {}", s);
            else
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
                s_pos[size] = 0;

                if constexpr (throw_exception)
                    throw ParsingException(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse time component of DateTime {}", s);
                else
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
                throw ParsingException(ErrorCodes::CANNOT_PARSE_DATETIME, "Cannot parse datetime");
            else
                return false;
        }
    }

    return ReturnType(true);
}

template void readDateTimeTextFallback<void>(time_t &, ReadBuffer &, const DateLUTImpl &);
template bool readDateTimeTextFallback<bool>(time_t &, ReadBuffer &, const DateLUTImpl &);


void skipJSONField(ReadBuffer & buf, StringRef name_of_field)
{
    if (buf.eof())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF for key '{}'", name_of_field.toString());
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
            throw Exception(ErrorCodes::INCORRECT_DATA, "Expected a number field for key '{}'", name_of_field.toString());
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
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());
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
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());

            // ':'
            skipWhitespaceIfAny(buf);
            if (buf.eof() || !(*buf.position() == ':'))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol for key '{}'", name_of_field.toString());
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
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF for key '{}'", name_of_field.toString());
        ++buf.position();
    }
    else
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected symbol '{}' for key '{}'",
                        std::string(*buf.position(), 1), name_of_field.toString());
    }
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

        if (checkString(row_between_delimiter, buf))
            break;
    }
}

// Use PeekableReadBuffer to copy field to string after parsing.
template <typename Vector, typename ParseFunc>
static void readParsedValueInto(Vector & s, ReadBuffer & buf, ParseFunc parse_func)
{
    PeekableReadBuffer peekable_buf(buf);
    peekable_buf.setCheckpoint();
    parse_func(peekable_buf);
    peekable_buf.makeContinuousMemoryFromCheckpointToPos();
    auto * end = peekable_buf.position();
    peekable_buf.rollbackToCheckpoint();
    s.append(peekable_buf.position(), end);
    peekable_buf.position() = end;
}

template <typename Vector>
static void readQuotedStringFieldInto(Vector & s, ReadBuffer & buf)
{
    assertChar('\'', buf);
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
        return;

    ++buf.position();
    s.push_back('\'');
}

template <char opening_bracket, char closing_bracket, typename Vector>
static void readQuotedFieldInBracketsInto(Vector & s, ReadBuffer & buf)
{
    assertChar(opening_bracket, buf);
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
            readQuotedStringFieldInto(s, buf);
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
}

template <typename Vector>
void readQuotedFieldInto(Vector & s, ReadBuffer & buf)
{
    if (buf.eof())
        return;

    /// Possible values in 'Quoted' field:
    /// - Strings: '...'
    /// - Arrays: [...]
    /// - Tuples: (...)
    /// - Maps: {...}
    /// - NULL
    /// - Bool: true/false
    /// - Number: integer, float, decimal.

    if (*buf.position() == '\'')
        readQuotedStringFieldInto(s, buf);
    else if (*buf.position() == '[')
        readQuotedFieldInBracketsInto<'[', ']'>(s, buf);
    else if (*buf.position() == '(')
        readQuotedFieldInBracketsInto<'(', ')'>(s, buf);
    else if (*buf.position() == '{')
        readQuotedFieldInBracketsInto<'{', '}'>(s, buf);
    else if (checkCharCaseInsensitive('n', buf))
    {
        /// NULL or NaN
        if (checkCharCaseInsensitive('u', buf))
        {
            assertStringCaseInsensitive("ll", buf);
            s.append("NULL");
        }
        else
        {
            assertStringCaseInsensitive("an", buf);
            s.append("NaN");
        }
    }
    else if (checkCharCaseInsensitive('t', buf))
    {
        assertStringCaseInsensitive("rue", buf);
        s.append("true");
    }
    else if (checkCharCaseInsensitive('f', buf))
    {
        assertStringCaseInsensitive("alse", buf);
        s.append("false");
    }
    else
    {
        /// It's an integer, float or decimal. They all can be parsed as float.
        auto parse_func = [](ReadBuffer & in)
        {
            Float64 tmp;
            readFloatText(tmp, in);
        };
        readParsedValueInto(s, buf, parse_func);
    }
}

template void readQuotedFieldInto<NullOutput>(NullOutput & s, ReadBuffer & buf);

void readQuotedField(String & s, ReadBuffer & buf)
{
    s.clear();
    readQuotedFieldInto(s, buf);
}

void readJSONField(String & s, ReadBuffer & buf)
{
    s.clear();
    auto parse_func = [](ReadBuffer & in) { skipJSONField(in, "json_field"); };
    readParsedValueInto(s, buf, parse_func);
}

void readTSVField(String & s, ReadBuffer & buf)
{
    s.clear();
    readEscapedStringIntoImpl<String, false>(s, buf);
}

}
