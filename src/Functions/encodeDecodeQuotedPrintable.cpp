#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>
#include <Common/Hex.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Quoted-Printable encoding, RFC 2045 section 6.7.
constexpr size_t QP_MAX_LINE_LENGTH = 76;

/// Every byte can become "=XX", plus a soft line break "=\r\n" for every 72 bytes of output.
size_t maxQuotedPrintableEncodedSize(size_t src_size)
{
    return src_size * 3 + (src_size * 3 / 72 + 1) * 3;
}

/// The result decodes back to exactly the input:
///  - printable ASCII (33..126) except '=' is kept as is, every other byte is written as "=XX";
///  - "\r\n" is kept as a hard line break, a lone '\r' or '\n' is escaped;
///  - a space or a tab is escaped when it would end a line, so it cannot be lost as trailing whitespace;
///  - lines are at most 76 characters long, longer ones are split with soft line breaks "=\r\n".
size_t encodeQuotedPrintable(const char * __restrict src, size_t src_size, char * __restrict dst)
{
    char * dst_pos = dst;
    size_t line_length = 0;

    auto soft_line_break = [&]
    {
        dst_pos[0] = '=';
        dst_pos[1] = '\r';
        dst_pos[2] = '\n';
        dst_pos += 3;
        line_length = 0;
    };

    for (size_t i = 0; i < src_size; ++i)
    {
        const auto c = static_cast<unsigned char>(src[i]);

        if (c == '\r' && i + 1 < src_size && src[i + 1] == '\n')
        {
            dst_pos[0] = '\r';
            dst_pos[1] = '\n';
            dst_pos += 2;
            line_length = 0;
            ++i;
            continue;
        }

        const bool ends_line = i + 1 == src_size || (src[i + 1] == '\r' && i + 2 < src_size && src[i + 2] == '\n');
        const bool is_literal = (c >= 33 && c <= 126 && c != '=') || ((c == ' ' || c == '\t') && !ends_line);
        const size_t width = is_literal ? 1 : 3;

        /// Unless this is the last character of the line, keep room for the '=' of a soft line break.
        if (line_length + width > QP_MAX_LINE_LENGTH - (ends_line ? 0 : 1))
            soft_line_break();

        if (is_literal)
        {
            *dst_pos = static_cast<char>(c);
            ++dst_pos;
        }
        else
        {
            *dst_pos = '=';
            ++dst_pos;
            writeHexByteUppercase(c, dst_pos);
            dst_pos += 2;
        }
        line_length += width;
    }
    return dst_pos - dst;
}

/// "=XX" with hex digits in either case becomes the byte, '=' followed by a line break is a soft line break and is
/// removed, a '=' at the very end is dropped, and any other '=' is kept as is. This matches Python's binascii.a2b_qp.
/// The result is never longer than the input.
size_t decodeQuotedPrintable(const char * __restrict src, size_t src_size, char * __restrict dst)
{
    const char * src_pos = src;
    const char * src_end = src + src_size;
    char * dst_pos = dst;

    while (src_pos < src_end)
    {
        const char * equals = find_first_symbols<'='>(src_pos, src_end);
        memcpySmallAllowReadWriteOverflow15(dst_pos, src_pos, equals - src_pos);
        dst_pos += equals - src_pos;
        src_pos = equals;
        if (src_pos == src_end)
            break;

        ++src_pos;
        if (src_pos == src_end)
            break;

        if (*src_pos == '\r' || *src_pos == '\n')
        {
            /// Soft line break: skip to the end of the line.
            src_pos = find_first_symbols<'\n'>(src_pos, src_end);
            if (src_pos != src_end)
                ++src_pos;
        }
        else if (*src_pos == '=')
        {
            *dst_pos = '=';
            ++dst_pos;
            ++src_pos;
        }
        else if (src_end - src_pos >= 2 && unhex(src_pos[0]) != 0xFF && unhex(src_pos[1]) != 0xFF)
        {
            *dst_pos = static_cast<char>((unhex(src_pos[0]) << 4) | unhex(src_pos[1]));
            ++dst_pos;
            src_pos += 2;
        }
        else
        {
            *dst_pos = '=';
            ++dst_pos;
        }
    }
    return dst_pos - dst;
}

enum class QuotedPrintableMode
{
    Encode,
    Decode
};

template <QuotedPrintableMode mode>
struct QuotedPrintableImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        if constexpr (mode == QuotedPrintableMode::Encode)
            res_data.resize(maxQuotedPrintableEncodedSize(data.size()) + input_rows_count * 3);
        else
            res_data.resize(data.size());

        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * src = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t src_size = offsets[i] - prev_offset;
            char * dst = reinterpret_cast<char *>(res_data.data() + res_offset);

            if constexpr (mode == QuotedPrintableMode::Encode)
                res_offset += encodeQuotedPrintable(src, src_size, dst);
            else
                res_offset += decodeQuotedPrintable(src, src_size, dst);

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by Quoted-Printable functions");
    }
};

struct NameEncodeQuotedPrintable { static constexpr auto name = "encodeQuotedPrintable"; };
struct NameDecodeQuotedPrintable { static constexpr auto name = "decodeQuotedPrintable"; };
using FunctionEncodeQuotedPrintable = FunctionStringToString<QuotedPrintableImpl<QuotedPrintableMode::Encode>, NameEncodeQuotedPrintable>;
using FunctionDecodeQuotedPrintable = FunctionStringToString<QuotedPrintableImpl<QuotedPrintableMode::Decode>, NameDecodeQuotedPrintable>;

}

REGISTER_FUNCTION(EncodeAndDecodeQuotedPrintable)
{
    FunctionDocumentation::Description description_encode = R"(
Encodes a string using [Quoted-Printable](https://en.wikipedia.org/wiki/Quoted-printable) encoding, according to [RFC 2045](https://www.rfc-editor.org/rfc/rfc2045#section-6.7), as used in email.

Printable ASCII characters except `=` are kept as is, other bytes are written as `=XX` with uppercase hex digits.
A `\r\n` line break is kept, a lone `\r` or `\n` is encoded. A space or tab is encoded when it would end a line.
Lines are kept to at most 76 characters with soft line breaks (`=\r\n`).
`decodeQuotedPrintable(encodeQuotedPrintable(s))` returns the original string `s` for any `s`.
    )";
    FunctionDocumentation::Syntax syntax_encode = "encodeQuotedPrintable(s)";
    FunctionDocumentation::Arguments arguments_encode = {{"s", "String to encode.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_encode = {"Returns the Quoted-Printable encoded string.", {"String"}};
    FunctionDocumentation::Examples examples_encode = {
    {
        "Usage example",
        "SELECT encodeQuotedPrintable('Café = 3 €') AS encoded",
        R"(
┌─encoded───────────────────┐
│ Caf=C3=A9 =3D 3 =E2=82=AC │
└───────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation documentation_encode = {description_encode, syntax_encode, arguments_encode, {}, returned_value_encode, examples_encode, introduced_in, category};

    factory.registerFunction<FunctionEncodeQuotedPrintable>(documentation_encode);

    FunctionDocumentation::Description description_decode = R"(
Decodes a [Quoted-Printable](https://en.wikipedia.org/wiki/Quoted-printable) encoded string, according to [RFC 2045](https://www.rfc-editor.org/rfc/rfc2045#section-6.7).

`=XX` (hex digits in either case) is replaced by the byte it encodes.
A `=` followed by `\r` or `\n` is a soft line break: it is removed together with the rest of the line up to and including the next `\n`.
Other malformed escape sequences (such as `=ZZ`, or `=4` at the end of the string) are kept as is, except that a bare `=` at the very end of the string is dropped.
The behavior matches Python's `quopri.decodestring`.
    )";
    FunctionDocumentation::Syntax syntax_decode = "decodeQuotedPrintable(s)";
    FunctionDocumentation::Arguments arguments_decode = {{"s", "Quoted-Printable encoded string.", {"String"}}};
    FunctionDocumentation::ReturnedValue returned_value_decode = {"Returns the decoded string.", {"String"}};
    FunctionDocumentation::Examples examples_decode = {
    {
        "Usage example",
        "SELECT decodeQuotedPrintable('Caf=C3=A9 =3D 3 =E2=82=AC') AS decoded",
        R"(
┌─decoded────┐
│ Café = 3 € │
└────────────┘
        )"
    }
    };
    FunctionDocumentation documentation_decode = {description_decode, syntax_decode, arguments_decode, {}, returned_value_decode, examples_decode, introduced_in, category};

    factory.registerFunction<FunctionDecodeQuotedPrintable>(documentation_decode);
}

}
