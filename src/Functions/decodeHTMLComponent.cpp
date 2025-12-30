#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/HTMLCharacterReference.h>
#include <base/find_symbols.h>
#include <base/hex.h>
#include <Common/StringUtils.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    struct DecodeHTMLComponentName
    {
        static constexpr auto name = "decodeHTMLComponent";
    };

    class FunctionDecodeHTMLComponentImpl
    {
    public:
        static void vector(
            const ColumnString::Chars & data,
            const ColumnString::Offsets & offsets,
            ColumnString::Chars & res_data,
            ColumnString::Offsets & res_offsets,
            size_t input_rows_count)
        {
            /// The size of result is always not more than the size of source.
            /// Because entities decodes to the shorter byte sequence.
            /// Example: &#xx... &#xx... will decode to UTF-8 byte sequence not longer than 4 bytes.
            res_data.resize(data.size());

            res_offsets.resize(input_rows_count);

            size_t prev_offset = 0;
            size_t res_offset = 0;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const char * src_data = reinterpret_cast<const char *>(&data[prev_offset]);
                size_t src_size = offsets[i] - prev_offset;
                size_t dst_size = execute(src_data, src_size, reinterpret_cast<char *>(res_data.data() + res_offset));

                res_offset += dst_size;
                res_offsets[i] = res_offset;
                prev_offset = offsets[i];
            }

            res_data.resize(res_offset);
        }

        [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function decodeHTMLComponent cannot work with FixedString argument");
        }

    private:
        static const int max_legal_unicode_value = 0x10FFFF;
        static const int max_decimal_length_of_unicode_point = 7; /// 1114111

        static size_t execute(const char * src, size_t src_size, char * dst)
        {
            const char * src_pos = src;
            const char * src_end = src + src_size;
            char * dst_pos = dst;

            // to hold char seq for lookup, reuse it
            std::vector<char> seq;
            while (true)
            {
                const char * entity_pos = find_first_symbols<'&'>(src_pos, src_end);

                /// Copy text between entities.
                size_t bytes_to_copy = entity_pos - src_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;
                src_pos = entity_pos;

                ++entity_pos;

                const char * entity_end = find_first_symbols<';'>(entity_pos, src_end);

                if (entity_end == src_end)
                    break;

                bool parsed = false;

                /// covers &#NNNN; or &#xNNNN hexadecimal values;
                uint32_t code_point = 0;
                if (isValidNumericEntity(entity_pos, entity_end, code_point))
                {
                    codePointToUTF8(code_point, dst_pos);
                    parsed = true;
                }
                else /// covers html encoded character sequences
                {
                    // seq_length should also include `;` at the end
                    size_t seq_length = (entity_end - entity_pos) + 1;
                    seq.assign(entity_pos, entity_pos + seq_length);
                    // null terminate the sequence
                    seq.push_back('\0');
                    // lookup the html sequence in the perfect hashmap.
                    const auto * res = HTMLCharacterHash::Lookup(seq.data(), strlen(seq.data()));
                    // reset so that it's reused in the next iteration
                    seq.clear();
                    if (res)
                    {
                        const auto * glyph = res->glyph;
                        for (size_t i = 0; i < strlen(glyph); ++i)
                        {
                            *dst_pos = glyph[i];
                            ++dst_pos;
                        }
                        parsed = true;
                    }
                    else
                        parsed = false;
                }

                if (parsed)
                {
                    /// Skip the parsed entity.
                    src_pos = entity_end + 1;
                }
                else
                {
                    /// Copy one byte as is and skip it.
                    *dst_pos = *src_pos;
                    ++dst_pos;
                    ++src_pos;
                }
            }

            /// Copy the rest of the string.
            if (src_pos < src_end)
            {
                size_t bytes_to_copy = src_end - src_pos;
                memcpySmallAllowReadWriteOverflow15(dst_pos, src_pos, bytes_to_copy);
                dst_pos += bytes_to_copy;
            }

            return dst_pos - dst;
        }

        static size_t codePointToUTF8(uint32_t code_point, char *& dst_pos)
        {
            if (code_point < (1 << 7))
            {
                dst_pos[0] = (code_point & 0x7F);
                ++dst_pos;
                return 1;
            }
            if (code_point < (1 << 11))
            {
                dst_pos[0] = ((code_point >> 6) & 0x1F) + 0xC0;
                dst_pos[1] = (code_point & 0x3F) + 0x80;
                dst_pos += 2;
                return 2;
            }
            if (code_point < (1 << 16))
            {
                dst_pos[0] = ((code_point >> 12) & 0x0F) + 0xE0;
                dst_pos[1] = ((code_point >> 6) & 0x3F) + 0x80;
                dst_pos[2] = (code_point & 0x3F) + 0x80;
                dst_pos += 3;
                return 3;
            }

            dst_pos[0] = ((code_point >> 18) & 0x07) + 0xF0;
            dst_pos[1] = ((code_point >> 12) & 0x3F) + 0x80;
            dst_pos[2] = ((code_point >> 6) & 0x3F) + 0x80;
            dst_pos[3] = (code_point & 0x3F) + 0x80;
            dst_pos += 4;
            return 4;
        }

        [[maybe_unused]] static bool isValidNumericEntity(const char * src, const char * end, uint32_t & code_point)
        {
            if (src + strlen("#") >= end)
                return false;
            if (src[0] != '#' || (end - src > 1 + max_decimal_length_of_unicode_point))
                return false;

            if (src + 2 < end && (src[1] == 'x' || src[1] == 'X'))
            {
                src += 2;
                for (; src < end; ++src)
                {
                    if (!isHexDigit(*src))
                        return false;
                    code_point *= 16;
                    code_point += unhex(*src);
                }
            }
            else
            {
                src += 1;
                for (; src < end; ++src)
                {
                    if (!isNumericASCII(*src))
                        return false;
                    code_point *= 10;
                    code_point += *src - '0';
                }
            }

            return code_point <= max_legal_unicode_value;
        }
    };

    using FunctionDecodeHTMLComponent = FunctionStringToString<FunctionDecodeHTMLComponentImpl, DecodeHTMLComponentName>;

}

REGISTER_FUNCTION(DecodeHTMLComponent)
{
    factory.registerFunction<FunctionDecodeHTMLComponent>();
}
}
